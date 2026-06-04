import csv
import io
import json
import queue
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, render_template, request, Response

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB upload limit

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_SEARCH = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "HierarchyLookup/1.0 (hierarchy-lookup-tool)"}

# --- Rate limiter (token bucket) -------------------------------------------
# Wikidata asks for ≤ 5 req/s total; DDG is best-effort.
# We use 4 req/s for Wikidata and a separate 2 req/s bucket for DDG.

class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
        # Wait for a token
        time.sleep(1.0 / self._rate)
        self.acquire()


_wikidata_bucket = TokenBucket(rate=4.0, capacity=4.0)
_ddg_bucket = TokenBucket(rate=2.0, capacity=2.0)


# ---------------------------------------------------------------------------

def search_wikidata_entity(name: str) -> str | None:
    """Return the Wikidata QID for a company name, or None."""
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "type": "item",
        "limit": 5,
        "format": "json",
    }
    _wikidata_bucket.acquire()
    try:
        r = requests.get(WIKIDATA_SEARCH, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        results = r.json().get("search", [])
        for result in results:
            desc = result.get("description", "").lower()
            if any(kw in desc for kw in ("company", "corporation", "business", "enterprise", "firm", "organization", "software", "technology", "tech")):
                return result["id"]
        if results:
            return results[0]["id"]
    except Exception:
        pass
    return None


def get_parent_from_wikidata(qid: str) -> dict | None:
    """Query Wikidata for parent organization (P749) of a given QID."""
    sparql = f"""
SELECT ?parent ?parentLabel ?website WHERE {{
  wd:{qid} wdt:P749 ?parent .
  OPTIONAL {{ ?parent wdt:P856 ?website . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 1
"""
    _wikidata_bucket.acquire()
    try:
        r = requests.get(
            WIKIDATA_ENDPOINT,
            params={"query": sparql, "format": "json"},
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        bindings = r.json().get("results", {}).get("bindings", [])
        if bindings:
            row = bindings[0]
            parent_name = row.get("parentLabel", {}).get("value", "")
            website = row.get("website", {}).get("value", "")
            domain = extract_domain(website) if website else ""
            return {"parent_name": parent_name, "parent_domain": domain}
    except Exception:
        pass
    return None


def search_parent_via_ddg(name: str, domain: str) -> dict | None:
    """Fall back to DuckDuckGo search to find parent company info."""
    _ddg_bucket.acquire()
    try:
        from ddgs import DDGS

        query = f'"{name}" parent company'
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=5))

        for result in results:
            body = (result.get("body") or "") + " " + (result.get("title") or "")
            patterns = [
                r"subsidiary of ([A-Z][A-Za-z0-9\s,\.&]+?)[\.,\(\)]",
                r"owned by ([A-Z][A-Za-z0-9\s,\.&]+?)[\.,\(\)]",
                r"acquired by ([A-Z][A-Za-z0-9\s,\.&]+?)[\.,\(\)]",
                r"division of ([A-Z][A-Za-z0-9\s,\.&]+?)[\.,\(\)]",
            ]
            for pat in patterns:
                m = re.search(pat, body)
                if m:
                    parent_name = m.group(1).strip()
                    return {"parent_name": parent_name, "parent_domain": ""}
    except Exception:
        pass
    return None


def extract_domain(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc or parsed.path
        host = host.lower().lstrip("www.")
        return host.split("/")[0]
    except Exception:
        return ""


def lookup_parent(name: str, domain: str) -> dict:
    """Try Wikidata first, fall back to DDG search."""
    result = {"parent_name": "", "parent_domain": "", "source": ""}

    qid = search_wikidata_entity(name)
    if qid:
        parent = get_parent_from_wikidata(qid)
        if parent and parent["parent_name"]:
            result.update(parent)
            result["source"] = "Wikidata"
            return result

    parent = search_parent_via_ddg(name, domain)
    if parent and parent["parent_name"]:
        result.update(parent)
        result["source"] = "Web search"

    return result


def detect_columns(fieldnames: list[str]) -> tuple[str, str]:
    """Detect name and domain columns with priority scoring."""
    name_col = domain_col = None

    def score_name(f: str) -> int:
        fl = f.lower().strip()
        if fl in ("name", "account name", "company name"):
            return 4
        if "name" in fl and "id" not in fl:
            return 3
        if any(kw in fl for kw in ("company", "org")) and "id" not in fl:
            return 2
        if "account" in fl and "id" not in fl:
            return 1
        return 0

    best = 0
    for f in fieldnames:
        s = score_name(f)
        if s > best:
            best = s
            name_col = f

    for f in fieldnames:
        fl = f.lower().strip()
        if any(kw in fl for kw in ("domain", "website", "url", "site")):
            domain_col = f
            break

    return name_col, domain_col


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("csv_file")
    if not file or not file.filename.endswith(".csv"):
        return render_template("index.html", error="Please upload a valid CSV file.")

    content = file.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    fieldnames = reader.fieldnames or []
    name_col, domain_col = detect_columns(fieldnames)

    if not name_col:
        return render_template(
            "index.html",
            error=f"Could not detect a name/account column. Columns found: {', '.join(fieldnames)}",
        )

    rows = list(reader)
    return render_template(
        "processing.html",
        rows=json.dumps([{name_col: r[name_col], domain_col: r.get(domain_col, "") if domain_col else ""} for r in rows]),
        name_col=name_col,
        domain_col=domain_col or "",
        total=len(rows),
    )


@app.route("/stream", methods=["POST"])
def stream():
    data = request.get_json()
    rows = data.get("rows", [])
    name_col = data.get("name_col", "name")
    domain_col = data.get("domain_col", "")
    workers = min(int(data.get("workers", 8)), 20)

    result_queue: queue.Queue = queue.Queue()

    def process(i: int, row: dict):
        name = row.get(name_col, "").strip()
        domain = row.get(domain_col, "").strip() if domain_col else ""
        parent = lookup_parent(name, domain) if name else {"parent_name": "", "parent_domain": "", "source": ""}
        result_queue.put({
            "index": i,
            "name": name,
            "domain": domain,
            "parent_name": parent["parent_name"],
            "parent_domain": parent["parent_domain"],
            "source": parent["source"],
        })

    def generate():
        completed = 0
        total = len(rows)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process, i, row): i for i, row in enumerate(rows)}
            while completed < total:
                try:
                    result = result_queue.get(timeout=30)
                    completed += 1
                    yield f"data: {json.dumps(result)}\n\n"
                except queue.Empty:
                    continue

        yield f"data: {json.dumps({'done': True, 'total': total})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


@app.route("/download", methods=["POST"])
def download():
    results = request.get_json()
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["name", "domain", "parent_name", "parent_domain", "source"],
    )
    writer.writeheader()
    for r in results:
        writer.writerow({
            "name": r.get("name", ""),
            "domain": r.get("domain", ""),
            "parent_name": r.get("parent_name", ""),
            "parent_domain": r.get("parent_domain", ""),
            "source": r.get("source", ""),
        })
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=hierarchy_results.csv"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
