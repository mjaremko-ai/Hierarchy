import csv
import io
import json
import re
import time
import urllib.parse

import requests
from flask import Flask, render_template, request, Response

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB upload limit

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_SEARCH = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "HierarchyLookup/1.0 (hierarchy-lookup-tool)"}


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
    try:
        from duckduckgo_search import DDGS

        query = f'"{name}" parent company'
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=5))

        for result in results:
            body = (result.get("body") or "") + " " + (result.get("title") or "")
            # Look for patterns like "is a subsidiary of X" or "owned by X"
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
    """Extract the bare domain from a URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc or parsed.path
        host = host.lower().lstrip("www.")
        return host.split("/")[0]
    except Exception:
        return ""


def lookup_parent(name: str, domain: str) -> dict:
    """Main lookup: try Wikidata first, fall back to DDG search."""
    result = {"parent_name": "", "parent_domain": "", "source": ""}

    qid = search_wikidata_entity(name)
    if qid:
        parent = get_parent_from_wikidata(qid)
        if parent and parent["parent_name"]:
            result.update(parent)
            result["source"] = "Wikidata"
            return result

    # Wikidata had no parent — try DDG
    parent = search_parent_via_ddg(name, domain)
    if parent and parent["parent_name"]:
        result.update(parent)
        result["source"] = "Web search"

    return result


def detect_columns(fieldnames: list[str]) -> tuple[str, str]:
    """Detect which CSV columns hold the account name and domain."""
    name_col = domain_col = None
    for f in fieldnames:
        fl = f.lower().strip()
        if name_col is None and any(kw in fl for kw in ("name", "account", "company", "org")):
            name_col = f
        if domain_col is None and any(kw in fl for kw in ("domain", "website", "url", "site")):
            domain_col = f
    return name_col, domain_col


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

    def generate():
        results = []
        for i, row in enumerate(rows):
            name = row.get(name_col, "").strip()
            domain = row.get(domain_col, "").strip() if domain_col else ""
            if not name:
                parent = {"parent_name": "", "parent_domain": "", "source": ""}
            else:
                parent = lookup_parent(name, domain)
                time.sleep(0.3)  # be polite to external APIs

            result = {
                "index": i,
                "name": name,
                "domain": domain,
                "parent_name": parent["parent_name"],
                "parent_domain": parent["parent_domain"],
                "source": parent["source"],
            }
            results.append(result)
            yield f"data: {json.dumps(result)}\n\n"

        yield f"data: {json.dumps({'done': True, 'total': len(results)})}\n\n"

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
    app.run(debug=True, port=5000)
