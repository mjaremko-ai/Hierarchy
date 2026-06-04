"""
Standalone CLI: process a CSV of accounts and output parent hierarchy results.

Usage:
    python run_lookup.py input.csv output.csv [--workers 8]
"""
import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from app import lookup_parent, detect_columns

CHECKPOINT_EVERY = 100  # write partial results every N rows


def main():
    parser = argparse.ArgumentParser(description="Account hierarchy lookup")
    parser.add_argument("input", help="Input CSV file")
    parser.add_argument("output", help="Output CSV file")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default 8)")
    args = parser.parse_args()

    with open(args.input, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    name_col, domain_col = detect_columns(fieldnames)
    if not name_col:
        print(f"ERROR: Cannot detect name column. Columns: {fieldnames}", file=sys.stderr)
        sys.exit(1)

    print(f"Columns detected  — name: '{name_col}', domain: '{domain_col or 'none'}'")
    print(f"Accounts to process: {len(rows)}")
    print(f"Workers: {args.workers}")

    results = [None] * len(rows)
    completed = 0
    start = time.monotonic()

    out_fields = ["name", "domain", "parent_name", "parent_domain", "source"]

    def process(i: int, row: dict) -> tuple[int, dict]:
        name = row.get(name_col, "").strip()
        domain = row.get(domain_col, "").strip() if domain_col else ""
        parent = lookup_parent(name, domain) if name else {"parent_name": "", "parent_domain": "", "source": ""}
        return i, {
            "name": name,
            "domain": domain,
            "parent_name": parent["parent_name"],
            "parent_domain": parent["parent_domain"],
            "source": parent["source"],
        }

    def write_checkpoint():
        done = [r for r in results if r is not None]
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_fields)
            w.writeheader()
            w.writerows(done)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process, i, row): i for i, row in enumerate(rows)}
        for future in as_completed(futures):
            i, result = future.result()
            results[i] = result
            completed += 1

            elapsed = time.monotonic() - start
            rate = completed / elapsed if elapsed > 0 else 0
            remaining = (len(rows) - completed) / rate if rate > 0 else 0
            print(
                f"\r[{completed}/{len(rows)}] {rate:.1f} acc/s  "
                f"ETA {int(remaining // 60)}m{int(remaining % 60):02d}s  "
                f"— {result['name'][:40]}",
                end="",
                flush=True,
            )

            if completed % CHECKPOINT_EVERY == 0:
                write_checkpoint()

    print()  # newline after progress line
    write_checkpoint()
    elapsed = time.monotonic() - start
    found = sum(1 for r in results if r and r["parent_name"])
    print(f"\nDone in {elapsed:.0f}s. {found}/{len(rows)} accounts have a parent.")
    print(f"Results written to: {args.output}")


if __name__ == "__main__":
    main()
