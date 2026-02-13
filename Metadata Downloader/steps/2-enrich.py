#!/usr/bin/env python3
"""
Enrich resources CSV with Date, Format, and Tags from the MSW resource API.

Default behavior: only update rows that have NOT yet been enriched. The output file
(if it exists) is the source of truth: we load it, only fetch RUIDs not already
enriched there, then write the merged result (existing enriched + new fetches + input).
We never overwrite existing enriched data. Use --force to re-fetch all.

When output exists: we write the full merged content to a temp file, then rename to
the output path on success, so the existing output is never corrupted by a crash.

Auth: same as collect_ruids (interactive F/C/E/S/O or --ifwt / MSW_IFWT / --no-browser).
Workers share a queue: RUIDs are processed in order (1st, 2nd, 3rd, …); when a worker
finishes one request it takes the next. --delay between each request.
"""

import argparse
import csv
import json
import os
import queue
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from steps._api import build_headers, get_default_ifwt, get_ifwt_interactive, BASE_URL

CSV_COLUMNS = ("RUID", "Category", "Subcategory", "Date", "Format", "Tags")


def _normalize_ruid(ruid: str) -> str:
    """Canonical form for lookup: 32 hex chars, lower. API may return guid with dashes or prefix; CSV has 32 hex."""
    if not ruid:
        return ""
    hex_only = "".join(c for c in ruid.strip().lower() if c in "0123456789abcdef")
    if len(hex_only) >= 32:
        return hex_only[-32:]  # last 32 so API "prefix:guid" still matches CSV "guid"
    return hex_only


def _row_key(row: dict) -> tuple[str, str, str]:
    """(ruid, category, subcategory) for matching input/output rows."""
    ruid = _normalize_ruid(row.get("RUID") or "")
    cat = (row.get("Category") or "").strip()
    sub = (row.get("Subcategory") or "").strip()
    return (ruid, cat, sub)


def _row_has_enrichment(row: dict) -> bool:
    """True if row has at least one of Date/Format/Tags non-empty."""
    return bool((row.get("Date") or row.get("Format") or row.get("Tags") or "").strip())


def _load_existing_output(output_path: str):
    """
    If output file exists, load it and return (already_enriched_set, output_rows_dict).
    already_enriched_set: set of (ruid, category, subcategory) that have Date/Format/Tags.
    output_rows_dict: (ruid, cat, sub) -> row dict (so we never overwrite these when writing).
    If file does not exist or is invalid, return (set(), {}).
    """
    already_enriched: set[tuple[str, str, str]] = set()
    output_rows_dict: dict[tuple[str, str, str], dict] = {}
    if not os.path.isfile(output_path):
        return (already_enriched, output_rows_dict)
    try:
        with open(output_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != list(CSV_COLUMNS):
                return (already_enriched, output_rows_dict)
            for row in reader:
                row = {k: row.get(k, "") for k in CSV_COLUMNS}
                key = _row_key(row)
                output_rows_dict[key] = row
                if _row_has_enrichment(row):
                    already_enriched.add(key)
    except OSError:
        pass
    return (already_enriched, output_rows_dict)


def _format_from_path(path: str) -> str:
    """Derive format (extension) from API path."""
    if not path:
        return ""
    segment = path.split("/")[-1]
    return segment.rsplit(".", 1)[-1].lower() if "." in segment else ""


def _parse_match(m: dict) -> tuple[str, str, str]:
    """Extract (date, format, tags_str) from one match object."""
    mtime = (m.get("mtime") or m.get("date") or m.get("modifiedTime") or "").strip()
    path = m.get("path") or m.get("url") or ""
    format_str = _format_from_path(path)
    tags_list = m.get("tags") or []
    tags_str = ", ".join(str(t) for t in tags_list)
    return (mtime, format_str, tags_str)


def fetch_one(
    ruid: str,
    headers: dict,
    timeout: int = 20,
    retries: int = 2,
    retry_delay: float = 1.0,
    error_out: list | None = None,
) -> dict[str, tuple[str, str, str]]:
    """
    Fetch one RUID via GET .../resource/v1/search/{ruid}. Returns {ruid: (date, format, tags_str)}.
    On failure returns {}. If error_out is a list, appends one short error string on failure.
    """
    if not ruid or len(_normalize_ruid(ruid)) < 32:
        return {}
    url = f"{BASE_URL}/resource/v1/search/{ruid}"
    key = _normalize_ruid(ruid)
    result: dict[str, tuple[str, str, str]] = {}
    for attempt in range(max(1, retries + 1)):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
            data = json.loads(body)
            if data.get("code") != 0:
                msg = f"code={data.get('code')} {data.get('message', '')}"
                if data.get("code") == -1 and attempt < retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                if error_out is not None and len(error_out) == 0:
                    error_out.append(msg)
                return result
            payload = data.get("data") or {}
            matches = payload.get("matches") or payload.get("results") or []
            if matches:
                result[key] = _parse_match(matches[0])
            return result
        except HTTPError as e:
            if attempt < retries and e.code in (429, 503, 502):
                backoff = retry_delay * (attempt + 1)
                if e.code == 429:
                    backoff = max(backoff, 5.0)
                time.sleep(backoff)
                continue
            if error_out is not None and len(error_out) == 0:
                error_out.append(f"HTTP {e.code} {e.reason}")
            return result
        except (URLError, json.JSONDecodeError, Exception) as e:
            if error_out is not None and len(error_out) == 0:
                error_out.append(str(e)[:80])
            return result
    return result


# Sentinel for writer queue: no more batch results
_WRITER_DONE = None


def _write_row(
    writer,
    row: dict,
    info: tuple[str, str, str] | None,
    output_rows_dict: dict,
    out,
    flush: bool = False,
) -> None:
    """
    Write one CSV row. Never overwrite existing enriched data.
    Precedence: new API info > existing output row with enrichment > input row.
    Caller flushes periodically (e.g. every 100 rows) to avoid per-row flush overhead.
    """
    key = _row_key(row)
    ruid = key[0]
    if info:
        date_str, format_str, tags_str = info
        writer.writerow([
            row.get("RUID", ""),
            row.get("Category", ""),
            row.get("Subcategory", ""),
            date_str,
            format_str,
            tags_str,
        ])
    elif key in output_rows_dict and _row_has_enrichment(output_rows_dict[key]):
        # Preserve existing enriched row from output file; do not overwrite
        existing = output_rows_dict[key]
        writer.writerow([existing.get(k, "") for k in CSV_COLUMNS])
    else:
        writer.writerow([row.get(k, "") for k in CSV_COLUMNS])
    if flush:
        out.flush()


# Flush disk every this many rows to avoid per-row flush overhead
_WRITER_FLUSH_INTERVAL = 100


def _writer_worker(
    result_queue: queue.Queue,
    rows: list[dict[str, str]],
    output_path: str,
    output_rows_dict: dict,
    verbose: bool,
) -> None:
    """
    Single writer thread: consume results from the queue, merge into enriched dict,
    and write rows in input order. No full-file read/merge per result—only append
    rows as their enrichment becomes available. Other workers only fetch and put
    results on this queue; this is the only thread that writes.
    Uses output_rows_dict so we NEVER overwrite existing enriched rows from the
    output file. Writes to a temp file then renames to output_path on success.
    """
    enriched: dict[str, tuple[str, str, str]] = {}
    next_row = 0
    fd, temp_path = tempfile.mkstemp(suffix=".csv", prefix="enrich_", dir=os.path.dirname(output_path) or ".")
    try:
        with open(fd, "w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(CSV_COLUMNS)
            out.flush()

            while True:
                item = result_queue.get()
                try:
                    if item is _WRITER_DONE:
                        break
                    enriched.update(item)
                    # Drain all consecutive rows we can write in order (no full merge/rewrite)
                    while next_row < len(rows):
                        ruid = _normalize_ruid(rows[next_row].get("RUID") or "")
                        if ruid not in enriched:
                            break
                        info = enriched.get(ruid)
                        flush_now = (next_row + 1) % _WRITER_FLUSH_INTERVAL == 0
                        _write_row(writer, rows[next_row], info, output_rows_dict, out, flush=flush_now)
                        next_row += 1
                        if verbose and next_row <= 100 and next_row % 20 == 0:
                            print(f"Writer: wrote {next_row} rows so far", file=sys.stderr)
                        elif verbose and next_row > 100 and next_row % 1000 == 0:
                            print(f"Writer: wrote {next_row} rows so far", file=sys.stderr)
                finally:
                    result_queue.task_done()

            while next_row < len(rows):
                ruid = _normalize_ruid(rows[next_row].get("RUID") or "")
                info = enriched.get(ruid)
                flush_now = (next_row + 1) % _WRITER_FLUSH_INTERVAL == 0
                _write_row(writer, rows[next_row], info, output_rows_dict, out, flush=flush_now)
                next_row += 1
            out.flush()
        os.replace(temp_path, output_path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise

    if verbose:
        print(f"Writer: merged {len(enriched)} RUIDs, wrote {len(rows)} rows (existing enriched rows preserved).", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Enrich resources CSV with Date, Format, Tags via GET .../resource/v1/search/{ruid} per RUID."
    )
    ap.add_argument("input", nargs="?", default="resources.csv", help="Input CSV from collect_ruids (default: resources.csv)")
    ap.add_argument("-I", dest="input", metavar="FILE", help="Input CSV (same as positional)")
    ap.add_argument("-o", "--output", help="Output CSV (default: input stem + _enriched.csv)")
    ap.add_argument("--ifwt", "-i", help="Session token (or set MSW_IFWT)")
    ap.add_argument("--no-browser", action="store_true", help="Do not prompt for browser/token")
    ap.add_argument("--workers", "-w", type=int, default=2, help="Concurrent single-RUID requests (default: 2). For 1.5M use 32–128 if API allows; cap 256.")
    ap.add_argument("--delay", type=float, default=0.1, help="Seconds between starting each request; base for retry backoff (default: 0.1)")
    ap.add_argument("--retries", type=int, default=2, help="Retries per RUID on API code=-1 or 5xx (default: 2)")
    ap.add_argument("--force", action="store_true", help="Re-fetch all RUIDs (default: only fetch rows with empty Date/Format/Tags; already-enriched rows are preserved)")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print progress")
    ap.add_argument("--limit", "-n", type=int, metavar="N", help="Process only first N rows (for testing)")
    ap.add_argument("--limit-per-category", type=int, metavar="N", help="Take up to N rows per Category (for testing; e.g. 50 per category)")
    ap.add_argument("--test", action="store_true", help="Test run: same as --limit-per-category 50 (50 rows per category)")
    args = ap.parse_args()

    if args.test and args.limit is None and args.limit_per_category is None:
        args.limit_per_category = 50

    _green = "\033[92m" if hasattr(sys.stderr, "isatty") and sys.stderr.isatty() else ""
    _reset = "\033[0m" if _green else ""
    print(f"{_green}Step 2: Enrich starting...{_reset}", file=sys.stderr)
    sys.stderr.flush()

    ifwt = args.ifwt or os.environ.get("MSW_IFWT")
    if not ifwt and not args.no_browser:
        ifwt = get_ifwt_interactive(verbose=True)
    if not ifwt:
        ifwt = get_default_ifwt()
    if not ifwt:
        print("Need _ifwt. Use --ifwt or MSW_IFWT or run without --no-browser.", file=sys.stderr)
        return 1

    input_path = args.input
    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    print(f"Loading input: {input_path} ...", file=sys.stderr)
    sys.stderr.flush()

    if args.output:
        output_path = args.output
    else:
        base, _ = os.path.splitext(input_path)
        output_path = f"{base}_enriched.csv"

    headers = build_headers(ifwt, None)

    # Output file is the source of truth for "already enriched". If it exists, load it so we never overwrite it.
    already_enriched: set[tuple[str, str, str]] = set()
    output_rows_dict: dict[tuple[str, str, str], dict] = {}
    if os.path.isfile(output_path):
        already_enriched, output_rows_dict = _load_existing_output(output_path)
        if args.verbose and output_rows_dict:
            print(f"Loaded existing output {output_path}: {len(output_rows_dict)} rows, {len(already_enriched)} already enriched (will not overwrite).", file=sys.stderr)

    # Load input rows; only fetch RUIDs that are NOT already enriched in the output file (or all if --force).
    # With --limit: take first N rows. With --limit-per-category: take up to N rows per Category value.
    rows: list[dict[str, str]] = []
    unique_needed: list[str] = []
    seen_ruid: set[str] = set()
    already_enriched_count = 0
    limit = args.limit
    limit_per_cat = getattr(args, "limit_per_category", None)
    category_counts: dict[str, int] = {}
    with open(input_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != list(CSV_COLUMNS):
            print("Expected columns: RUID, Category, Subcategory, Date, Format, Tags", file=sys.stderr)
            return 1
        for row in reader:
            if limit is not None and len(rows) >= limit:
                break
            cat = (row.get("Category") or "").strip()
            if limit_per_cat is not None:
                if category_counts.get(cat, 0) >= limit_per_cat:
                    continue
                category_counts[cat] = category_counts.get(cat, 0) + 1
            row_dict = {k: row.get(k, "") for k in CSV_COLUMNS}
            rows.append(row_dict)
            ruid = _normalize_ruid(row.get("RUID") or "")
            if not ruid or len(ruid) < 32:
                continue
            key = _row_key(row_dict)
            if args.force:
                if ruid not in seen_ruid:
                    seen_ruid.add(ruid)
                    unique_needed.append(ruid)
            elif key in already_enriched:
                # Already enriched in output file; do not fetch, writer will preserve it
                already_enriched_count += 1
            else:
                if ruid not in seen_ruid:
                    seen_ruid.add(ruid)
                    unique_needed.append(ruid)
    if limit is not None and args.verbose:
        print(f"Limited to first {len(rows)} rows.", file=sys.stderr)
    if limit_per_cat is not None and args.verbose:
        print(f"Limited to up to {limit_per_cat} rows per category: {len(rows)} rows total.", file=sys.stderr)
    if args.verbose:
        print(f"Rows: {len(rows)}, unique RUIDs needing enrichment: {len(unique_needed)}", file=sys.stderr)
        if already_enriched_count:
            print(f"Skipping {already_enriched_count} row(s) already enriched in output (will not overwrite).", file=sys.stderr)

    if not unique_needed:
        if args.verbose:
            print("Nothing to enrich.", file=sys.stderr)
        if os.path.isfile(output_path):
            if args.verbose:
                print("Output file exists; leaving it unchanged (will not overwrite).", file=sys.stderr)
            return 0
        with open(output_path, "w", encoding="utf-8", newline="") as out:
            w = csv.DictWriter(out, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerows(rows)
        return 0

    num_ruids = len(unique_needed)
    workers = max(1, min(args.workers, 256))
    if args.verbose:
        print(f"Fetching {num_ruids} RUID(s) via GET .../search/{{ruid}} with 1 writer + {workers} fetch worker(s).", file=sys.stderr)

    # Single writer thread: only it writes to disk. Fetch workers only add results to the queue.
    result_queue: queue.Queue = queue.Queue()
    writer_thread = threading.Thread(
        target=_writer_worker,
        args=(result_queue, rows, output_path, output_rows_dict, args.verbose),
    )
    writer_thread.start()

    # Pre-flight: fetch first RUID so we see result or error quickly (avoids "nothing for 10 min")
    first_error_list: list[str] = []
    first_ruid = unique_needed[0]
    print(f"Pre-flight: fetching first RUID ({first_ruid[:20]}...)...", file=sys.stderr)
    sys.stderr.flush()
    preflight_result: list = []

    def _do_preflight() -> None:
        r = fetch_one(first_ruid, headers, timeout=10, retries=1, retry_delay=2, error_out=first_error_list)
        preflight_result.append(r)

    preflight_thread = threading.Thread(target=_do_preflight, daemon=True)
    preflight_thread.start()
    preflight_thread.join(timeout=20)
    if preflight_thread.is_alive():
        print("Pre-flight TIMEOUT after 20s (request did not complete). Check network/proxy.", file=sys.stderr)
        sys.stderr.flush()
        preflight = None
    else:
        preflight = preflight_result[0] if preflight_result else None
    if preflight:
        result_queue.put(preflight)
        print("Pre-flight OK: first result queued.", file=sys.stderr)
    else:
        if first_error_list:
            print(f"Pre-flight failed: {first_error_list[0]}", file=sys.stderr)
        else:
            print("Pre-flight failed: no data returned.", file=sys.stderr)
    sys.stderr.flush()

    def do_one_and_put(ruid: str, index: int) -> None:
        time.sleep(args.delay * (1 + (index % max(1, workers))))  # stagger starts (single-worker: delay once)
        result = fetch_one(
            ruid,
            headers,
            timeout=15,
            retries=getattr(args, "retries", 2),
            retry_delay=args.delay,
            error_out=first_error_list,
        )
        if result:
            result_queue.put(result)

    def worker_take_next(
        next_index: list[int],
        lock: threading.Lock,
        unique_needed_list: list[str],
        num: int,
        first_error_list: list,
        first_error_printed: list,
        completed_count: list,
        results_count: list,
    ) -> None:
        """Worker: take next RUID index in order, fetch it, put result in queue. Repeat until no more indices."""
        while True:
            with lock:
                if next_index[0] >= num:
                    return
                i = next_index[0]
                next_index[0] += 1
                ruid = unique_needed_list[i]
            try:
                result = fetch_one(
                    ruid,
                    headers,
                    timeout=15,
                    retries=getattr(args, "retries", 2),
                    retry_delay=args.delay,
                    error_out=first_error_list,
                )
                if result:
                    result_queue.put(result)
            except Exception as e:
                if args.verbose:
                    print(f"  RUID {i + 1} failed: {e}", file=sys.stderr)
            with lock:
                completed_count[0] += 1
                if result:
                    results_count[0] += 1
                n, r = completed_count[0], results_count[0]
                if first_error_list and not first_error_printed[0]:
                    first_error_printed[0] = True
                    print(f"  First fetch failed: {first_error_list[0]}", file=sys.stderr)
                if args.verbose and n <= 100 and n % 20 == 0:
                    print(f"  Progress: {n}/{num} fetched, {r} results", file=sys.stderr)
                elif args.verbose and n > 100 and n % 500 == 0:
                    print(f"  Progress: {n}/{num} fetched, {r} results", file=sys.stderr)
            time.sleep(args.delay)

    if workers <= 1:
        for i, ruid in enumerate(unique_needed):
            if i == 0:
                continue  # already fetched by pre-flight
            if args.verbose and (i + 1) % 100 == 0:
                print(f"  RUID {i + 1}/{num_ruids}...", file=sys.stderr)
            try:
                do_one_and_put(ruid, i)
            except Exception as e:
                if args.verbose:
                    print(f"  RUID failed: {e}", file=sys.stderr)
        result_queue.put(_WRITER_DONE)
    else:
        if args.verbose:
            print(f"  Workers share a queue: RUIDs 1, 2, 3, ... in order; each worker takes the next when free.", file=sys.stderr)
        next_index: list[int] = [1]  # 0 already fetched by pre-flight
        lock = threading.Lock()
        unique_needed_list = unique_needed
        first_error_printed: list[bool] = [False]
        completed_count: list[int] = [0]
        results_count: list[int] = [0]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    worker_take_next,
                    next_index,
                    lock,
                    unique_needed_list,
                    num_ruids,
                    first_error_list,
                    first_error_printed,
                    completed_count,
                    results_count,
                )
                for _ in range(workers)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    if args.verbose:
                        print(f"  Worker failed: {e}", file=sys.stderr)
        result_queue.put(_WRITER_DONE)

    # Script complete only when writer has processed the queue and written the file.
    writer_thread.join()
    print(f"Wrote {len(rows)} rows to {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
