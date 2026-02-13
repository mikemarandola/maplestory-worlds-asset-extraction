#!/usr/bin/env python3
"""
Step 0: Find the actual last page with data for every category and subcategory.
Outputs last_pages.csv (category_id, subcategory_id, category_name, subcategory_name, last_page).
Strategy: Phase 1 probe every 500 pages; Phase 2 binary search; confirm P has data and P+1 does not.
Requires MSW_IFWT.
"""
import argparse
import csv
import json
import os
import sys
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

import importlib.util
spec = importlib.util.spec_from_file_location("collect_mod", os.path.join(_SCRIPT_DIR, "1-collect.py"))
collect_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collect_mod)

from steps._api import build_headers

PROBE_STEP = 500
CONFIRM_ATTEMPTS = 3
RETRY_DELAY = 2.0
DELAY_SEARCH = 0.5
DELAY_CONFIRM = 2.0


def fetch_page_with_retries(
    page: int,
    category: int,
    subcategory: int,
    page_headers: dict,
    verbose: bool = False,
    max_attempts: int = 1,
    retry_delay: float = 2.0,
) -> int:
    """Fetch page; return number of RUIDs (0 if no data or fail after retries)."""
    url = collect_mod.page_url(page, category, subcategory)
    for attempt in range(max_attempts):
        html = collect_mod.fetch_page(url, page_headers)
        found = collect_mod._extract_ruids_from_listing_html(html, category) if html else {}
        count = len(found)
        if verbose:
            if html is None:
                print(f"      page {page} attempt {attempt + 1}: html=None", flush=True)
            else:
                print(f"      page {page} attempt {attempt + 1}: html={len(html)} chars, count={count}", flush=True)
        if count > 0:
            return count
        if attempt < max_attempts - 1:
            time.sleep(retry_delay)
    return 0


def phase1_first_empty(
    category: int,
    subcategory: int,
    sub_name: str,
    max_page: int,
    page_headers: dict,
    verbose: bool = False,
) -> int:
    """Probe every 500 pages (1 attempt each); return first page with no data."""
    print(f"  Phase 1: probing every {PROBE_STEP} pages (1..{max_page}) ...", flush=True)
    for p in range(PROBE_STEP, max_page + 1, PROBE_STEP):
        count = fetch_page_with_retries(p, category, subcategory, page_headers, verbose, max_attempts=1)
        if verbose:
            print(f"    probe page {p}: count={count}", flush=True)
        time.sleep(DELAY_SEARCH)
        if count == 0:
            print(f"    First empty at page {p}.", flush=True)
            return p
    return max_page + PROBE_STEP


def phase2_binary_search_to_last(
    category: int,
    subcategory: int,
    sub_name: str,
    first_empty: int,
    max_page: int,
    page_headers: dict,
    verbose: bool = False,
) -> int | None:
    """Binary search in [1, first_empty - 1] for largest page P with data; confirm P has data and P+1 does not."""
    low = 1
    high = first_empty - 1
    if high < 1:
        return None
    candidate = low
    outer_iters = 0
    max_outer = 20
    while outer_iters < max_outer:
        outer_iters += 1
        if verbose:
            print(f"    Phase 2: binary search in [{low}, {high}] for last page with data ...", flush=True)
        while low < high:
            mid = (low + high + 1) // 2
            count = fetch_page_with_retries(mid, category, subcategory, page_headers, verbose, max_attempts=1)
            time.sleep(DELAY_SEARCH)
            if verbose:
                print(f"    mid={mid}: count={count} -> ", end="", flush=True)
            if count > 0:
                low = mid
                if verbose:
                    print(f"has data, low={low}", flush=True)
            else:
                high = mid - 1
                if verbose:
                    print(f"no data, high={high}", flush=True)
        candidate = low
        if verbose:
            print(f"    Candidate last page: {candidate}. Confirming (3 tries, 2s apart) ...", flush=True)
        for attempt in range(CONFIRM_ATTEMPTS):
            c_prev = fetch_page_with_retries(candidate, category, subcategory, page_headers, verbose, max_attempts=1)
            time.sleep(DELAY_CONFIRM)
            c_next = fetch_page_with_retries(candidate + 1, category, subcategory, page_headers, verbose, max_attempts=1)
            time.sleep(DELAY_CONFIRM)
            if verbose:
                print(f"    confirm attempt {attempt + 1}: page {candidate} count={c_prev}, page {candidate + 1} count={c_next}", flush=True)
            if c_prev > 0 and c_next == 0:
                print(f"    Confirmed: page {candidate} has data, page {candidate + 1} is empty -> last page = {candidate}.", flush=True)
                return candidate
            if c_prev == 0 and attempt == CONFIRM_ATTEMPTS - 1:
                if candidate > 1:
                    print(f"    Warning: page {candidate} had no data on confirmation; returning {candidate - 1}.", flush=True)
                    return candidate - 1
                return None
            if c_next > 0:
                low = candidate + 1
                high = first_empty - 1
                if low > high:
                    break
                if verbose:
                    print(f"    Page {candidate + 1} has data, resuming search in [{low}, {high}] ...", flush=True)
                break
        else:
            print(f"    Last page with data: {candidate}.", flush=True)
            return candidate
        if low > high:
            print(f"    Last page with data: {candidate}.", flush=True)
            return candidate
    print(f"    Max outer iterations reached; last candidate: {candidate}.", flush=True)
    return candidate


def iter_all_segments() -> list[tuple[int, int, str, str]]:
    """Yield (category_id, subcategory_id, category_name, subcategory_name) from config."""
    path = os.path.join(_SCRIPT_DIR, "category_subcategories.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    raw = data.get("categories") or {}
    out = []
    for cid_str, cat_obj in raw.items():
        try:
            cid = int(cid_str)
        except ValueError:
            continue
        cat_name = (cat_obj or {}).get("name") or str(cid)
        subs = (cat_obj or {}).get("subcategories") or {}
        # Emit subcategories in order: specific ones first (sorted), then -1 ("all") last if listed in JSON
        sub_items = []
        for sid_str, sname in subs.items():
            try:
                sid = int(sid_str)
            except ValueError:
                continue
            sub_items.append((sid, sname or str(sid)))
        sub_items.sort(key=lambda x: (x[0] == -1, x[0]))  # -1 last
        for sid, sub_name in sub_items:
            out.append((cid, sid, cat_name, sub_name))
    return out


def load_existing_last_pages(path: str) -> dict[tuple[int, int], tuple[str, str, int]]:
    """Load existing last_pages.csv. Returns (category_id, subcategory_id) -> (category_name, subcategory_name, last_page)."""
    out: dict[tuple[int, int], tuple[str, str, int]] = {}
    if not os.path.isfile(path):
        return out
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                cid = int(row["category_id"])
                sid = int(row["subcategory_id"])
                last = int(row["last_page"])
            except (KeyError, ValueError):
                continue
            cat_name = row.get("category_name", str(cid))
            sub_name = row.get("subcategory_name", str(sid) if sid != -1 else "all")
            out[(cid, sid)] = (cat_name, sub_name, last)
    return out


def main():
    parser = argparse.ArgumentParser(description="Step 0: Find last page with data per category/subcategory; output last_pages.csv.")
    parser.add_argument(
        "--output", "-o",
        default=None,
        metavar="PATH",
        help="Output CSV path (default: last_pages.csv in project root).",
    )
    parser.add_argument(
        "--category",
        type=int,
        default=None,
        help="Run only this category ID.",
    )
    parser.add_argument(
        "--subcategory",
        type=int,
        default=None,
        help="Run only this subcategory ID (use with --category).",
    )
    parser.add_argument(
        "--all-only",
        action="store_true",
        help="Run only subcategory -1 ('all') for each category (4 segments total).",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test run: skip full probe; one segment per category (sprite/object, others 'all'), page 1 only.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Log every fetch and step.")
    args = parser.parse_args()

    if args.test:
        args.all_only = True
    # One segment per category; sprite uses object (5) because site often returns no listing for "all"
    TEST_SEGMENTS = [(0, 5, "sprite", "object"), (1, -1, "audioclip", "all"), (3, -1, "animationclip", "all"), (25, -1, "avataritem", "all")]

    ifwt = os.environ.get("MSW_IFWT", "").strip()
    if not ifwt:
        print("Set MSW_IFWT", file=sys.stderr)
        sys.exit(1)
    page_headers = build_headers(ifwt)
    page_headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    page_headers["Cookie"] = f"_ifwt={ifwt}"
    api_headers = build_headers(ifwt)

    if args.test:
        segments = TEST_SEGMENTS
    else:
        segments = iter_all_segments()
        if args.all_only:
            segments = [(c, s, cn, sn) for c, s, cn, sn in segments if s == -1]
        elif args.category is not None:
            segments = [(c, s, cn, sn) for c, s, cn, sn in segments if c == args.category]
            if args.subcategory is not None:
                segments = [(c, s, cn, sn) for c, s, cn, sn in segments if s == args.subcategory]
    if not segments:
        print("No segments to process.", file=sys.stderr)
        sys.exit(1)

    out_path = args.output or os.path.join(_PROJECT_ROOT, "last_pages.csv")

    # Merge with existing CSV: load current rows, then update/add only the segments we run.
    existing = load_existing_last_pages(out_path)
    if existing:
        print(f"Merging with existing {out_path} ({len(existing)} rows).", flush=True)

    results = []
    if args.test:
        print("Test mode: skipping full probe; checking page 1 only for each category (subcategory 'all').", flush=True)
    for category_id, subcategory_id, cat_name, sub_name in segments:
        display = f"{cat_name} / {sub_name}"
        print(f"--- {display} (category {category_id}, subcategory {subcategory_id}) ---", flush=True)
        if args.test:
            url = collect_mod.page_url(1, category_id, subcategory_id)
            html = collect_mod.fetch_page(url, page_headers)
            count = len(collect_mod._extract_ruids_from_listing_html(html, category_id)) if html else 0
            if args.verbose:
                print(f"      page 1 attempt 1: html={len(html) if html else 0} chars, count={count}", flush=True)
            if count == 0 and html and len(html) > 0:
                if args.verbose:
                    print(f"      page 1 attempt 2: html={len(html)} chars, count={count}", flush=True)
                time.sleep(RETRY_DELAY)
                html2 = collect_mod.fetch_page(url, page_headers)
                count = len(collect_mod._extract_ruids_from_listing_html(html2, category_id)) if html2 else 0
                if args.verbose:
                    print(f"      page 1 attempt 2: html={len(html2) if html2 else 0} chars, count={count}", flush=True)
                if count == 0 and html2:
                    html = html2
            # When we get 0 RUIDs, save first response and hint if it looks like the loading shell (session expired?)
            if count == 0 and html and not results:
                debug_path = os.path.join(os.path.dirname(out_path) or ".", "debug_response.html")
                try:
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"  Saved response ({len(html)} chars) to {debug_path} for inspection.", flush=True)
                except OSError:
                    pass
                if "loading-container" in html or 'id="loading"' in html:
                    print("  Response looks like the site loading shell (no listing data). Your _ifwt session may have expired.", flush=True)
                    print("  Copy a fresh _ifwt from the browser (F12 -> Application -> Cookies -> maplestoryworlds.nexon.com) and re-run.", flush=True)
            if count == 0 and args.verbose:
                print(f"  Page 1 had no data; writing last_page=1 anyway so collect can try.", flush=True)
            last = 1
            results.append((category_id, subcategory_id, cat_name, sub_name, last))
            print(f"  => last page: {last} (test: page 1 only)\n", flush=True)
            time.sleep(DELAY_SEARCH)
            continue
        total = collect_mod.get_api_total_for_segment(category_id, subcategory_id, api_headers)
        max_page = int(collect_mod.math.ceil(total / collect_mod.MAX_ITEMS_PER_PAGE)) if total else 8750
        if not results:
            print(f"API total: {total} -> max_page estimate: {max_page}\n", flush=True)
        first_empty = phase1_first_empty(category_id, subcategory_id, sub_name, max_page, page_headers, args.verbose)
        last = phase2_binary_search_to_last(
            category_id, subcategory_id, sub_name, first_empty, max_page, page_headers, args.verbose
        )
        if last is None:
            print(f"  => last page: (none)\n", flush=True)
            sys.exit(1)
        results.append((category_id, subcategory_id, cat_name, sub_name, last))
        print(f"  => last page: {last}\n", flush=True)
        time.sleep(DELAY_SEARCH)

    # Update existing with new results; keep all other rows unchanged.
    for (cid, sid, cat_name, sub_name, last) in results:
        existing[(cid, sid)] = (cat_name, sub_name, last)
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(("category_id", "subcategory_id", "category_name", "subcategory_name", "last_page"))
        for (cid, sid) in sorted(existing.keys()):
            cat_name, sub_name, last = existing[(cid, sid)]
            w.writerow((cid, sid, cat_name, sub_name, last))
    print(f"Wrote {out_path} ({len(existing)} rows, {len(results)} updated).", flush=True)


if __name__ == "__main__":
    main()
