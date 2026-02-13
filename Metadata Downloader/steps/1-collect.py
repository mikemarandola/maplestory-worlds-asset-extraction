#!/usr/bin/env python3
"""
Collect RUIDs from the live MapleStory Worlds resource site by category and page.
With --out CSV: writes RUID and Category name per row (Date, Format, Tags left empty for bulk fetch later).

Default: only adds RUIDs that do not already exist in the output file. Loads existing output
into seen, writes new rows to a temp file, then merges (existing + new) and replaces output
so the existing file is never corrupted by a crash.

URL pattern:
  https://maplestoryworlds.nexon.com/en/resource/?page=N&category=C&subCategory=S&type=text

Categories: 0=sprite, 3=animationclip, 1=audioclip, 25=avataritem
Sprite (0) is further bucketed by subCategory: 5=object, 6=foothold, 7=monster, 8=npc, 9=trap (from site).

Auth: interactive F/C/E/S/O or --ifwt / MSW_IFWT / --cookies (same as step 2).
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Run from project root (script directory). Steps use shared _api in same package.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from steps._api import (
    get_ifwt_interactive,
    load_cookies_from_file,
    build_headers,
    resource_detail_url,
    try_url,
    BASE_URL as API_BASE,
)
_HAS_DISCOVERY = True

# Category/subcategory data: loaded from category_subcategories.json (same dir as this script).
# The saved MapleStory Worlds.htm is a Nuxt SPA—the category tree lives in minified JS, not readable HTML.
# To find more subcategories: open the live site, expand a category, click a sub-item and check the URL (subCategory=...).
def _load_category_data() -> tuple[dict[int, str], dict[int, dict[int, str]]]:
    """Load categories and subcategories from JSON. Returns (CATEGORY_NAMES, SUBCATS_BY_CATEGORY)."""
    path = os.path.join(_SCRIPT_DIR, "category_subcategories.json")
    fallback_names = {0: "sprite", 3: "animationclip", 1: "audioclip", 25: "avataritem"}
    fallback_subs = {0: {5: "object", 6: "foothold", 7: "monster", 8: "npc", 9: "trap"}}
    if not os.path.isfile(path):
        subs_by_cat: dict[int, dict[int, str]] = {}
        for c in fallback_subs:
            subs_by_cat[c] = fallback_subs[c]
        return fallback_names, subs_by_cat
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return fallback_names, {c: fallback_subs.get(c, {-1: "all"}) for c in fallback_names}
    raw = data.get("categories") or {}
    names: dict[int, str] = {}
    subs_by_cat = {}
    for cid_str, cat_obj in raw.items():
        try:
            cid = int(cid_str)
        except ValueError:
            continue
        names[cid] = (cat_obj or {}).get("name") or str(cid)
        sub_raw = (cat_obj or {}).get("subcategories") or {}
        subs_by_cat[cid] = {}
        for sid_str, sname in sub_raw.items():
            try:
                sid = int(sid_str)
            except ValueError:
                continue
            subs_by_cat[cid][sid] = sname or str(sid)
    if not names:
        return fallback_names, {c: fallback_subs.get(c, {-1: "all"}) for c in fallback_names}
    for cid in names:
        if cid not in subs_by_cat:
            subs_by_cat[cid] = {}
    return names, subs_by_cat


CATEGORY_NAMES, SUBCATS_BY_CATEGORY = _load_category_data()
CSV_COLUMNS = ("RUID", "Category", "Subcategory", "Date", "Format", "Tags")


def iter_segments(categories: list[int], all_only: bool = False) -> list[tuple[int, int]]:
    """Yield (category, subcategory) to scrape.
    If all_only: only subcategory -1 ('all') per category (only for categories that list -1 in JSON).
    Else: specific subcategories first (sorted), then -1 ('all') at the end if listed in JSON."""
    out: list[tuple[int, int]] = []
    for c in categories:
        subs = SUBCATS_BY_CATEGORY.get(c) or {}
        if all_only:
            if -1 in subs:
                out.append((c, -1))
            continue
        sub_ids = [sid for sid in subs if sid != -1]
        for sub in sorted(sub_ids):
            out.append((c, sub))
        if -1 in subs:
            out.append((c, -1))
    return out


def load_last_pages(project_root: str, last_pages_path: str | None = None) -> dict[tuple[int, int], int]:
    """Load last_pages.csv. Returns (category_id, subcategory_id) -> last_page.
    If last_pages_path is set, load from that file; else load from project_root/LAST_PAGES_FILENAME.
    Raises FileNotFoundError if file missing. Caller must validate every segment has an entry with last_page >= 1."""
    path = os.path.join(project_root, LAST_PAGES_FILENAME) if not last_pages_path else last_pages_path
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Missing {LAST_PAGES_FILENAME}. Run step 0 first: python steps/0-find_last_pages.py")
    out: dict[tuple[int, int], int] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                cid = int(row["category_id"])
                sid = int(row["subcategory_id"])
                last = int(row["last_page"])
            except (KeyError, ValueError):
                raise ValueError(f"Invalid row in {LAST_PAGES_FILENAME}: {row}")
            if last < 1:
                raise ValueError(f"Invalid last_page {last} for ({cid}, {sid}) in {LAST_PAGES_FILENAME}; must be >= 1.")
            out[(cid, sid)] = last
    return out


def category_display_name(category: int, subcategory: int) -> str:
    """Display name for logging: e.g. 'sprite / object' or 'audioclip'."""
    base = CATEGORY_NAMES.get(category, str(category))
    subs = SUBCATS_BY_CATEGORY.get(category) or {}
    sub_name = subs.get(subcategory) if subcategory in subs else None
    if sub_name and subcategory != -1 and sub_name != "all":
        return f"{base} / {sub_name}"
    return base


def get_category_and_subcategory_names(category: int, subcategory: int) -> tuple[str, str]:
    """Return (category_name, subcategory_name) for CSV columns. Subcategory -1 is 'all' for all types."""
    cat_name = CATEGORY_NAMES.get(category, str(category))
    if subcategory == -1:
        return (cat_name, "all")
    sub_name = (SUBCATS_BY_CATEGORY.get(category) or {}).get(subcategory) or str(subcategory)
    return (cat_name, sub_name)


def _name_to_segment() -> dict[tuple[str, str], tuple[int, int]]:
    """Build (category_name, subcategory_name) -> (category_id, subcategory_id) for loading CSV. Subcategory -1 is 'all'."""
    out: dict[tuple[str, str], tuple[int, int]] = {}
    for cat_id, cat_name in CATEGORY_NAMES.items():
        subs = SUBCATS_BY_CATEGORY.get(cat_id) or {}
        for sub_id, sub_name in subs.items():
            out[(cat_name, sub_name)] = (cat_id, sub_id)
        out[(cat_name, "")] = (cat_id, -1)
        out[(cat_name, "all")] = (cat_id, -1)
    return out


def load_existing_ruids_from_csv(path: str) -> set[tuple[str, int, int]]:
    """Load (ruid, category, subcategory) from existing CSV so we skip them and never overwrite."""
    seen: set[tuple[str, int, int]] = set()
    name_to_seg = _name_to_segment()
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None or tuple(header[:3]) != CSV_COLUMNS[:3]:
                return seen
            for row in reader:
                if len(row) < 3:
                    continue
                ruid, cat_name, sub_name = (row[0].strip(), row[1].strip(), row[2].strip())
                if not ruid:
                    continue
                key = (cat_name, sub_name)
                if key not in name_to_seg:
                    continue
                cat_id, sub_id = name_to_seg[key]
                seen.add((ruid.lower(), cat_id, sub_id))
    except OSError:
        pass
    return seen


# RUID: 32 hex chars. Prefer <div class="list_resource"> + live CDN URLs only; if that div is
# missing (client-rendered), fall back to "guid"/CDN anywhere in HTML (how we got 122k+ before).
RUID_IN_LISTING_RE = re.compile(
    r"mod-resource-search-images\.dn\.nexoncdn[^/]*/maplestory_world/([a-f0-9]{32})\.(gif|png|jpg|jpeg|webp|mp3|ogg|wav|m4a|webm|mp4)",
    re.IGNORECASE,
)
# Audioclip pages: extract RUIDs only from live URLs in <audio> / <source> (no local/saved paths).
RUID_IN_AUDIO_SRC_RE = re.compile(
    r'<audio[^>]+src=["\']https?://[^"\']*/([a-f0-9]{32})\.(ogg|wav|mp3|m4a)["\']',
    re.IGNORECASE,
)
RUID_IN_AUDIO_SOURCE_RE = re.compile(
    r'<source[^>]+src=["\']https?://[^"\']*/([a-f0-9]{32})\.(ogg|wav|mp3|m4a)["\']',
    re.IGNORECASE,
)
# Fallback when list_resource not in HTML: JSON "guid":"..." or 'guid':'...' (embedded page data)
RUID_IN_JSON_RE = re.compile(
    r'["\'](?:guid|ruid|resourceId)["\']\s*:\s*["\']([a-f0-9]{32})["\']',
    re.IGNORECASE,
)
# Audioclip: RUIDs in embedded path data (e.g. .../32hex.ogg.mod). Site HTML contains these even when client-rendered.
RUID_AUDIO_IN_HTML_RE = re.compile(
    r"([a-f0-9]{32})\.(ogg|mp3|wav|m4a)(?:\.[a-z]+)?",
    re.IGNORECASE,
)
# Start of the listing div (class contains list_resource)
LIST_RESOURCE_DIV_START = re.compile(
    r'<div[^>]*\bclass="[^"]*list_resource[^"]*"[^>]*>',
    re.IGNORECASE,
)

BASE_URL = "https://maplestoryworlds.nexon.com/en/resource/"
CATEGORIES = (0, 1, 3, 25)
RETRY_DELAY = 2.0  # minimum seconds between retry attempts for the same page (per-page, not global)
MAX_PAGE_ATTEMPTS = 2  # max attempts per page for normal retry list
INDEFINITE_RETRY_MAX_ATTEMPTS = 5  # max retries for pages that had a later page with content (at least RETRY_DELAY apart each)
RETRY_PAGE_BUFFER = 20  # only retry a failed page if page <= max_page_with_content + this (avoid retrying past-the-end pages)
LAST_PAGES_FILENAME = "last_pages.csv"  # step 0 output; required for collect (no fallback)
SKEPTICAL_PAGE_BUFFER = 10  # collect fetches up to last_page + this to verify no extra pages
# Nominal max items per page. Stop only when a page has FEWER than this (last page).
# Do NOT stop when a page has 150 or more—pages may have extras (>150); only stop when len(found) < MAX_ITEMS_PER_PAGE.
MAX_ITEMS_PER_PAGE = 150


def _extract_list_resource_html(html: str) -> str | None:
    """Extract the inner HTML of the first <div class="list_resource">. Returns None if not found."""
    m = LIST_RESOURCE_DIV_START.search(html)
    if not m:
        return None
    content_start = m.end()
    depth = 1
    i = content_start
    while depth > 0 and i < len(html):
        next_close = html.find("</div>", i)
        next_open = html.find("<div", i)
        if next_open == -1:
            next_open = len(html)
        if next_close == -1:
            next_close = len(html)
        if next_close < next_open:
            depth -= 1
            if depth == 0:
                return html[content_start:next_close]
            i = next_close + 6
        else:
            depth += 1
            i = next_open + 4
    return None


def page_url(page: int, category: int, subcategory: int = -1) -> str:
    """URL for listing page; &keyword matches browser and can be required for full list_resource."""
    return f"{BASE_URL}?page={page}&category={category}&subCategory={subcategory}&type=text&keyword"


def extract_ruids_from_html(html: str) -> dict[str, str]:
    """Return dict of ruid -> extension. Uses live URLs only (no local/saved paths).
    In list_resource: CDN URLs (images/video) and <audio>/<source> src with https?://... for audioclips.
    If list_resource is missing (client-rendered), fall back to \"guid\" in HTML (embedded page data)."""
    found: dict[str, str] = {}
    inner = _extract_list_resource_html(html)
    search_in = inner if inner is not None else html
    for m in RUID_IN_LISTING_RE.finditer(search_in):
        ruid, ext = m.group(1).lower(), m.group(2).lower()
        if ruid not in found or (found[ruid] == "" and ext):
            found[ruid] = ext
    for m in RUID_IN_AUDIO_SRC_RE.finditer(search_in):
        ruid, ext = m.group(1).lower(), m.group(2).lower()
        if ruid not in found or (found[ruid] == "" and ext):
            found[ruid] = ext
    for m in RUID_IN_AUDIO_SOURCE_RE.finditer(search_in):
        ruid, ext = m.group(1).lower(), m.group(2).lower()
        if ruid not in found or (found[ruid] == "" and ext):
            found[ruid] = ext
    if not found and inner is None:
        for m in RUID_IN_JSON_RE.finditer(html):
            ruid = m.group(1).lower()
            if ruid not in found:
                found[ruid] = ""
    return found


def extract_ruids_from_audioclip_html(html: str) -> dict[str, str]:
    """Extract RUID -> extension from audioclip page HTML (embedded path data: 32hex.ogg/.mp3/.wav/.m4a)."""
    found: dict[str, str] = {}
    for m in RUID_AUDIO_IN_HTML_RE.finditer(html):
        ruid, ext = m.group(1).lower(), m.group(2).lower()
        if ruid not in found or (found[ruid] == "" and ext):
            found[ruid] = ext
    return found


def _extract_ruids_from_listing_html(html: str | None, category: int) -> dict[str, str]:
    """Parse listing HTML into ruid -> extension. Audioclip (category=1) uses embedded path pattern; others use list_resource/CDN/guid."""
    if not html:
        return {}
    if category == 1:
        return extract_ruids_from_audioclip_html(html)
    return extract_ruids_from_html(html)


def _throttle_request(throttle_lock: threading.Lock, last_request_time: list, interval: float) -> None:
    """Ensure at least `interval` seconds since last request start. Call before fetch_page when using parallel workers."""
    with throttle_lock:
        now = time.monotonic()
        wait = max(0.0, interval - (now - last_request_time[0]))
        last_request_time[0] = now + wait
    if wait > 0:
        time.sleep(wait)


def fetch_page(url: str, headers: dict, timeout: int = 20) -> str | None:
    """Fetch page HTML only. Returns body or None on failure."""
    try:
        req = Request(url, headers=headers, method="GET")
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        try:
            return e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        return None
    except URLError:
        return None
    except Exception:
        return None


# Listing: we use the website (page_url + fetch_page). Pagination and category/subCategory are in the URL.
# For audioclip, RUIDs are in embedded path data (32hex.ogg etc.); for others, list_resource div / CDN URLs / JSON guid.
# The API (fetch_listing_via_api) is kept for reference but not used for listing; API is only for resource detail (step 2).
API_LISTING_COUNT = 500


def fetch_listing_via_api(
    page: int, category: int, subcategory: int, api_headers: dict, timeout: int = 25
) -> tuple[dict[str, str], int | None]:
    """Fetch one page of listing from mverse-api. Returns (ruid -> extension, total_count or None).
    Used for all categories (0,1,3,25); the site HTML is client-rendered and does not embed RUIDs."""
    url = f"{API_BASE}/resource/v1/search?page={page}&count={API_LISTING_COUNT}&category={category}&subCategory={subcategory}"
    try:
        req = Request(url, headers=api_headers, method="GET")
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, Exception):
        return {}, None
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {}, None
    if data.get("code") != 0:
        return {}, None
    payload = data.get("data") or {}
    matches = payload.get("matches") or []
    total_count: int | None = payload.get("totalCount") or payload.get("total_count") or payload.get("total")
    if total_count is not None and not isinstance(total_count, int):
        total_count = None
    found: dict[str, str] = {}
    for m in matches:
        guid = (m.get("guid") or m.get("ruid") or "").strip().lower()
        if not guid or len(guid) != 32:
            continue
        path = m.get("path") or ""
        url_val = m.get("url") or ""
        segment = (path or url_val).split("/")[-1] if (path or url_val) else ""
        ext = segment.rsplit(".", 1)[-1].lower() if "." in segment else ""
        if guid not in found or (found[guid] == "" and ext):
            found[guid] = ext
    return found, total_count


def get_api_total_for_segment(category: int, subcategory: int, api_headers: dict, timeout: int = 15) -> int | None:
    """Fetch API totalMatchCount for (category, subcategory). Used for max_page_cap so we don't fetch past category end.
    Works for all categories including audioclip (category=1). Returns None on failure."""
    url = f"{API_BASE}/resource/v1/search?page=1&count=1&category={category}&subCategory={subcategory}"
    try:
        req = Request(url, headers=api_headers, method="GET")
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, Exception):
        return None
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return None
    if data.get("code") != 0:
        return None
    payload = data.get("data") or {}
    total = payload.get("totalMatchCount")
    if total is not None and isinstance(total, int) and total >= 0:
        return total
    return None


def fetch_resource_detail(ruid: str, api_headers: dict) -> tuple[str, str, str] | None:
    """Fetch one RUID from the resource API. Returns (date, format, tags_str) or None."""
    if not _HAS_DISCOVERY:
        return None
    url = resource_detail_url(ruid)
    ok, data, code = try_url(url, api_headers)
    if not ok or code != 200 or not isinstance(data, dict) or data.get("code") != 0:
        return None
    matches = data.get("data", {}).get("matches") or []
    if not matches:
        return None
    m = matches[0]
    mtime = m.get("mtime") or ""
    path = m.get("path") or ""
    # Format: extension of filename (e.g. model.mod, gif, mp3)
    segment = path.split("/")[-1] if path else ""
    format_str = segment.rsplit(".", 1)[-1].lower() if "." in segment else ""
    tags_list = m.get("tags") or []
    tags_str = ", ".join(str(t) for t in tags_list)
    return (mtime, format_str, tags_str)


def _process_one_segment(
    segment: tuple[int, int],
    page_headers: dict,
    api_headers: dict,
    delay: float,
    verbose: bool,
    pages_per_category: int | None,
    writer: csv.writer,
    f,
    lock: threading.Lock,
    seen: set,
    seen_per_ruid_cat: set,
    rows_written_list: list,
    throttle_lock: threading.Lock | None,
    last_request_time: list | None,
    global_retry_list: list,
    total_pages_fetched: list | None = None,
    max_total_pages: int | None = None,
    progress_interval: int = 1,
    max_page_with_content: dict | None = None,
    max_page_cap: int | None = None,
    step0_last_page: int | None = None,
) -> int:
    """Process one (category, subcategory) sequentially. Stops at max_page_cap (from step 0 last_pages + buffer), last page, or test limit. No API total used."""
    category, subcategory = segment
    if max_page_with_content is None:
        max_page_with_content = {}
    display_name = category_display_name(category, subcategory)
    cat_name, sub_name = get_category_and_subcategory_names(category, subcategory)
    if verbose and max_page_cap is not None:
        print(f"  Max page cap for {display_name}: {max_page_cap} (from last_pages + {SKEPTICAL_PAGE_BUFFER}).", file=sys.stderr)
    pages_done = 0
    page = 1
    retry_later: list[tuple[int, int]] = []

    while True:
        if max_total_pages is not None and total_pages_fetched is not None and total_pages_fetched[0] >= max_total_pages:
            break
        if pages_per_category is not None and pages_done >= pages_per_category:
            if verbose:
                print(f"  --test: done {pages_done} pages for {display_name}.", file=sys.stderr)
            break
        if max_page_cap is not None and page > max_page_cap:
            if verbose:
                print(f"  Reached max page cap (page {page} > {max_page_cap}), ending segment.", file=sys.stderr)
            break
        url = page_url(page, category, subcategory)
        if verbose and (page - 1) % progress_interval == 0:
            print(f"Fetching {display_name} page {page} ...", file=sys.stderr)
        if throttle_lock is not None and last_request_time is not None:
            _throttle_request(throttle_lock, last_request_time, delay)
        html = fetch_page(url, page_headers)
        found = _extract_ruids_from_listing_html(html, category)
        if html is None:
            retry_later.append((page, 1))
            if verbose:
                print(f"  Failed to fetch page {page}, added to retry list.", file=sys.stderr)
            page += 1
            continue
        if len(found) == 0:
            retry_later.append((page, 1))
            if verbose:
                print(f"  No RUIDs on page {page}, added to retry list.", file=sys.stderr)
            page += 1
            continue
        pages_done += 1
        max_page_with_content[(category, subcategory)] = max(max_page_with_content.get((category, subcategory), 0), page)
        if max_total_pages is not None and total_pages_fetched is not None:
            with lock:
                total_pages_fetched[0] += 1
                if total_pages_fetched[0] >= max_total_pages:
                    if verbose:
                        print(f"  --test: reached {max_total_pages} total pages (2x workers), stopping segment.", file=sys.stderr)
                    break
        for ruid in found:
            key = (ruid, category, subcategory)
            with lock:
                if subcategory == -1:
                    if (ruid, category) in seen_per_ruid_cat:
                        continue
                    seen_per_ruid_cat.add((ruid, category))
                else:
                    if key in seen:
                        continue
                    seen_per_ruid_cat.add((ruid, category))
                seen.add(key)
                writer.writerow([ruid, cat_name, sub_name, "", "", ""])
                f.flush()
                os.fsync(f.fileno())
                rows_written_list[0] += 1
                if verbose and rows_written_list[0] % 50 == 0:
                    print(f"  Wrote {rows_written_list[0]} rows to CSV.", file=sys.stderr)
        if len(found) < MAX_ITEMS_PER_PAGE:
            # End segment only when we've reached at least step0_last_page (for all subcategories). Never stop before that.
            is_last = (step0_last_page is None or page >= step0_last_page) and (len(found) == 0 or len(found) < MAX_ITEMS_PER_PAGE)
            if is_last:
                if verbose:
                    print(f"  Page {page} has {len(found)} items (< {MAX_ITEMS_PER_PAGE}), last page for this segment.", file=sys.stderr)
                break
            if verbose:
                print(f"  Page {page} has {len(found)} items (< {MAX_ITEMS_PER_PAGE}); continuing to next page (step0 last_page={step0_last_page}).", file=sys.stderr)
        page += 1
        time.sleep(delay)

    # Failed pages are added to global_retry_list and processed at end of full run (after all segments)
    for p, _ in retry_later:
        global_retry_list.append((category, subcategory, p, 1))
    time.sleep(delay)
    return pages_done


def _worker_fetch_next_page(
    segment: tuple[int, int],
    page_headers: dict,
    api_headers: dict,
    delay: float,
    verbose: bool,
    pages_per_category: int | None,
    writer: csv.writer,
    f,
    lock: threading.Lock,
    seen: set,
    seen_per_ruid_cat: set,
    rows_written_list: list,
    next_page: list,
    segment_done: list,
    throttle_lock: threading.Lock,
    last_request_time: list,
    global_retry_list: list,
    global_retry_lock: threading.Lock,
    total_pages_fetched: list | None = None,
    max_total_pages: int | None = None,
    progress_interval: int = 1,
    max_page_with_content: dict | None = None,
    max_page_cap: int | None = None,
    step0_last_page: int | None = None,
) -> None:
    """Worker: repeatedly take the next page number, fetch it, write RUIDs. Stops at max_page_cap or last page. Only treat partial page as last when empty or page >= step0_last_page."""
    category, subcategory = segment
    if max_page_with_content is None:
        max_page_with_content = {}
    display_name = category_display_name(category, subcategory)
    cat_name, sub_name = get_category_and_subcategory_names(category, subcategory)
    while True:
        with lock:
            if segment_done[0]:
                return
            if max_total_pages is not None and total_pages_fetched is not None and total_pages_fetched[0] >= max_total_pages:
                segment_done[0] = True
                return
            if pages_per_category is not None and next_page[0] > pages_per_category:
                segment_done[0] = True
                return
            page = next_page[0]
            if max_page_cap is not None and page > max_page_cap:
                segment_done[0] = True
                return
            next_page[0] += 1
        if segment_done[0]:
            return
        url = page_url(page, category, subcategory)
        if verbose and (page - 1) % progress_interval == 0:
            print(f"Fetching {display_name} page {page} ...", file=sys.stderr)
        _throttle_request(throttle_lock, last_request_time, delay)
        html = fetch_page(url, page_headers)
        found = _extract_ruids_from_listing_html(html, category)
        if segment_done[0]:
            return
        if html is None:
            with global_retry_lock:
                global_retry_list.append((category, subcategory, page, 1))
            if verbose:
                print(f"  Failed to fetch page {page}, added to global retry list.", file=sys.stderr)
            time.sleep(delay)
            continue
        if len(found) == 0:
            with global_retry_lock:
                global_retry_list.append((category, subcategory, page, 1))
            if verbose:
                print(f"  No RUIDs on page {page}, added to global retry list.", file=sys.stderr)
            time.sleep(delay)
            continue
        with lock:
            max_page_with_content[(category, subcategory)] = max(max_page_with_content.get((category, subcategory), 0), page)
            if max_total_pages is not None and total_pages_fetched is not None:
                total_pages_fetched[0] += 1
                if total_pages_fetched[0] >= max_total_pages:
                    segment_done[0] = True
        for ruid in found:
            key = (ruid, category, subcategory)
            with lock:
                if subcategory == -1:
                    if (ruid, category) in seen_per_ruid_cat:
                        continue
                    seen_per_ruid_cat.add((ruid, category))
                else:
                    if key in seen:
                        continue
                    seen_per_ruid_cat.add((ruid, category))
                seen.add(key)
                writer.writerow([ruid, cat_name, sub_name, "", "", ""])
                f.flush()
                os.fsync(f.fileno())
                rows_written_list[0] += 1
                if verbose and rows_written_list[0] % 50 == 0:
                    print(f"  Wrote {rows_written_list[0]} rows to CSV.", file=sys.stderr)
        if len(found) < MAX_ITEMS_PER_PAGE:
            # End segment only when we've reached at least step0_last_page (for all subcategories). Never stop before that.
            is_last = (step0_last_page is None or page >= step0_last_page) and (len(found) == 0 or len(found) < MAX_ITEMS_PER_PAGE)
            if is_last:
                with lock:
                    segment_done[0] = True
                if verbose:
                    print(f"  Page {page} has {len(found)} items (< {MAX_ITEMS_PER_PAGE}), last page for this segment.", file=sys.stderr)
            elif verbose:
                print(f"  Page {page} has {len(found)} items (< {MAX_ITEMS_PER_PAGE}); continuing (step0 last_page={step0_last_page}).", file=sys.stderr)
        time.sleep(delay)


def _process_segment_parallel_pages(
    segment: tuple[int, int],
    page_headers: dict,
    api_headers: dict,
    delay: float,
    verbose: bool,
    pages_per_category: int | None,
    writer: csv.writer,
    f,
    lock: threading.Lock,
    seen: set,
    seen_per_ruid_cat: set,
    rows_written_list: list,
    workers: int,
    throttle_lock: threading.Lock,
    last_request_time: list,
    global_retry_list: list,
    global_retry_lock: threading.Lock,
    total_pages_fetched: list | None = None,
    max_total_pages: int | None = None,
    progress_interval: int = 1,
    max_page_with_content: dict | None = None,
    max_page_cap: int | None = None,
    step0_last_page: int | None = None,
) -> None:
    """Process one segment with N workers. Stops at max_page_cap (from step 0 last_pages + buffer). No API total used."""
    category, subcategory = segment
    if max_page_with_content is None:
        max_page_with_content = {}
    if verbose and max_page_cap is not None:
        print(f"  Max page cap for {category_display_name(category, subcategory)}: {max_page_cap} (from last_pages + {SKEPTICAL_PAGE_BUFFER}).", file=sys.stderr)
    next_page: list[int] = [1]
    segment_done: list[bool] = [False]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _worker_fetch_next_page,
                segment,
                page_headers,
                api_headers,
                delay,
                verbose,
                pages_per_category,
                writer,
                f,
                lock,
                seen,
                seen_per_ruid_cat,
                rows_written_list,
                next_page,
                segment_done,
                throttle_lock,
                last_request_time,
                global_retry_list,
                global_retry_lock,
                total_pages_fetched,
                max_total_pages,
                progress_interval,
                max_page_with_content,
                max_page_cap,
                step0_last_page,
            )
            for _ in range(workers)
        ]
        for _ in as_completed(futures):
            pass
    time.sleep(delay)


def get_auth(args: argparse.Namespace):
    """Resolve _ifwt and build request headers. Order: --ifwt > MSW_IFWT > any browser > cookies file."""
    ifwt = None
    source = None
    cookie_header = ""

    # 1) Explicit --ifwt or env MSW_IFWT
    ifwt = args.ifwt or os.environ.get("MSW_IFWT")
    if ifwt:
        source = "MSW_IFWT" if os.environ.get("MSW_IFWT") and not args.ifwt else "--ifwt"

    # 2) Interactive: choose browser (F/C/E/S) or paste token (O)
    if not ifwt and _HAS_DISCOVERY and not args.no_browser:
        ifwt = get_ifwt_interactive(verbose=args.verbose)
        if ifwt:
            source = "browser/token"

    # 3) Cookie file
    if args.cookies:
        if _HAS_DISCOVERY:
            cookie_header = load_cookies_from_file(args.cookies)
        else:
            with open(args.cookies, "r", encoding="utf-8") as f:
                cookie_header = f.read().strip()
            if cookie_header.lower().startswith("cookie:"):
                cookie_header = cookie_header[7:].strip()
        if not ifwt and cookie_header:
            for part in cookie_header.split(";"):
                part = part.strip()
                if part.startswith("_ifwt="):
                    ifwt = part.split("=", 1)[1].strip()
                    source = "cookies file"
                    break

    # Ensure Cookie header includes _ifwt so the page gets the session
    if ifwt:
        if not cookie_header:
            cookie_header = f"_ifwt={ifwt}"
        elif "_ifwt=" not in cookie_header:
            cookie_header = f"_ifwt={ifwt}; {cookie_header}"

    # API headers (Accept: application/json) for resource detail calls
    if _HAS_DISCOVERY:
        api_headers = build_headers(ifwt, cookie_header if cookie_header else None)
    else:
        api_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://maplestoryworlds.nexon.com/",
        }
        if ifwt:
            api_headers["x-mverse-ifwt"] = ifwt
        if cookie_header:
            api_headers["Cookie"] = cookie_header

    # Page headers (Accept: text/html) for listing pages; ensure Cookie is set so server returns full HTML
    page_headers = dict(api_headers)
    page_headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    if "Accept-Language" not in page_headers:
        page_headers["Accept-Language"] = "en-US,en;q=0.9"
    if ifwt and "Cookie" not in page_headers:
        page_headers["Cookie"] = f"_ifwt={ifwt}"

    if args.verbose:
        if ifwt:
            print(f"Auth: using _ifwt from {source} (length={len(ifwt)})", file=sys.stderr)
        else:
            print(
                "Auth: no _ifwt. Run without --no-browser to select a browser or paste token, or use --ifwt or MSW_IFWT or --cookies.",
                file=sys.stderr,
            )
    return page_headers, api_headers, ifwt


def main():
    ap = argparse.ArgumentParser(
        description="Collect RUIDs from resource pages; with --out CSV, fetch detail (date, format, tags) per RUID and write incrementally."
    )
    ap.add_argument("--ifwt", "-i", help="Session token (or set MSW_IFWT)")
    ap.add_argument("--cookies", "-c", help="Cookie file (optional)")
    ap.add_argument("--no-browser", action="store_true", help="Do not prompt for browser/token (use --ifwt or MSW_IFWT)")
    ap.add_argument("--categories", default="0,1,3,25", help="Comma-separated category IDs (default: 0,1,3,25)")
    ap.add_argument("--delay", type=float, default=0.5, help="Minimum seconds between the start of each page request (default: 0.5). Throttles parallel workers.")
    ap.add_argument("--out", "-o", help="Output CSV file (RUID, Category, Subcategory, Date, Format, Tags). Writes incrementally.")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print progress")
    ap.add_argument(
        "--test",
        action="store_true",
        help="Test run: one page per segment (one page from each category/subcategory).",
    )
    ap.add_argument(
        "--last-pages",
        metavar="PATH",
        help="Path to last_pages.csv (default: project root). Use for test runs writing to a test directory.",
    )
    _nproc = os.cpu_count() or 2
    _default_workers = max(1, _nproc // 2)
    ap.add_argument(
        "--workers", "-w",
        type=int,
        default=_default_workers,
        metavar="N",
        help=f"Within each segment, N workers fetch pages in parallel: they share a page queue (1, 2, 3, ...); each takes the next page when free. Segments run in order. Default: {_default_workers} (half of {_nproc} cores). Pass a larger N to use more cores.",
    )
    ap.add_argument(
        "--all-only",
        action="store_true",
        help="Only fetch subcategory -1 ('all') per category (faster; use when you do not need per-subcategory breakdown).",
    )
    args = ap.parse_args()

    _green = "\033[92m" if hasattr(sys.stderr, "isatty") and sys.stderr.isatty() else ""
    _reset = "\033[0m" if _green else ""
    print(f"{_green}Step 1: Collect RUIDs starting...{_reset}", file=sys.stderr)
    sys.stderr.flush()

    categories = [int(c.strip()) for c in args.categories.split(",") if c.strip()]
    for c in categories:
        if c not in CATEGORY_NAMES and args.verbose:
            print(f"Note: category {c} has no display name (will show as '{c}').", file=sys.stderr)

    csv_mode = bool(args.out)
    # Before any real work (auth, opening files): require last_pages.csv and validate every segment.
    if csv_mode:
        try:
            last_pages = load_last_pages(_PROJECT_ROOT, args.last_pages)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        # Test mode with custom last_pages (e.g. test/last_pages.csv): only run segments in that file
        if args.test and args.last_pages:
            segments_list = sorted(s for s in last_pages if s[0] in categories)
        else:
            segments_list = list(iter_segments(categories, all_only=args.all_only))
        missing = [s for s in segments_list if s not in last_pages]
        if missing:
            print(f"Error: {LAST_PAGES_FILENAME} is missing last_page for segment(s): {missing}. Run step 0 for all categories/subcategories.", file=sys.stderr)
            sys.exit(1)
        invalid = [s for s in segments_list if last_pages[s] < 1]
        if invalid:
            print(f"Error: {LAST_PAGES_FILENAME} has invalid last_page (< 1) for segment(s): {invalid}.", file=sys.stderr)
            sys.exit(1)
        print(f"Last pages OK: {LAST_PAGES_FILENAME} has valid entry for all {len(segments_list)} segment(s).", file=sys.stderr)
        sys.stderr.flush()

    page_headers, api_headers, ifwt = get_auth(args)
    print("Auth OK. Loading segments...", file=sys.stderr)
    sys.stderr.flush()

    # Test mode: one page per segment (one page from each category/subcategory)
    workers = max(1, min(args.workers, 32)) if args.out else 1
    max_total_pages = None
    pages_per_category = 1 if args.test else None

    if csv_mode:
        # CSV mode: only add RUIDs that don't exist in output. Write new rows to temp file, then merge/replace.
        out_path = args.out
        if args.all_only:
            print("Using --all-only: fetching only subcategory 'all' (-1) per category.", file=sys.stderr)
        print(f"Opening {out_path} ({len(segments_list)} segments)...", file=sys.stderr)
        sys.stderr.flush()
        if os.path.exists(out_path):
            print("  Loading existing RUIDs from output file (may take a while for large files)...", file=sys.stderr)
            sys.stderr.flush()
            seen = load_existing_ruids_from_csv(out_path)
            if args.verbose and seen:
                print(f"  Loaded {len(seen)} existing RUIDs from CSV (will skip; only new rows written to temp, then merge).", file=sys.stderr)
        else:
            seen = set()
        # For "all" (-1): only add RUID if (ruid, category) not already seen from a prior specific sub
        seen_per_ruid_cat: set[tuple[str, int]] = {(r, c) for (r, c, s) in seen}
        fd, temp_path = tempfile.mkstemp(suffix=".csv", prefix="collect_ruids_", dir=os.path.dirname(out_path) or ".")
        os.close(fd)  # close so we open by path; ensures writes go to the file at temp_path
        f = open(temp_path, "w", newline="", encoding="utf-8")
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        f.flush()
        os.fsync(f.fileno())
        rows_written_list: list[int] = [0]
        workers = max(1, min(args.workers, 32))
        total_pages_fetched: list[int] = [0]

        lock = threading.Lock()
        throttle_lock = threading.Lock()
        global_retry_lock = threading.Lock()
        last_request_time: list[float] = [0.0]
        global_retry_list: list[tuple[int, int, int, int]] = []
        max_page_with_content: dict[tuple[int, int], int] = {}  # (cat, sub) -> max page num that returned RUIDs
        if args.test and pages_per_category is not None:
            print(f"Test mode: fetching one page per segment ({len(segments_list)} segments).", file=sys.stderr)
            sys.stderr.flush()
        print("Starting fetch (first segment)...", file=sys.stderr)
        sys.stderr.flush()
        if workers <= 1:
            for segment in segments_list:
                if max_total_pages is not None and total_pages_fetched[0] >= max_total_pages:
                    break
                seg_cap = last_pages[segment] + SKEPTICAL_PAGE_BUFFER
                _process_one_segment(
                    segment,
                    page_headers,
                    api_headers,
                    args.delay,
                    args.verbose,
                    pages_per_category,
                    writer,
                    f,
                    lock,
                    seen,
                    seen_per_ruid_cat,
                    rows_written_list,
                    throttle_lock,
                    last_request_time,
                    global_retry_list,
                    total_pages_fetched,
                    max_total_pages,
                    progress_interval=workers,
                    max_page_with_content=max_page_with_content,
                    max_page_cap=seg_cap,
                    step0_last_page=last_pages[segment],
                )
        else:
            if args.verbose:
                print(f"Using {workers} workers: pages within each segment fetched in order (1, 2, 3, ...) in parallel.", file=sys.stderr)
            print(f"Progress logs every {workers} page(s) to reduce noise; all pages 1, 2, 3, ... are fetched.", file=sys.stderr)
            for segment in segments_list:
                if max_total_pages is not None and total_pages_fetched[0] >= max_total_pages:
                    break
                seg_cap = last_pages[segment] + SKEPTICAL_PAGE_BUFFER
                _process_segment_parallel_pages(
                    segment,
                    page_headers,
                    api_headers,
                    args.delay,
                    args.verbose,
                    pages_per_category,
                    writer,
                    f,
                    lock,
                    seen,
                    seen_per_ruid_cat,
                    rows_written_list,
                    workers,
                    throttle_lock,
                    last_request_time,
                    global_retry_list,
                    global_retry_lock,
                    total_pages_fetched,
                    max_total_pages,
                    progress_interval=workers,
                    max_page_with_content=max_page_with_content,
                    max_page_cap=seg_cap,
                    step0_last_page=last_pages[segment],
                )

        # Pages that failed but had a later page with content for same (cat, sub) get up to INDEFINITE_RETRY_MAX_ATTEMPTS retries
        indefinite_retry_set: set[tuple[int, int, int]] = {
            (cat, sub, p) for (cat, sub, p, _) in global_retry_list
            if max_page_with_content.get((cat, sub), 0) > p
        }
        if indefinite_retry_set and args.verbose:
            print(f"Indefinite retry: {len(indefinite_retry_set)} page(s) had a later page with content; will retry up to {INDEFINITE_RETRY_MAX_ATTEMPTS} times each (at least {RETRY_DELAY}s apart per page).", file=sys.stderr)

        # Global retry pass: process failed pages; wait at least RETRY_DELAY since last attempt for that same page before retrying
        last_attempt_time: dict[tuple[int, int, int], float] = {}  # (cat, sub, page) -> time.monotonic()
        failed_indefinite_segments: set[tuple[int, int]] = set()  # (cat, sub) that had a page fail after indefinite retry
        if global_retry_list:
            print(f"Global retry pass: {len(global_retry_list)} failed page(s) from all segments.", file=sys.stderr)
            sys.stderr.flush()
            retry_queue: deque[tuple[int, int, int, int]] = deque(global_retry_list)
            while retry_queue:
                category, subcategory, p, attempts = retry_queue.popleft()
                max_p = max_page_with_content.get((category, subcategory), 0)
                if p > max_p + RETRY_PAGE_BUFFER:
                    if args.verbose:
                        print(f"  Skipping {category_display_name(category, subcategory)} page {p} (past max page {max_p} + buffer {RETRY_PAGE_BUFFER}).", file=sys.stderr)
                    continue
                display_name = category_display_name(category, subcategory)
                cat_name, sub_name = get_category_and_subcategory_names(category, subcategory)
                is_indefinite = (category, subcategory, p) in indefinite_retry_set
                max_attempts = INDEFINITE_RETRY_MAX_ATTEMPTS if is_indefinite else MAX_PAGE_ATTEMPTS
                key = (category, subcategory, p)
                now = time.monotonic()
                # Only wait 2s before retrying the SAME page again; different pages run back-to-back (subject to throttle).
                if key in last_attempt_time:
                    last = last_attempt_time[key]
                    wait = max(0.0, RETRY_DELAY - (now - last))
                    if wait > 0:
                        time.sleep(wait)
                print(f"  Retrying {display_name} page {p} (attempt {attempts + 1}/{max_attempts}{' [indefinite]' if is_indefinite else ''}) ...", file=sys.stderr)
                sys.stderr.flush()
                _throttle_request(throttle_lock, last_request_time, args.delay)
                url = page_url(p, category, subcategory)
                html = fetch_page(url, page_headers)
                found = _extract_ruids_from_listing_html(html, category)
                last_attempt_time[key] = time.monotonic()  # record after attempt so next retry is at least RETRY_DELAY later
                if html is None:
                    can_retry = (attempts + 1) < max_attempts
                    if can_retry:
                        retry_queue.append((category, subcategory, p, attempts + 1))
                        if args.verbose:
                            print(f"    Retry fetch failed for page {p}, re-queued (attempt {attempts + 2}/{max_attempts}).", file=sys.stderr)
                    else:
                        if is_indefinite:
                            failed_indefinite_segments.add((category, subcategory))
                        if args.verbose:
                            print(f"    Retry fetch failed for page {p}, max attempts reached.", file=sys.stderr)
                    continue
                if len(found) == 0:
                    can_retry = (attempts + 1) < max_attempts
                    if can_retry:
                        retry_queue.append((category, subcategory, p, attempts + 1))
                        if args.verbose:
                            print(f"    Retry still 0 RUIDs for page {p}, re-queued (attempt {attempts + 2}/{max_attempts}).", file=sys.stderr)
                    else:
                        if is_indefinite:
                            failed_indefinite_segments.add((category, subcategory))
                        if args.verbose:
                            print(f"    Retry still 0 RUIDs for page {p}, max attempts reached.", file=sys.stderr)
                    continue
                for ruid in found:
                    key = (ruid, category, subcategory)
                    with lock:
                        if subcategory == -1:
                            if (ruid, category) in seen_per_ruid_cat:
                                continue
                            seen_per_ruid_cat.add((ruid, category))
                        else:
                            if key in seen:
                                continue
                            seen_per_ruid_cat.add((ruid, category))
                        seen.add(key)
                        writer.writerow([ruid, cat_name, sub_name, "", "", ""])
                        f.flush()
                        os.fsync(f.fileno())
                        rows_written_list[0] += 1
                print(f"    Got {len(found)} RUIDs from page {p}.", file=sys.stderr)

        else:
            if args.verbose:
                print("No failed pages; skipping global retry pass.", file=sys.stderr)

        if failed_indefinite_segments:
            out_dir = os.path.dirname(out_path) or _PROJECT_ROOT
            failed_csv_path = os.path.join(out_dir, "failed_indefinite_retry.csv")
            with open(failed_csv_path, "w", newline="", encoding="utf-8") as failed_f:
                w = csv.writer(failed_f)
                w.writerow(("category_id", "subcategory_id", "category_name", "subcategory_name"))
                for (cat, sub) in sorted(failed_indefinite_segments):
                    cname, sname = get_category_and_subcategory_names(cat, sub)
                    w.writerow((cat, sub, cname, sname))
            print(f"Wrote {len(failed_indefinite_segments)} segment(s) with failed indefinite retry to {failed_csv_path}.", file=sys.stderr)

        f.close()
        rows_written = rows_written_list[0]
        # Merge: existing + new rows, dedup by (ruid, category, subcategory). Subcategories are preserved.
        if os.path.isfile(out_path):
            fd_final, temp_final = tempfile.mkstemp(suffix=".csv", prefix="collect_ruids_final_", dir=os.path.dirname(out_path) or ".")
            try:
                name_to_seg = _name_to_segment()
                seen_merge: set[tuple[str, int, int]] = set()
                with open(fd_final, "w", newline="", encoding="utf-8") as out_final:
                    writer_final = csv.writer(out_final)
                    with open(out_path, "r", encoding="utf-8", newline="") as existing:
                        reader = csv.reader(existing)
                        header = next(reader, None)
                        if header:
                            writer_final.writerow(header)
                        for row in reader:
                            if len(row) < 3:
                                continue
                            ruid, cat_name, sub_name = row[0].strip(), row[1].strip(), row[2].strip()
                            key_n = (cat_name, sub_name)
                            if key_n not in name_to_seg:
                                continue
                            cat_id, sub_id = name_to_seg[key_n]
                            k = (ruid.lower(), cat_id, sub_id)
                            if k in seen_merge:
                                continue
                            seen_merge.add(k)
                            writer_final.writerow(row)
                    with open(temp_path, "r", encoding="utf-8", newline="") as new_f:
                        reader = csv.reader(new_f)
                        next(reader, None)  # skip header
                        for row in reader:
                            if len(row) < 3:
                                continue
                            ruid, cat_name, sub_name = row[0].strip(), row[1].strip(), row[2].strip()
                            key_n = (cat_name, sub_name)
                            if key_n not in name_to_seg:
                                continue
                            cat_id, sub_id = name_to_seg[key_n]
                            k = (ruid.lower(), cat_id, sub_id)
                            if k in seen_merge:
                                continue
                            seen_merge.add(k)
                            writer_final.writerow(row)
                final_count = len(seen_merge)
                os.replace(temp_final, out_path)
            except Exception:
                try:
                    os.unlink(temp_final)
                except OSError:
                    pass
                raise
            finally:
                pass
            if args.verbose:
                print(f"Merged {rows_written} new rows into existing output.", file=sys.stderr)
            print(f"Wrote {rows_written} new row(s) to temp; {out_path} has {final_count} rows (one per RUID+category+subcategory).", file=sys.stderr)
        else:
            os.replace(temp_path, out_path)
            print(f"Wrote {rows_written} new row(s) to {out_path}.", file=sys.stderr)
        return 0

    # Non-CSV mode: collect RUIDs only, print to stdout (listing via website only)
    print("Starting fetch (stdout mode, no --out)...", file=sys.stderr)
    sys.stderr.flush()
    all_ruids: dict[str, str] = {}
    _progress_interval = max(1, getattr(args, "workers", 1))
    for category, subcategory in segments_list:
        pages_done = 0
        page = 1
        while True:
            if pages_per_category is not None and pages_done >= pages_per_category:
                break
            url = page_url(page, category, subcategory)
            display_name = category_display_name(category, subcategory)
            if (page - 1) % _progress_interval == 0:
                print(f"Fetching {display_name} page {page} ...", file=sys.stderr)
            sys.stderr.flush()
            html = fetch_page(url, page_headers)
            if html is None:
                break
            found = _extract_ruids_from_listing_html(html, category)
            if len(found) == 0:
                break
            pages_done += 1
            for ruid, ext in found.items():
                if ruid not in all_ruids or (all_ruids[ruid] == "" and ext):
                    all_ruids[ruid] = ext
            if args.verbose:
                print(f"  Found {len(found)} RUIDs (total unique: {len(all_ruids)})", file=sys.stderr)
            if len(found) < MAX_ITEMS_PER_PAGE:
                break  # last page for this segment
            page += 1
            time.sleep(args.delay)
    lines = sorted(f"{ruid}\t{ext}" if ext else ruid for ruid, ext in all_ruids.items())
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
