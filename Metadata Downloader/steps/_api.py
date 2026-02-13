#!/usr/bin/env python3
"""
Shared API helpers for MapleStory Worlds resource API (auth, headers, request).
Used by steps/1-collect.py and steps/2-enrich.py.
"""

import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    import browser_cookie3
    _HAS_BROWSER_COOKIE = True
except ImportError:
    _HAS_BROWSER_COOKIE = False

BASE_URL = "https://mverse-api.nexon.com"
DEFAULT_IFWT = ""


def get_default_ifwt() -> str:
    """Return token from env or empty (launcher supplies token)."""
    return os.environ.get("MSW_IFWT", "") or DEFAULT_IFWT


def resource_detail_url(ruid: str) -> str:
    return f"{BASE_URL}/resource/v1/search/{ruid}"


_BROWSER_LOADERS = ("chrome", "edge", "firefox", "chromium", "opera", "safari")
_BROWSER_CHOICE = {"F": "firefox", "C": "chrome", "E": "edge", "S": "safari"}


def _get_ifwt_from_cookiejar(cj, verbose: bool, browser_name: str) -> str | None:
    for cookie in cj:
        if cookie.name == "_ifwt":
            return cookie.value
    if verbose:
        print(f"{browser_name}: no _ifwt cookie for maplestoryworlds.nexon.com.", file=sys.stderr)
    return None


def get_ifwt_from_browser(browser_name: str, verbose: bool = False) -> str | None:
    if not _HAS_BROWSER_COOKIE:
        if verbose:
            print("browser_cookie3 not installed (pip install browser-cookie3).", file=sys.stderr)
        return None
    loader = getattr(browser_cookie3, browser_name, None)
    if loader is None:
        return None
    try:
        cj = loader(domain_name="maplestoryworlds.nexon.com")
        return _get_ifwt_from_cookiejar(cj, verbose, browser_name)
    except Exception as e:
        if verbose:
            print(f"{browser_name}: {e}", file=sys.stderr)
        return None


IFWT_INSTRUCTIONS = """
How to get your _ifwt token:
  1. Log in at https://maplestoryworlds.nexon.com in any browser.
  2. Open Developer Tools (F12) → Application/Storage → Cookies → site.
  3. Find the cookie named _ifwt and copy its value.
  Or: Network tab → request to mverse-api.nexon.com → Request Headers → x-mverse-ifwt.
"""


def get_ifwt_interactive(verbose: bool = False) -> str | None:
    """Prompt user: browser (F/C/E/S) or Other to paste _ifwt; return token or None."""
    print("Select where to get your session token (_ifwt):", file=sys.stderr)
    print("  F = Firefox    C = Chrome    E = Edge    S = Safari    O = Other (paste token)", file=sys.stderr)
    while True:
        try:
            choice = input("Choice [F/C/E/S/O]: ").strip().upper() or " "
            key = choice[0] if choice else " "
        except (EOFError, KeyboardInterrupt):
            return None
        if key in _BROWSER_CHOICE:
            ifwt = get_ifwt_from_browser(_BROWSER_CHOICE[key], verbose=True)
            if ifwt:
                return ifwt
            continue
        if key == "O":
            print(IFWT_INSTRUCTIONS, file=sys.stderr)
            try:
                token = input("Paste your _ifwt token: ").strip()
            except (EOFError, KeyboardInterrupt):
                return None
            if token:
                return token
            continue
        print("Invalid choice. Enter F, C, E, S, or O.", file=sys.stderr)


def load_cookies_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        return ""
    first = lines[0]
    if first.lower().startswith("cookie:"):
        return first[7:].strip()
    cookies = []
    for line in lines:
        if "\t" in line:
            parts = line.split("\t")
            if len(parts) >= 7:
                cookies.append(f"{parts[5]}={parts[6]}")
        elif "=" in line and not line.startswith("#"):
            cookies.append(line.split("#", 1)[0].strip())
    return "; ".join(cookies)


def build_headers(ifwt: str | None, cookie_header: str | None = None) -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://maplestoryworlds.nexon.com",
        "Referer": "https://maplestoryworlds.nexon.com/",
        "x-mverse-language": "en",
        "x-mverse-countrycode": "US",
    }
    if ifwt:
        headers["x-mverse-ifwt"] = ifwt
    if cookie_header:
        headers["Cookie"] = cookie_header
    return headers


def try_url(url: str, headers: dict) -> tuple[bool, dict | str, int]:
    try:
        req = Request(url, headers=headers, method="GET")
        with urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            try:
                data = json.loads(body)
                return True, data, resp.status
            except json.JSONDecodeError:
                return True, body[:500], resp.status
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            try:
                data = json.loads(body)
                return False, data, e.code
            except json.JSONDecodeError:
                return False, body[:500], e.code
        except Exception:
            return False, str(e), e.code
    except URLError as e:
        return False, str(e.reason), 0
    except Exception as e:
        return False, str(e), 0
