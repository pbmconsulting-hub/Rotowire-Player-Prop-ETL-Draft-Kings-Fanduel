"""Extraction layer: pulls player prop data from the RotoWire API or via Selenium."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, UTC
from typing import Optional

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config import Config


# ---------------------------------------------------------------------------
# Prop-type tabs that Selenium should iterate through
# ---------------------------------------------------------------------------

PROP_TYPE_TABS: list[str] = [
    "Points", "Rebounds", "Assists", "Threes", "Blocks",
    "Steals", "Turnovers", "Pts+Reb+Ast", "Pts+Reb",
    "Pts+Ast", "Reb+Ast", "Stl+Blk",
]


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

_FIELD_KEYS: dict[str, list[str]] = {
    "player_name": ["name", "playerName", "player_name", "player"],
    "player_id": ["playerId", "player_id", "id"],
    "team": ["team", "teamAbbr", "team_abbr"],
    "position": ["position", "pos"],
    "opponent": ["opponent", "opp", "oppTeam", "opp_team"],
    "prop_type": ["propType", "prop_type", "market", "stat", "statType"],
    "line": ["line", "propLine", "value", "total"],
    "over_odds": ["overOdds", "over_odds", "overPrice", "over"],
    "under_odds": ["underOdds", "under_odds", "underPrice", "under"],
    "sportsbook": ["source", "sportsbook", "book", "site"],
    "game_date": ["gameDate", "game_date", "date"],
    "game_time": ["gameTime", "game_time", "time"],
}


def _normalise_api_record(record: dict, site_override: str | None = None) -> dict:
    """Normalise a raw API record to our canonical field names.

    Tries multiple key name variants for each canonical field.
    """
    out: dict = {}
    for canonical, variants in _FIELD_KEYS.items():
        for key in variants:
            if key in record:
                out[canonical] = record[key]
                break
        else:
            out[canonical] = None

    if site_override and not out.get("sportsbook"):
        out["sportsbook"] = None  # will be set below
    if site_override:
        out["sportsbook"] = site_override

    return out


def _extract_records_from_response(data, site: str) -> list[dict]:
    """Extract a list of raw records from various API response shapes.

    Handles plain list responses and nested dict responses with keys like
    'props', 'data', 'players', or 'results'.
    """
    if isinstance(data, list):
        raw_list = data
    elif isinstance(data, dict):
        for key in ("props", "data", "players", "results"):
            if key in data and isinstance(data[key], list):
                raw_list = data[key]
                break
        else:
            # Fallback: flatten any list values
            raw_list = []
            for v in data.values():
                if isinstance(v, list):
                    raw_list = v
                    break
            if not raw_list:
                raw_list = [data]
    else:
        logger.warning("Unexpected API response type: {}", type(data))
        return []

    records = []
    for item in raw_list:
        if isinstance(item, dict):
            records.append(_normalise_api_record(item, site_override=site))
    return records


# ---------------------------------------------------------------------------
# API extraction
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    reraise=True,
)
def _fetch_api(site: str, date: Optional[str] = None) -> list[dict]:
    """Fetch props from the RotoWire API for a single sportsbook site code."""
    params: dict = {"sport": "nba", "site": site}
    if date:
        params["date"] = date

    headers = {
        "User-Agent": Config.USER_AGENT,
        "Referer": Config.ROTOWIRE_PAGE_URL,
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    logger.debug("Fetching API: site={} date={}", site, date)
    resp = requests.get(
        Config.ROTOWIRE_API_URL,
        params=params,
        headers=headers,
        timeout=Config.REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    records = _extract_records_from_response(data, site)
    logger.info("API site={}: {} records fetched", site, len(records))
    return records


def _extract_api(date: Optional[str] = None) -> list[dict]:
    """Extract from API for DK and FD, combine results."""
    all_records: list[dict] = []
    for site in ("dk", "fd"):
        try:
            records = _fetch_api(site=site, date=date)
            all_records.extend(records)
        except Exception as exc:
            logger.warning("API fetch failed for site={}: {}", site, exc)
    return all_records


# ---------------------------------------------------------------------------
# Selenium extraction (fallback)
# ---------------------------------------------------------------------------

def _extract_selenium(date: Optional[str] = None) -> list[dict]:
    """Extract props using a headless Chrome browser as a fallback strategy.

    Iterates through all prop-type tabs and scrapes each one.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError as exc:
        raise RuntimeError(f"Selenium dependencies not installed: {exc}") from exc

    options = Options()
    if Config.HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={Config.USER_AGENT}")
    options.set_capability(
        "goog:loggingPrefs", {"performance": "ALL"}
    )

    url = Config.ROTOWIRE_PAGE_URL
    if date:
        url += f"?date={date}"

    game_date = date or datetime.now(UTC).strftime("%Y-%m-%d")

    logger.info("Starting Selenium extraction: {}", url)
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    try:
        driver.get(url)
        # Wait for at least one prop-like element to appear
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
        except Exception:
            pass
        time.sleep(Config.PAGE_LOAD_WAIT)

        # --- Strategy A: intercept XHR via performance logs ---
        records = _intercept_xhr(driver)
        if records:
            # Ensure game_date is populated on XHR records
            for rec in records:
                if not rec.get("game_date"):
                    rec["game_date"] = game_date
            logger.info("Selenium XHR intercept: {} records", len(records))
            return records

        # --- Strategy B: iterate prop-type tabs and parse HTML ---
        all_records: list[dict] = []
        for tab_name in PROP_TYPE_TABS:
            try:
                _click_tab(driver, tab_name)
                time.sleep(2)  # allow table to refresh
            except Exception:
                logger.debug("Could not click tab '{}'; skipping", tab_name)
                continue

            page_html = driver.page_source
            tab_records = _parse_html(page_html, game_date=game_date)
            all_records.extend(tab_records)
            logger.debug("Tab '{}': {} records", tab_name, len(tab_records))

        # If no tabs were clickable, try parsing the default page once
        if not all_records:
            all_records = _parse_html(driver.page_source, game_date=game_date)

        logger.info("Selenium HTML parse: {} records", len(all_records))
        return all_records
    finally:
        driver.quit()


def _click_tab(driver, tab_name: str) -> None:
    """Click a prop-type tab element in the Selenium browser."""
    from selenium.webdriver.common.by import By

    # Try common selectors for tab/nav elements
    for selector in (
        f"a[data-prop='{tab_name}']",
        f"button[data-prop='{tab_name}']",
        f"li[data-prop='{tab_name}'] a",
        f"a[data-type='{tab_name}']",
        f"button[data-type='{tab_name}']",
    ):
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if elements:
            elements[0].click()
            return

    # Fallback: try matching by visible text
    for tag in ("a", "button", "li", "span"):
        elements = driver.find_elements(By.TAG_NAME, tag)
        for el in elements:
            if el.text.strip() == tab_name:
                el.click()
                return

    raise RuntimeError(f"Tab '{tab_name}' not found")


def _intercept_xhr(driver) -> list[dict]:
    """Try to extract JSON data from Chrome performance logs."""
    try:
        import json as _json
        logs = driver.get_log("performance")
        for entry in logs:
            message = _json.loads(entry["message"])["message"]
            if message.get("method") == "Network.responseReceived":
                url = message.get("params", {}).get("response", {}).get("url", "")
                if "prop" in url.lower() and "api" in url.lower():
                    request_id = message["params"]["requestId"]
                    try:
                        body = driver.execute_cdp_cmd(
                            "Network.getResponseBody", {"requestId": request_id}
                        )
                        data = _json.loads(body.get("body", "[]"))
                        site = "dk" if "dk" in url else ("fd" if "fd" in url else "")
                        records = _extract_records_from_response(data, site)
                        if records:
                            return records
                    except Exception as exc:
                        logger.debug("XHR body extraction failed: {}", exc)
    except Exception as exc:
        logger.debug("Performance log extraction failed: {}", exc)
    return []


# ---------------------------------------------------------------------------
# Header-based sportsbook mapping
# ---------------------------------------------------------------------------

# Known header-text → canonical sportsbook-name mapping
_HEADER_SPORTSBOOK_MAP: dict[str, str] = {
    "draftkings": "DraftKings",
    "dk": "DraftKings",
    "fanduel": "FanDuel",
    "fd": "FanDuel",
    "betmgm": "BetMGM",
    "betrivers": "BetRivers",
    "caesars": "Caesars",
    "hard rock": "Hard Rock",
    "hardrock": "Hard Rock",
}


def _resolve_header_sportsbook(header_text: str) -> str | None:
    """Map a column-header prefix (e.g. 'DK', 'FanDuel') to a canonical name."""
    key = header_text.strip().lower()
    # Strip trailing _line / _over / _under etc.
    key = re.sub(r"[_\s]?(line|over|under|odds)$", "", key).strip()
    return _HEADER_SPORTSBOOK_MAP.get(key)


def _detect_sportsbook_columns(headers: list[str]) -> list[tuple[str, int, int, int]]:
    """Detect sportsbook column groups from table headers.

    Each sportsbook occupies 3 consecutive columns (Line, Over, Under).
    Returns a list of (sportsbook_name, line_idx, over_idx, under_idx).
    """
    result: list[tuple[str, int, int, int]] = []

    # Approach 1: look for header patterns like "DK_Line", "FD_Over"
    sportsbook_at: dict[str, dict[str, int]] = {}
    for idx, hdr in enumerate(headers):
        h = hdr.strip()
        # Try splitting on underscore or space: "DK_Line" → ("DK", "Line")
        parts = re.split(r"[_\s]+", h, maxsplit=1)
        if len(parts) == 2:
            book = _resolve_header_sportsbook(parts[0])
            col_type = parts[1].strip().lower()
            if book and col_type in ("line", "over", "under"):
                sportsbook_at.setdefault(book, {})[col_type] = idx

    for book, cols in sportsbook_at.items():
        if "line" in cols and "over" in cols and "under" in cols:
            result.append((book, cols["line"], cols["over"], cols["under"]))

    if result:
        return result

    # Approach 2: look for repeated triplets after the player/team/opp columns
    # Headers: [Player, Team, Opp, Line, Over, Under, Line, Over, Under, ...]
    # We need to figure out which sportsbook each triplet belongs to.
    # Try to find known sportsbook names in the headers themselves.
    book_order: list[str] = []
    for hdr in headers:
        book = _resolve_header_sportsbook(hdr)
        if book and book not in book_order:
            book_order.append(book)

    # If we found sportsbook names in headers, map triplets
    if book_order:
        # Find where triplets start (after non-triplet columns like Player, Team, Opp)
        triplet_start = None
        for idx, hdr in enumerate(headers):
            h = hdr.strip().lower()
            if h in ("line", "over", "under"):
                triplet_start = idx
                break
        if triplet_start is not None:
            for i, book in enumerate(book_order):
                base = triplet_start + i * 3
                if base + 2 < len(headers):
                    result.append((book, base, base + 1, base + 2))

    return result


def _detect_prop_type(soup) -> str | None:
    """Detect the current prop type from the page heading or active tab."""
    # Try active tab element
    for selector in (
        "a.is-active", "button.is-active",
        "li.active a", "a.active", "button.active",
        "[class*='tab'][class*='active']",
        "[class*='tab'][class*='selected']",
        "a[aria-selected='true']",
        "button[aria-selected='true']",
    ):
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            if text and len(text) < 64:
                return text

    # Try h2 heading
    h2 = soup.select_one("h2")
    if h2:
        text = h2.get_text(strip=True)
        if text and len(text) < 64:
            return text

    # Try h1 heading
    h1 = soup.select_one("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text and len(text) < 64:
            return text

    return None


def _parse_html(html: str, game_date: str | None = None) -> list[dict]:
    """Parse RotoWire page HTML to extract prop records.

    Handles the real table structure where each row is:
    [Player, Team, Opp, DK_Line, DK_Over, DK_Under, FD_Line, FD_Over, FD_Under, ...]

    Each row produces one record per sportsbook.  The prop type is read from the
    page heading / active tab, not from a table column.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    records: list[dict] = []

    # --- Detect prop type from heading / active tab ---
    prop_type = _detect_prop_type(soup)

    # --- Parse table headers to find sportsbook column groups ---
    table = soup.select_one("table")
    if table is None:
        return _parse_html_card_layout(soup, prop_type, game_date)

    header_row = table.select_one("thead tr")
    headers: list[str] = []
    if header_row:
        headers = [th.get_text(strip=True) for th in header_row.find_all("th")]

    sportsbook_groups = _detect_sportsbook_columns(headers)

    # --- Determine column indices for player / team / opponent ---
    # If no sportsbook groups found, fall back to the simple legacy format
    if not sportsbook_groups:
        return _parse_html_simple(
            table, headers, prop_type, game_date,
        )

    # Find player/team/opp column indices by header name
    player_idx = _find_header_index(headers, ("player", "name"))
    team_idx = _find_header_index(headers, ("team",))
    opp_idx = _find_header_index(headers, ("opp", "opponent"))

    rows = table.select("tbody tr")
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cols:
            continue

        player_name = cols[player_idx] if player_idx is not None and player_idx < len(cols) else cols[0] if cols else None
        team = cols[team_idx] if team_idx is not None and team_idx < len(cols) else None
        opponent = cols[opp_idx] if opp_idx is not None and opp_idx < len(cols) else None

        for book_name, line_idx, over_idx, under_idx in sportsbook_groups:
            if line_idx >= len(cols):
                continue
            line_val = cols[line_idx] if line_idx < len(cols) else None
            over_val = cols[over_idx] if over_idx < len(cols) else None
            under_val = cols[under_idx] if under_idx < len(cols) else None

            # Skip empty sportsbook columns (player may not have odds at this book)
            if not line_val and not over_val and not under_val:
                continue

            records.append({
                "player_name": player_name,
                "team": team,
                "opponent": opponent,
                "prop_type": prop_type,
                "line": line_val,
                "over_odds": over_val,
                "under_odds": under_val,
                "sportsbook": book_name,
                "game_date": game_date,
            })

    return records


def _find_header_index(headers: list[str], needles: tuple[str, ...]) -> int | None:
    """Return the index of the first header whose lower-cased text matches a needle."""
    for idx, h in enumerate(headers):
        if h.strip().lower() in needles:
            return idx
    return None


def _parse_html_simple(
    table, headers: list[str], prop_type: str | None, game_date: str | None,
) -> list[dict]:
    """Legacy parser for simple table layouts.

    Handles the format: [Player, Prop, Line, Over, Under, Book] or
    [Player, Team, Opp, Line, Over, Under].
    """
    records: list[dict] = []
    # Detect column roles by header names
    h_lower = [h.strip().lower() for h in headers]

    player_idx = _find_header_index(headers, ("player", "name"))
    team_idx = _find_header_index(headers, ("team",))
    opp_idx = _find_header_index(headers, ("opp", "opponent"))
    prop_idx = _find_header_index(headers, ("prop", "prop_type", "prop type", "market"))
    line_idx = _find_header_index(headers, ("line", "value", "total"))
    over_idx = _find_header_index(headers, ("over",))
    under_idx = _find_header_index(headers, ("under",))
    book_idx = _find_header_index(headers, ("book", "sportsbook", "source", "site"))

    rows = table.select("tbody tr")
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 4:
            continue

        row_prop = None
        if prop_idx is not None and prop_idx < len(cols):
            row_prop = cols[prop_idx]
        effective_prop = row_prop or prop_type

        row_book = None
        if book_idx is not None and book_idx < len(cols):
            row_book = cols[book_idx]

        records.append({
            "player_name": cols[player_idx] if player_idx is not None and player_idx < len(cols) else cols[0],
            "team": cols[team_idx] if team_idx is not None and team_idx < len(cols) else None,
            "opponent": cols[opp_idx] if opp_idx is not None and opp_idx < len(cols) else None,
            "prop_type": effective_prop,
            "line": cols[line_idx] if line_idx is not None and line_idx < len(cols) else None,
            "over_odds": cols[over_idx] if over_idx is not None and over_idx < len(cols) else None,
            "under_odds": cols[under_idx] if under_idx is not None and under_idx < len(cols) else None,
            "sportsbook": row_book,
            "game_date": game_date,
        })

    return records


def _parse_html_card_layout(
    soup, prop_type: str | None, game_date: str | None,
) -> list[dict]:
    """Parse div/card-based layouts as a last resort."""
    records: list[dict] = []
    for selector in (
        "div[class*='prop'] div[class*='row']",
        "div[class*='player-prop']",
        "li[class*='prop']",
    ):
        cards = soup.select(selector)
        for card in cards:
            name_el = card.select_one("[class*='name']") or card.select_one("[class*='player']")
            prop_el = card.select_one("[class*='prop-type']") or card.select_one("[class*='market']")
            line_el = card.select_one("[class*='line']") or card.select_one("[class*='value']")
            over_el = card.select_one("[class*='over']")
            under_el = card.select_one("[class*='under']")
            if name_el:
                records.append({
                    "player_name": name_el.get_text(strip=True),
                    "prop_type": prop_el.get_text(strip=True) if prop_el else prop_type,
                    "line": line_el.get_text(strip=True) if line_el else None,
                    "over_odds": over_el.get_text(strip=True) if over_el else None,
                    "under_odds": under_el.get_text(strip=True) if under_el else None,
                    "game_date": game_date,
                })
        if records:
            return records

    return records


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract(date: Optional[str] = None) -> tuple[list[dict], str]:
    """Extract player prop records from RotoWire.

    Tries the direct API first; falls back to Selenium if the API fails.

    Returns:
        (records, method) where method is 'api' or 'selenium'.

    Raises:
        RuntimeError: if both extraction strategies fail.
    """
    try:
        records = _extract_api(date=date)
        if records:
            return records, "api"
        logger.warning("API returned 0 records; falling back to Selenium")
    except Exception as exc:
        logger.warning("API extraction failed ({}); falling back to Selenium", exc)

    try:
        records = _extract_selenium(date=date)
        return records, "selenium"
    except Exception as exc:
        raise RuntimeError(
            f"Both API and Selenium extraction failed. Last error: {exc}"
        ) from exc
