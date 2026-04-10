"""Extraction layer: pulls player prop data from the RotoWire API or via Selenium."""

from __future__ import annotations

import json
import time
from typing import Optional

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config import Config


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
    """Extract props using a headless Chrome browser as a fallback strategy."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        from bs4 import BeautifulSoup
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
            logger.info("Selenium XHR intercept: {} records", len(records))
            return records

        # --- Strategy B: BeautifulSoup HTML parsing ---
        records = _parse_html(driver.page_source)
        logger.info("Selenium HTML parse: {} records", len(records))
        return records
    finally:
        driver.quit()


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


def _parse_html(html: str) -> list[dict]:
    """Parse RotoWire page HTML to extract prop records."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    records: list[dict] = []

    # Strategy 1: table rows
    rows = soup.select("table tbody tr")
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) >= 4:
            records.append({
                "player_name": cols[0],
                "prop_type": cols[1] if len(cols) > 1 else None,
                "line": cols[2] if len(cols) > 2 else None,
                "over_odds": cols[3] if len(cols) > 3 else None,
                "under_odds": cols[4] if len(cols) > 4 else None,
            })

    if records:
        return records

    # Strategy 2: div-based card layout
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
                    "prop_type": prop_el.get_text(strip=True) if prop_el else None,
                    "line": line_el.get_text(strip=True) if line_el else None,
                    "over_odds": over_el.get_text(strip=True) if over_el else None,
                    "under_odds": under_el.get_text(strip=True) if under_el else None,
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
