"""Configuration management for the NBA Props ETL pipeline."""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Central configuration loaded from environment variables."""

    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///nba_props.db")
    ROTOWIRE_PAGE_URL: str = os.getenv(
        "ROTOWIRE_PAGE_URL",
        "https://www.rotowire.com/betting/nba/player-props.php",
    )
    ROTOWIRE_API_URL: str = os.getenv(
        "ROTOWIRE_API_URL",
        "https://www.rotowire.com/betting/api/player-props.php",
    )

    HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"
    PAGE_LOAD_WAIT: int = int(os.getenv("PAGE_LOAD_WAIT", "8"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    USER_AGENT: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )

    SCRAPE_INTERVAL_MINUTES: int = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "15"))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/etl.log")
    LOG_ROTATION: str = os.getenv("LOG_ROTATION", "10 MB")

    ALLOWED_SPORTSBOOKS: set = {
        "DraftKings", "FanDuel", "BetMGM", "BetRivers", "Caesars", "Hard Rock",
    }

    SPORTSBOOK_ALIASES: dict = {
        "dk": "DraftKings",
        "draftkings": "DraftKings",
        "draft kings": "DraftKings",
        "fd": "FanDuel",
        "fanduel": "FanDuel",
        "fan duel": "FanDuel",
        "betmgm": "BetMGM",
        "bet mgm": "BetMGM",
        "mgm": "BetMGM",
        "betrivers": "BetRivers",
        "bet rivers": "BetRivers",
        "rivers": "BetRivers",
        "caesars": "Caesars",
        "czr": "Caesars",
        "william hill": "Caesars",
        "hard rock": "Hard Rock",
        "hardrock": "Hard Rock",
        "hard rock bet": "Hard Rock",
    }
