# NBA Player Props ETL Pipeline

Production-grade ETL pipeline for extracting NBA player prop betting lines from RotoWire, filtered to **DraftKings** and **FanDuel** only.

## Architecture

```
RotoWire API ──► Extract ──► Transform (filter DK/FD) ──► Load ──► Analytics Views
                  │                                          │
                  └── Selenium fallback                      └── line_movements table
```

## Quick Start (SQLite — zero config)

```bash
pip install -r requirements.txt
python -m src.pipeline
```

## Docker Start (PostgreSQL)

```bash
docker-compose up --build
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///nba_props.db` | SQLAlchemy connection string |
| `ROTOWIRE_PAGE_URL` | RotoWire page URL | Browser URL |
| `ROTOWIRE_API_URL` | RotoWire API URL | JSON API endpoint |
| `HEADLESS` | `true` | Run Chrome headless |
| `PAGE_LOAD_WAIT` | `8` | Extra seconds to wait for JS |
| `REQUEST_TIMEOUT` | `30` | HTTP timeout seconds |
| `USER_AGENT` | Chrome 120 UA | Browser User-Agent |
| `SCRAPE_INTERVAL_MINUTES` | `15` | Scheduler interval |
| `LOG_LEVEL` | `INFO` | Loguru log level |
| `LOG_FILE` | `logs/etl.log` | Log file path |
| `LOG_ROTATION` | `10 MB` | Log rotation size |

## Database Schema

### `player_props`
Point-in-time snapshot of every prop line. Never overwritten — appended on each scrape.

### `scrape_runs`
Audit log for every pipeline execution.

### `line_movements`
Computed summary: opening line, current line, line diff, number of changes.

## SQL Analytics Views

| View | Description |
|------|-------------|
| `v_current_props` | Latest snapshot per player/prop/book |
| `v_dk_vs_fd` | Side-by-side DK vs FD with line_diff |
| `v_edges` | Rows where `|line_diff| >= 1.0` |
| `v_best_over` | Best over odds per prop |
| `v_line_history` | Full chronological history |
| `v_steam_moves` | Lines that moved >= 1.0 |
| `v_biggest_movers` | All movements ordered by `|line_diff|` |
| `v_etl_health` | Last 50 scrape run audit records |

## Query Helpers

```python
from src.queries import get_current_props, get_dk_vs_fd, get_edges, get_line_movement

# Latest props for today
df = get_current_props(game_date="2024-01-15")

# Side-by-side DK vs FD
df = get_dk_vs_fd("2024-01-15")

# Find arbitrage edges
df = get_edges("2024-01-15", min_line_diff=0.5)

# Track line movement for a player
df = get_line_movement("LeBron James", "Points", "2024-01-15")
```

## Running Tests

```bash
pytest tests/ -v
```

## Roadmap

- Slack/Discord alerting on steam moves
- FastAPI dashboard
- Historical backfill support
- Grafana dashboards
