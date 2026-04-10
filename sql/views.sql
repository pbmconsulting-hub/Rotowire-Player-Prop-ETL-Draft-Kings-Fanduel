-- NBA Player Props Analytics Views (PostgreSQL)

-- 1. Latest snapshot per player / prop / book
CREATE OR REPLACE VIEW v_current_props AS
SELECT pp.*
FROM player_props pp
INNER JOIN (
    SELECT player_name, prop_type, sportsbook, game_date, MAX(scraped_at) AS max_scraped
    FROM player_props
    WHERE sportsbook IN ('DraftKings', 'FanDuel')
    GROUP BY player_name, prop_type, sportsbook, game_date
) latest
  ON pp.player_name = latest.player_name
 AND pp.prop_type   = latest.prop_type
 AND pp.sportsbook  = latest.sportsbook
 AND pp.game_date   = latest.game_date
 AND pp.scraped_at  = latest.max_scraped
WHERE pp.sportsbook IN ('DraftKings', 'FanDuel');

-- 2. Side-by-side DK vs FD
CREATE OR REPLACE VIEW v_dk_vs_fd AS
SELECT
    dk.player_name,
    dk.prop_type,
    dk.game_date,
    dk.line          AS dk_line,
    dk.over_odds     AS dk_over,
    dk.under_odds    AS dk_under,
    fd.line          AS fd_line,
    fd.over_odds     AS fd_over,
    fd.under_odds    AS fd_under,
    ROUND(CAST(dk.line - fd.line AS numeric), 2)                          AS line_diff,
    ROUND(CAST(dk.over_implied_prob - fd.over_implied_prob AS numeric), 4) AS prob_diff
FROM v_current_props dk
JOIN v_current_props fd
  ON dk.player_name = fd.player_name
 AND dk.prop_type   = fd.prop_type
 AND dk.game_date   = fd.game_date
WHERE dk.sportsbook = 'DraftKings'
  AND fd.sportsbook = 'FanDuel';

-- 3. Edges: |line_diff| >= 1.0
CREATE OR REPLACE VIEW v_edges AS
SELECT *
FROM v_dk_vs_fd
WHERE ABS(line_diff) >= 1.0;

-- 4. Best over (lower implied prob = higher payout)
CREATE OR REPLACE VIEW v_best_over AS
SELECT
    player_name,
    prop_type,
    game_date,
    sportsbook,
    over_odds,
    over_implied_prob
FROM (
    SELECT *,
           RANK() OVER (
               PARTITION BY player_name, prop_type, game_date
               ORDER BY over_implied_prob ASC
           ) AS rnk
    FROM v_current_props
    WHERE over_implied_prob IS NOT NULL
) ranked
WHERE rnk = 1;

-- 5. Chronological line / odds history
CREATE OR REPLACE VIEW v_line_history AS
SELECT
    player_name,
    prop_type,
    game_date,
    sportsbook,
    line,
    over_odds,
    under_odds,
    over_implied_prob,
    under_implied_prob,
    scraped_at
FROM player_props
WHERE sportsbook IN ('DraftKings', 'FanDuel')
ORDER BY player_name, prop_type, sportsbook, scraped_at;

-- 6. Steam moves
CREATE OR REPLACE VIEW v_steam_moves AS
SELECT *
FROM line_movements
WHERE ABS(line_diff) >= 1.0
  AND sportsbook IN ('DraftKings', 'FanDuel')
ORDER BY ABS(line_diff) DESC;

-- 7. Biggest movers
CREATE OR REPLACE VIEW v_biggest_movers AS
SELECT *
FROM line_movements
ORDER BY ABS(line_diff) DESC;

-- 8. ETL health
CREATE OR REPLACE VIEW v_etl_health AS
SELECT
    id,
    started_at,
    finished_at,
    duration_seconds,
    status,
    extraction_method,
    rows_extracted,
    rows_validated,
    rows_rejected,
    rows_loaded,
    error_message
FROM scrape_runs
ORDER BY started_at DESC
LIMIT 50;
