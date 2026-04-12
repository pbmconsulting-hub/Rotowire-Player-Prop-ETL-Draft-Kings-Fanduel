"""Tests for src/extract.py."""

import pytest
from src.extract import (
    _normalise_api_record,
    _extract_records_from_response,
    _parse_html,
    _detect_sportsbook_columns,
    _detect_prop_type,
    _find_header_index,
)


class TestNormaliseApiRecord:
    def test_standard_keys(self):
        record = {
            "name": "LeBron James",
            "playerId": 1001,
            "team": "LAL",
            "pos": "SF",
            "opponent": "GSW",
            "propType": "Points",
            "line": 25.5,
            "overOdds": "-115",
            "underOdds": "-105",
            "source": "dk",
            "gameDate": "2024-01-15",
            "gameTime": "7:30 PM ET",
        }
        out = _normalise_api_record(record)
        assert out["player_name"] == "LeBron James"
        assert out["player_id"] == 1001
        assert out["prop_type"] == "Points"
        assert out["line"] == 25.5
        assert out["over_odds"] == "-115"

    def test_alternate_keys(self):
        record = {
            "playerName": "Stephen Curry",
            "player_id": 1002,
            "teamAbbr": "GSW",
            "position": "PG",
            "opp": "LAL",
            "market": "Points",
            "value": 29.5,
            "overPrice": "-120",
            "underPrice": "+100",
            "book": "fd",
            "date": "2024-01-15",
        }
        out = _normalise_api_record(record)
        assert out["player_name"] == "Stephen Curry"
        assert out["team"] == "GSW"
        assert out["prop_type"] == "Points"
        assert out["line"] == 29.5
        assert out["over_odds"] == "-120"

    def test_site_override(self):
        record = {"name": "Test Player", "propType": "Points", "line": 10.5}
        out = _normalise_api_record(record, site_override="dk")
        assert out["sportsbook"] == "dk"

    def test_missing_fields_are_none(self):
        out = _normalise_api_record({"name": "Player X"})
        assert out["line"] is None
        assert out["over_odds"] is None


class TestExtractRecordsFromResponse:
    def test_list_response(self):
        data = [
            {"name": "Player A", "propType": "Points", "line": 20.5, "source": "dk"},
            {"name": "Player B", "propType": "Rebounds", "line": 8.5, "source": "fd"},
        ]
        records = _extract_records_from_response(data, "dk")
        assert len(records) == 2
        assert records[0]["player_name"] == "Player A"

    def test_nested_props_key(self):
        data = {"props": [{"name": "Player C", "propType": "Assists", "line": 5.5}]}
        records = _extract_records_from_response(data, "dk")
        assert len(records) == 1
        assert records[0]["player_name"] == "Player C"

    def test_nested_data_key(self):
        data = {"data": [{"name": "Player D", "propType": "Points", "line": 18.0}]}
        records = _extract_records_from_response(data, "fd")
        assert len(records) == 1

    def test_nested_results_key(self):
        data = {"results": [{"name": "Player E", "propType": "Blocks", "line": 1.5}]}
        records = _extract_records_from_response(data, "dk")
        assert len(records) == 1

    def test_empty_list(self):
        records = _extract_records_from_response([], "dk")
        assert records == []


class TestParseHtmlMultiSportsbook:
    """Tests for the rewritten _parse_html with multi-sportsbook table format."""

    def test_multi_sportsbook_table(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        # 3 players × 2 sportsbooks (DK + FD) = 6 records
        assert len(records) == 6
        # Verify a complete DraftKings record
        lebron_dk = [r for r in records if r["player_name"] == "LeBron James" and r["sportsbook"] == "DraftKings"]
        assert len(lebron_dk) == 1
        assert lebron_dk[0]["line"] == "25.5"
        assert lebron_dk[0]["over_odds"] == "-115"
        assert lebron_dk[0]["under_odds"] == "-105"
        assert lebron_dk[0]["team"] == "LAL"
        assert lebron_dk[0]["opponent"] == "GSW"

    def test_sportsbook_names_extracted(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        sportsbooks = {r["sportsbook"] for r in records}
        assert "DraftKings" in sportsbooks
        assert "FanDuel" in sportsbooks

    def test_prop_type_from_heading(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        for r in records:
            assert r["prop_type"] == "Points"

    def test_team_and_opponent_populated(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        lebron_records = [r for r in records if r["player_name"] == "LeBron James"]
        assert len(lebron_records) == 2
        for r in lebron_records:
            assert r["team"] == "LAL"
            assert r["opponent"] == "GSW"

    def test_game_date_populated(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        for r in records:
            assert r["game_date"] == "2024-01-15"

    def test_dk_and_fd_lines_differ(self, sample_html):
        records = _parse_html(sample_html, game_date="2024-01-15")
        lebron_dk = [r for r in records if r["player_name"] == "LeBron James" and r["sportsbook"] == "DraftKings"]
        lebron_fd = [r for r in records if r["player_name"] == "LeBron James" and r["sportsbook"] == "FanDuel"]
        assert len(lebron_dk) == 1
        assert len(lebron_fd) == 1
        assert lebron_dk[0]["line"] == "25.5"
        assert lebron_fd[0]["line"] == "26.5"


class TestDetectSportsbookColumns:
    def test_underscore_format(self):
        headers = ["Player", "Team", "Opp", "DK_Line", "DK_Over", "DK_Under", "FD_Line", "FD_Over", "FD_Under"]
        groups = _detect_sportsbook_columns(headers)
        assert len(groups) == 2
        books = {g[0] for g in groups}
        assert "DraftKings" in books
        assert "FanDuel" in books

    def test_empty_headers(self):
        groups = _detect_sportsbook_columns([])
        assert groups == []


class TestDetectPropType:
    def test_h2_heading(self):
        from bs4 import BeautifulSoup
        html = "<html><body><h2>Rebounds</h2><table></table></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_prop_type(soup) == "Rebounds"

    def test_active_tab(self):
        from bs4 import BeautifulSoup
        html = '<html><body><a class="is-active">Assists</a><table></table></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_prop_type(soup) == "Assists"

    def test_no_heading_returns_none(self):
        from bs4 import BeautifulSoup
        html = "<html><body><table></table></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_prop_type(soup) is None


class TestFindHeaderIndex:
    def test_finds_player(self):
        assert _find_header_index(["Player", "Team", "Line"], ("player",)) == 0

    def test_finds_team(self):
        assert _find_header_index(["Player", "Team", "Line"], ("team",)) == 1

    def test_not_found(self):
        assert _find_header_index(["Player", "Team"], ("missing",)) is None
