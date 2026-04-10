"""Tests for src/extract.py."""

import pytest
from src.extract import _normalise_api_record, _extract_records_from_response


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
