"""Tests for src/transform.py."""

import pytest
import pandas as pd
from src.transform import transform, _normalise_prop_type, _resolve_sportsbook


VALID_RECORD = {
    "player_name": "LeBron James",
    "player_id": 1001,
    "team": "LAL",
    "position": "SF",
    "opponent": "GSW",
    "prop_type": "Points",
    "line": 25.5,
    "over_odds": "-115",
    "under_odds": "-105",
    "sportsbook": "dk",
    "game_date": "2024-01-15",
    "game_time": "7:30 PM ET",
}


class TestTransformValid:
    def test_valid_row_passes(self):
        df, validated, rejected = transform([VALID_RECORD])
        assert validated == 1
        assert rejected == 0
        assert len(df) == 1
        assert df.iloc[0]["player_name"] == "LeBron James"

    def test_sportsbook_resolved_to_canonical(self):
        df, validated, _ = transform([VALID_RECORD])
        assert df.iloc[0]["sportsbook"] == "DraftKings"

    def test_implied_probs_computed(self):
        df, _, _ = transform([VALID_RECORD])
        assert 0 < df.iloc[0]["over_implied_prob"] < 1
        assert 0 < df.iloc[0]["under_implied_prob"] < 1


class TestTransformRejections:
    def test_missing_player_name(self):
        record = {**VALID_RECORD, "player_name": None}
        df, validated, rejected = transform([record])
        assert validated == 0
        assert rejected >= 1

    def test_missing_line(self):
        record = {**VALID_RECORD, "line": None}
        df, validated, rejected = transform([record])
        assert validated == 0
        assert rejected >= 1

    def test_missing_prop_type(self):
        record = {**VALID_RECORD, "prop_type": None}
        df, validated, rejected = transform([record])
        assert validated == 0
        assert rejected >= 1

    def test_caesars_rejected(self):
        record = {**VALID_RECORD, "sportsbook": "Caesars"}
        df, validated, rejected = transform([record])
        assert validated == 0
        assert rejected >= 1

    def test_betmgm_rejected(self):
        record = {**VALID_RECORD, "sportsbook": "BetMGM"}
        df, validated, rejected = transform([record])
        assert validated == 0
        assert rejected >= 1


class TestSportsbookAliases:
    def test_dk_resolves_to_draftkings(self):
        assert _resolve_sportsbook("dk") == "DraftKings"

    def test_fd_resolves_to_fanduel(self):
        assert _resolve_sportsbook("fd") == "FanDuel"

    def test_draftkings_passthrough(self):
        assert _resolve_sportsbook("draftkings") == "DraftKings"

    def test_fanduel_passthrough(self):
        assert _resolve_sportsbook("fanduel") == "FanDuel"


class TestPropTypeNormalisation:
    def test_pts_to_points(self):
        assert _normalise_prop_type("pts") == "Points"

    def test_reb_to_rebounds(self):
        assert _normalise_prop_type("reb") == "Rebounds"

    def test_ast_to_assists(self):
        assert _normalise_prop_type("ast") == "Assists"

    def test_3pm_to_three_pointers(self):
        assert _normalise_prop_type("3pm") == "Three Pointers Made"

    def test_pra_to_combo(self):
        assert _normalise_prop_type("pra") == "Pts+Reb+Ast"

    def test_unknown_titlecased(self):
        result = _normalise_prop_type("fantasy points")
        assert result == "Fantasy Points"


class TestDeduplication:
    def test_exact_dupes_removed(self):
        records = [VALID_RECORD, VALID_RECORD]
        df, validated, _ = transform(records)
        assert len(df) == 1


class TestImpliedProbRange:
    def test_probs_in_range(self):
        df, _, _ = transform([VALID_RECORD])
        row = df.iloc[0]
        assert 0 < row["over_implied_prob"] < 1
        assert 0 < row["under_implied_prob"] < 1
