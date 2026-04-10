"""Tests for src/utils/odds.py."""

import pytest
from src.utils.odds import (
    parse_american_odds,
    american_to_decimal,
    american_to_implied_prob,
    remove_vig,
    format_american,
)


class TestParseAmericanOdds:
    def test_negative(self):
        assert parse_american_odds("-110") == -110

    def test_positive(self):
        assert parse_american_odds("+150") == 150

    def test_unicode_minus(self):
        assert parse_american_odds("\u2212110") == -110

    def test_en_dash(self):
        assert parse_american_odds("\u2013110") == -110

    def test_even(self):
        assert parse_american_odds("even") == 100

    def test_pk(self):
        assert parse_american_odds("pk") == 100

    def test_none(self):
        assert parse_american_odds(None) is None

    def test_empty(self):
        assert parse_american_odds("") is None

    def test_bare_number(self):
        assert parse_american_odds("150") == 150

    def test_bare_negative(self):
        assert parse_american_odds("-120") == -120


class TestAmericanToDecimal:
    def test_favourite(self):
        result = american_to_decimal(-110)
        assert abs(result - 1.9091) < 0.001

    def test_underdog(self):
        result = american_to_decimal(150)
        assert abs(result - 2.5) < 0.001

    def test_even_money(self):
        result = american_to_decimal(100)
        assert abs(result - 2.0) < 0.001


class TestAmericanToImpliedProb:
    def test_favourite(self):
        result = american_to_implied_prob(-110)
        assert 0.52 < result < 0.53

    def test_underdog(self):
        result = american_to_implied_prob(150)
        assert 0.39 < result < 0.41

    def test_even_money(self):
        result = american_to_implied_prob(100)
        assert abs(result - 0.5) < 0.001

    def test_heavy_favourite(self):
        result = american_to_implied_prob(-300)
        assert result > 0.7


class TestRemoveVig:
    def test_probs_sum_to_one(self):
        over, under = remove_vig(0.5238, 0.5238)
        assert abs(over + under - 1.0) < 1e-5

    def test_symmetric(self):
        over, under = remove_vig(0.5, 0.5)
        assert abs(over - 0.5) < 1e-5
        assert abs(under - 0.5) < 1e-5

    def test_asymmetric(self):
        over, under = remove_vig(0.55, 0.48)
        assert abs(over + under - 1.0) < 1e-5
        assert over > under


class TestFormatAmerican:
    def test_positive(self):
        assert format_american(150) == "+150"

    def test_negative(self):
        assert format_american(-110) == "-110"

    def test_zero(self):
        assert format_american(0) == "+0"
