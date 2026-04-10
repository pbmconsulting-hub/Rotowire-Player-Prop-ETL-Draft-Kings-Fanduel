"""Pydantic models for validating raw and cleaned prop records."""

from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator
import re


class RawPropRecord(BaseModel):
    """Loose-typed model that accepts raw data directly from extraction."""

    model_config = {"extra": "allow"}

    player_name: Optional[str] = None
    player_id: Optional[str | int] = None
    team: Optional[str] = None
    position: Optional[str] = None
    opponent: Optional[str] = None
    prop_type: Optional[str] = None
    line: Optional[str | float | int] = None
    over_odds: Optional[str | int] = None
    under_odds: Optional[str | int] = None
    over_implied_prob: Optional[float] = None
    under_implied_prob: Optional[float] = None
    sportsbook: Optional[str] = None
    game_date: Optional[str] = None
    game_time: Optional[str] = None


class CleanPropRecord(BaseModel):
    """Validated, normalised prop record ready for database insertion."""

    player_name: str
    player_id: Optional[int] = None
    team: Optional[str] = None
    position: Optional[str] = None
    opponent: Optional[str] = None
    prop_type: str
    line: float
    over_odds: Optional[str] = None
    under_odds: Optional[str] = None
    over_implied_prob: Optional[float] = None
    under_implied_prob: Optional[float] = None
    sportsbook: Optional[str] = None
    game_date: str
    game_time: Optional[str] = None

    @field_validator("player_name")
    @classmethod
    def normalise_player_name(cls, v: str) -> str:
        v = re.sub(r"\s+", " ", v).strip()
        if len(v) < 2 or len(v) > 128:
            raise ValueError("player_name length must be 2–128 characters")
        return v

    @field_validator("team", "position", "opponent", mode="before")
    @classmethod
    def upper_short_field(cls, v):
        if v is None:
            return v
        v = str(v).strip().upper()
        if len(v) > 10:
            v = v[:10]
        return v

    @field_validator("prop_type")
    @classmethod
    def check_prop_type(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 2 or len(v) > 64:
            raise ValueError("prop_type length must be 2–64 characters")
        return v

    @field_validator("over_odds", "under_odds", mode="before")
    @classmethod
    def coerce_odds_string(cls, v):
        if v is None:
            return v
        return str(v)[:16]

    @field_validator("sportsbook", mode="before")
    @classmethod
    def truncate_sportsbook(cls, v):
        if v is None:
            return v
        return str(v)[:64]
