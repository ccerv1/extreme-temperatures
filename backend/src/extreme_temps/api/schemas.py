"""Pydantic response models for the API."""

from __future__ import annotations

from datetime import date
from pydantic import BaseModel


class StationResponse(BaseModel):
    station_id: str
    name: str
    lat: float
    lon: float
    elevation_m: float | None = None
    distance_km: float | None = None
    coverage_years: int | None = None
    completeness_temp_pct: float | None = None
    quality_score: float | None = None
    is_active: bool = True
    first_obs_date: date | None = None
    last_obs_date: date | None = None


class NormalBand(BaseModel):
    p25: float
    p75: float


class DataQuality(BaseModel):
    coverage_years: int
    first_year: int
    coverage_ratio: float
    n_samples: int | None = None
    since_year: int | None = None


class RecordInfo(BaseModel):
    record_type: str
    record_value: float
    record_start: date
    record_end: date
    is_new_record: bool


class InsightResponse(BaseModel):
    station_id: str
    end_date: date
    window_days: int
    metric: str
    primary_statement: str
    supporting_line: str
    value: float | None = None
    severity: str
    percentile: float | None = None
    normal_band: NormalBand | None = None
    data_quality: DataQuality
    record_info: RecordInfo | None = None
    since_year: int | None = None


class SeriesPoint(BaseModel):
    end_date: date
    value: float | None = None
    percentile: float | None = None
    p10: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    p90: float | None = None


class SeriesResponse(BaseModel):
    station_id: str
    window_days: int
    metric: str
    series: list[SeriesPoint]
    since_year: int | None = None


class RecordResponse(BaseModel):
    station_id: str
    metric: str
    window_days: int
    record_type: str
    value: float
    start_date: date
    end_date: date
    n_years: int


class SeasonalRankingEntry(BaseModel):
    rank: int
    year: int
    value_c: float
    value_f: float
    delta_f: float
    is_current: bool = False


class SeasonalRankingResponse(BaseModel):
    rankings: list[SeasonalRankingEntry]
    current_rank: int
    total_years: int
    direction: str


class ExtremesRankingEntry(BaseModel):
    rank: int
    year: int
    value_c: float
    value_f: float
    delta_f: float
    start_date: date
    end_date: date
    is_current: bool = False


class ExtremesRankingResponse(BaseModel):
    rankings: list[ExtremesRankingEntry]
    current_rank: int
    total_years: int
    direction: str


class LatestInsightItem(BaseModel):
    station_id: str
    end_date: date
    window_days: int
    metric: str
    value: float | None = None
    percentile: float | None = None
    severity: str
    direction: str
    primary_statement: str
    supporting_line: str
    coverage_years: int | None = None
    first_year: int | None = None
    since_year: int | None = None
