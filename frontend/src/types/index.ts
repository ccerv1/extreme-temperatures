export interface Station {
  station_id: string;
  name: string;
  lat: number;
  lon: number;
  elevation_m: number | null;
  distance_km: number | null;
  coverage_years: number | null;
  completeness_temp_pct: number | null;
  quality_score: number | null;
  is_active: boolean;
  first_obs_date: string | null;
  last_obs_date: string | null;
}

export interface NormalBand {
  p25: number;
  p75: number;
}

export interface DataQuality {
  coverage_years: number;
  first_year: number;
  coverage_ratio: number;
  n_samples: number | null;
  since_year: number | null;
}

export interface RecordInfo {
  record_type: string;
  record_value: number;
  record_start: string;
  record_end: string;
  is_new_record: boolean;
}

export interface Insight {
  station_id: string;
  end_date: string;
  window_days: number;
  metric: string;
  primary_statement: string;
  supporting_line: string;
  value: number | null;
  severity: "normal" | "a_bit" | "unusual" | "extreme" | "insufficient_data";
  percentile: number | null;
  normal_band: NormalBand | null;
  data_quality: DataQuality;
  record_info: RecordInfo | null;
  since_year: number | null;
}

export interface SeriesPoint {
  end_date: string;
  value: number | null;
  percentile: number | null;
  p10: number | null;
  p25: number | null;
  p50: number | null;
  p75: number | null;
  p90: number | null;
}

export interface Series {
  station_id: string;
  window_days: number;
  metric: string;
  series: SeriesPoint[];
  since_year: number | null;
}

export interface StationRecord {
  station_id: string;
  metric: string;
  window_days: number;
  record_type: string;
  value: number;
  start_date: string;
  end_date: string;
  n_years: number;
}

export interface SeasonalRankingEntry {
  rank: number;
  year: number;
  value_c: number;
  value_f: number;
  delta_f: number;
  is_current: boolean;
}

export interface SeasonalRanking {
  rankings: SeasonalRankingEntry[];
  current_rank: number;
  total_years: number;
  direction: "cold" | "warm";
}

export interface ExtremesRankingEntry {
  rank: number;
  year: number;
  value_c: number;
  value_f: number;
  delta_f: number;
  start_date: string;
  end_date: string;
  is_current: boolean;
}

export interface ExtremesRanking {
  rankings: ExtremesRankingEntry[];
  current_rank: number;
  total_years: number;
  direction: "cold" | "warm";
}

export interface LatestInsight {
  station_id: string;
  end_date: string;
  window_days: number;
  metric: string;
  value: number | null;
  normal_value: number | null;
  percentile: number | null;
  severity: string;
  direction: string;
  primary_statement: string;
  supporting_line: string;
  coverage_years: number | null;
  first_year: number | null;
  since_year: number | null;
}
