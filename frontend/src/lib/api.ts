import type { Station, Insight, Series, StationRecord, SeasonalRanking, ExtremesRanking, LatestInsight } from "@/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function fetchJSON<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(path, API_BASE);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchNearbyStations(lat: number, lon: number, radiusKm = 50): Promise<Station[]> {
  return fetchJSON<Station[]>("/stations/nearby", {
    lat: String(lat),
    lon: String(lon),
    radius_km: String(radiusKm),
  });
}

export async function fetchStation(stationId: string): Promise<Station> {
  return fetchJSON<Station>(`/stations/${stationId}`);
}

export async function fetchInsight(
  stationId: string,
  endDate: string,
  windowDays = 7,
  metric = "tavg_c",
  sinceYear?: number
): Promise<Insight> {
  const params: Record<string, string> = {
    station_id: stationId,
    end_date: endDate,
    window_days: String(windowDays),
    metric,
  };
  if (sinceYear != null) params.since_year = String(sinceYear);
  return fetchJSON<Insight>("/insights/window", params);
}

export async function fetchSeries(
  stationId: string,
  windowDays: number,
  startDate: string,
  endDate: string,
  metric = "tavg_c",
  sinceYear?: number
): Promise<Series> {
  const params: Record<string, string> = {
    station_id: stationId,
    window_days: String(windowDays),
    start_date: startDate,
    end_date: endDate,
    metric,
  };
  if (sinceYear != null) params.since_year = String(sinceYear);
  return fetchJSON<Series>("/series/window", params);
}

export async function fetchRecords(stationId: string, metric = "tavg_c"): Promise<StationRecord[]> {
  return fetchJSON<StationRecord[]>("/records/", {
    station_id: stationId,
    metric,
  });
}

export async function fetchSeasonalRankings(
  stationId: string,
  endDate: string,
  windowDays: number,
  metric = "tavg_c",
  sinceYear?: number
): Promise<SeasonalRanking> {
  const params: Record<string, string> = {
    station_id: stationId,
    end_date: endDate,
    window_days: String(windowDays),
    metric,
  };
  if (sinceYear != null) params.since_year = String(sinceYear);
  return fetchJSON<SeasonalRanking>("/rankings/seasonal", params);
}

export async function fetchExtremesRankings(
  stationId: string,
  endDate: string,
  windowDays: number,
  metric = "tavg_c",
  direction = "cold",
  sinceYear?: number
): Promise<ExtremesRanking> {
  const params: Record<string, string> = {
    station_id: stationId,
    end_date: endDate,
    window_days: String(windowDays),
    metric,
    direction,
  };
  if (sinceYear != null) params.since_year = String(sinceYear);
  return fetchJSON<ExtremesRanking>("/rankings/extremes", params);
}

export async function fetchLatestInsights(): Promise<LatestInsight[]> {
  return fetchJSON<LatestInsight[]>("/insights/latest");
}

export async function fetchLastUpdated(): Promise<string | null> {
  const data = await fetchJSON<{ last_updated: string | null }>("/manage/last-updated");
  return data.last_updated;
}
