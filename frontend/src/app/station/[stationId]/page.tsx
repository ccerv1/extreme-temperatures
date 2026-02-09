"use client";

import { useEffect, useState, useCallback } from "react";
import { useParams } from "next/navigation";
import type { Insight, Series, StationRecord, Station, SeasonalRanking, ExtremesRanking } from "@/types";
import { fetchInsight, fetchSeries, fetchRecords, fetchStation, fetchSeasonalRankings, fetchExtremesRankings } from "@/lib/api";
import InsightCard from "@/components/InsightCard";
import WindowSelector from "@/components/WindowSelector";
import IntervalSelector from "@/components/IntervalSelector";
import TemperatureChart from "@/components/TemperatureChart";
import RecordsTable from "@/components/RecordsTable";
import SeasonalRankingTable from "@/components/SeasonalRankingTable";
import ExtremesRankingTable from "@/components/ExtremesRankingTable";
import allStations from "@/data/stations.json";

function celsiusToFahrenheit(c: number): number {
  return c * 9 / 5 + 32;
}

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}

function daysAgo(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr + "T12:00:00");
  return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
}

function ordinal(n: number): string {
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

const DEFAULT_SINCE_YEAR = new Date().getFullYear() - 24;

const TODAY_SEVERITY_STYLES: Record<string, Record<string, { label: string; className: string }>> = {
  cold: {
    extreme: { label: "Extremely Cold", className: "text-blue-700" },
    unusual: { label: "Unusually Cold", className: "text-blue-600" },
    a_bit:   { label: "A Bit Colder",   className: "text-blue-400" },
  },
  warm: {
    extreme: { label: "Extremely Warm", className: "text-red-700" },
    unusual: { label: "Unusually Warm", className: "text-orange-600" },
    a_bit:   { label: "A Bit Warmer",   className: "text-amber-400" },
  },
};

function getTodaySeverityDisplay(insight: Insight): { label: string; className: string } {
  if (insight.severity === "normal" || insight.severity === "insufficient_data") {
    return { label: "Near Normal", className: "text-neutral-500" };
  }
  return TODAY_SEVERITY_STYLES[insight.percentile != null && insight.percentile <= 50 ? "cold" : "warm"]?.[insight.severity]
    ?? { label: "Near Normal", className: "text-neutral-500" };
}

export default function StationPage() {
  const params = useParams();
  const stationId = params.stationId as string;

  const [station, setStation] = useState<Station | null>(null);
  const [windowDays, setWindowDays] = useState(7);
  const [sinceYear, setSinceYear] = useState<number | null>(DEFAULT_SINCE_YEAR);
  const [todayInsight, setTodayInsight] = useState<Insight | null>(null);
  const [insight, setInsight] = useState<Insight | null>(null);
  const [series, setSeries] = useState<Series | null>(null);
  const [records, setRecords] = useState<StationRecord[]>([]);
  const [seasonalRanking, setSeasonalRanking] = useState<SeasonalRanking | null>(null);
  const [extremesRanking, setExtremesRanking] = useState<ExtremesRanking | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const startDate = daysAgo(365);

      // Fetch station metadata and records (date-independent)
      const [stationData, recordsData] = await Promise.all([
        fetchStation(stationId),
        fetchRecords(stationId),
      ]);
      setStation(stationData);
      setRecords(recordsData);

      // Fetch 1-day insight independently (start from yesterday — today is partial)
      let todayInsightData: Insight | null = null;
      for (let i = 1; i <= 7; i++) {
        try {
          todayInsightData = await fetchInsight(stationId, daysAgo(i), 1, "tavg_c", sinceYear ?? undefined);
          break;
        } catch {
          continue;
        }
      }
      setTodayInsight(todayInsightData);

      // Fetch rolling window insight (start from yesterday)
      let insightData: Insight | null = null;
      let workingEndDate = daysAgo(1);
      for (let i = 1; i <= 7; i++) {
        const endDate = daysAgo(i);
        try {
          insightData = await fetchInsight(stationId, endDate, windowDays, "tavg_c", sinceYear ?? undefined);
          workingEndDate = endDate;
          break;
        } catch {
          continue;
        }
      }

      if (!insightData) {
        setError("No recent data available for this station");
        return;
      }

      setInsight(insightData);

      // Fetch series and rankings using the working end date
      const [seriesData, seasonalData] = await Promise.all([
        fetchSeries(stationId, windowDays, startDate, workingEndDate, "tavg_c", sinceYear ?? undefined),
        fetchSeasonalRankings(stationId, workingEndDate, windowDays, "tavg_c", sinceYear ?? undefined).catch(() => null),
      ]);

      setSeries(seriesData);
      setSeasonalRanking(seasonalData);

      // Fetch extremes for unusual or more severe
      if (insightData.severity === "unusual" || insightData.severity === "extreme") {
        const direction = insightData.percentile != null && insightData.percentile <= 50 ? "cold" : "warm";
        const extremesData = await fetchExtremesRankings(
          stationId, workingEndDate, windowDays, "tavg_c", direction, sinceYear ?? undefined
        ).catch(() => null);
        setExtremesRanking(extremesData);
      } else {
        setExtremesRanking(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  }, [stationId, windowDays, sinceYear]);

  useEffect(() => {
    load();
  }, [load]);

  if (error) {
    return (
      <div className="space-y-4">
        <p className="text-sm text-red-600">{error}</p>
        <p className="text-xs text-neutral-400">
          Make sure the API server is running at {process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}
        </p>
      </div>
    );
  }

  const todayTempF = todayInsight?.value != null ? celsiusToFahrenheit(todayInsight.value) : null;
  const todayDisplay = todayInsight ? getTodaySeverityDisplay(todayInsight) : null;
  const todayDateLabel = todayInsight ? formatDate(todayInsight.end_date) : null;

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <a href="/" className="text-xs text-neutral-400 hover:text-neutral-600 transition-colors">
          &larr; All stations
        </a>
        {(() => {
          const meta = allStations.find((s) => s.station_id === stationId);
          return (
            <>
              <h1 className="mt-1 text-2xl font-semibold tracking-tight">
                {meta?.city ?? station?.name ?? stationId}
              </h1>
              <p className="text-xs text-neutral-400 mt-0.5">
                {meta?.location ?? station?.name}
                {station?.coverage_years != null && ` · ${station.coverage_years} years of data`}
              </p>
            </>
          );
        })()}
      </div>

      {/* Today card */}
      {todayInsight && (
        <div className="rounded-xl border border-neutral-100 bg-white p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-medium text-neutral-400 uppercase tracking-wider mb-1">
                {todayDateLabel}
              </p>
              {todayTempF != null && (
                <span className="text-4xl font-light tabular-nums tracking-tight">
                  {todayTempF.toFixed(0)}
                  <span className="text-lg text-neutral-400">°F</span>
                </span>
              )}
            </div>
            <div className="text-right">
              {todayDisplay && (
                <span className={`text-sm font-medium ${todayDisplay.className}`}>
                  {todayDisplay.label}
                </span>
              )}
              {todayInsight.percentile != null && todayInsight.data_quality && (
                <p className="text-xs text-neutral-400 mt-0.5">
                  {todayInsight.percentile <= 50
                    ? `${ordinal(Math.max(1, Math.round((1 - todayInsight.percentile / 100) * todayInsight.data_quality.coverage_years)))} coldest`
                    : `${ordinal(Math.max(1, Math.round((todayInsight.percentile / 100) * todayInsight.data_quality.coverage_years)))} warmest`
                  } out of {todayInsight.data_quality.coverage_years} years
                </p>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Window selector + Interval selector */}
      <div className="flex flex-wrap items-center gap-4">
        <WindowSelector value={windowDays} onChange={setWindowDays} />
        <IntervalSelector
          value={sinceYear}
          onChange={setSinceYear}
          firstObsYear={
            station?.first_obs_date
              ? new Date(station.first_obs_date).getFullYear()
              : null
          }
        />
      </div>

      {/* Loading state */}
      {loading && (
        <div className="py-12 text-center text-sm text-neutral-400">
          Loading...
        </div>
      )}

      {/* Insight */}
      {!loading && insight && (
        <div className="border-t border-neutral-100 pt-6">
          <InsightCard insight={insight} seasonalRanking={seasonalRanking} />
        </div>
      )}

      {/* Chart — immediate visual context for the insight */}
      {!loading && series && series.series.length > 0 && (
        <div className="border-t border-neutral-100 pt-6">
          <TemperatureChart
            series={series.series}
            stationName={station?.name ?? stationId}
            windowDays={windowDays}
          />
        </div>
      )}

      {/* Seasonal Ranking */}
      {!loading && seasonalRanking && (
        <div className="border-t border-neutral-100 pt-6">
          <SeasonalRankingTable data={seasonalRanking} />
        </div>
      )}

      {/* Extremes Ranking */}
      {!loading && extremesRanking && (
        <div className="border-t border-neutral-100 pt-6">
          <ExtremesRankingTable data={extremesRanking} />
        </div>
      )}

      {/* Records */}
      {!loading && records.length > 0 && (
        <div className="border-t border-neutral-100 pt-6">
          <h3 className="text-xs font-medium text-neutral-400 uppercase tracking-wider mb-3">
            All-time records
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <RecordsTable records={records} type="lowest" />
            <RecordsTable records={records} type="highest" />
          </div>
        </div>
      )}
    </div>
  );
}
