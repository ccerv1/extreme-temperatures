"use client";

import { useEffect, useState, useMemo } from "react";
import Link from "next/link";
import stations from "@/data/stations.json";
import { fetchLatestInsights } from "@/lib/api";
import type { LatestInsight } from "@/types";
import DistributionCurve from "@/components/DistributionCurve";

function celsiusToFahrenheit(c: number): number {
  return c * 9 / 5 + 32;
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr + "T12:00:00");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

const SEVERITY_LABELS: Record<string, Record<string, { label: string; className: string }>> = {
  cold: {
    extreme: { label: "Extremely Cold", className: "text-blue-700 font-semibold" },
    unusual: { label: "Unusually Cold", className: "text-blue-600 font-semibold" },
    a_bit:   { label: "A Bit Colder",   className: "text-blue-400 font-medium" },
  },
  warm: {
    extreme: { label: "Extremely Warm", className: "text-red-700 font-semibold" },
    unusual: { label: "Unusually Warm", className: "text-orange-600 font-semibold" },
    a_bit:   { label: "A Bit Warmer",   className: "text-amber-400 font-medium" },
  },
};

const WINDOW_OPTIONS = [
  { days: 7, label: "7d" },
  { days: 14, label: "14d" },
  { days: 30, label: "30d" },
];

// Lower = more extreme = sort first
const SEVERITY_SORT_ORDER: Record<string, number> = {
  extreme: 0,
  unusual: 1,
  a_bit: 2,
  normal: 3,
  insufficient_data: 4,
};

function getSeverityDisplay(insight: LatestInsight): { label: string; className: string } {
  if (insight.severity === "normal" || insight.severity === "insufficient_data") {
    return { label: "Near Normal", className: "text-neutral-400" };
  }
  return SEVERITY_LABELS[insight.direction]?.[insight.severity] ?? { label: "Near Normal", className: "text-neutral-400" };
}

export default function Home() {
  // Nested map: station_id → window_days → LatestInsight
  const [insightMap, setInsightMap] = useState<Record<string, Record<number, LatestInsight>>>({});
  const [selectedWindow, setSelectedWindow] = useState(7);

  useEffect(() => {
    fetchLatestInsights()
      .then((items) => {
        const map: Record<string, Record<number, LatestInsight>> = {};
        for (const item of items) {
          if (!map[item.station_id]) {
            map[item.station_id] = {};
          }
          map[item.station_id][item.window_days] = item;
        }
        setInsightMap(map);
      })
      .catch(() => {
        // API not available — show empty state
      });
  }, []);

  // Derive the date label from window=1 data
  const dateLabel = useMemo(() => {
    const firstStation = Object.values(insightMap)[0];
    const todayInsight = firstStation?.[1];
    if (todayInsight?.end_date) {
      return formatDate(todayInsight.end_date);
    }
    return "Temp";
  }, [insightMap]);

  // Sort stations: most extreme severity first, then by percentile distance from 50
  const sortedStations = useMemo(() => {
    return [...stations].sort((a, b) => {
      const insA = insightMap[a.station_id]?.[selectedWindow];
      const insB = insightMap[b.station_id]?.[selectedWindow];

      // Stations without data go to bottom
      if (!insA && !insB) return 0;
      if (!insA) return 1;
      if (!insB) return -1;

      const sevA = SEVERITY_SORT_ORDER[insA.severity] ?? 5;
      const sevB = SEVERITY_SORT_ORDER[insB.severity] ?? 5;
      if (sevA !== sevB) return sevA - sevB;

      // Within same severity, sort by percentile distance from 50 (more extreme first)
      const distA = insA.percentile != null ? Math.abs(insA.percentile - 50) : 0;
      const distB = insB.percentile != null ? Math.abs(insB.percentile - 50) : 0;
      return distB - distA;
    });
  }, [insightMap, selectedWindow]);

  // Points for the distribution curve
  const distributionPoints = useMemo(() => {
    return stations
      .map((s) => insightMap[s.station_id]?.[selectedWindow])
      .filter((ins): ins is LatestInsight => ins != null && ins.percentile != null)
      .map((ins) => ({
        percentile: ins.percentile!,
        severity: ins.severity,
        direction: ins.direction,
      }));
  }, [insightMap, selectedWindow]);

  const outlierCount = distributionPoints.filter(
    (p) => p.severity !== "normal" && p.severity !== "insufficient_data"
  ).length;

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-semibold tracking-tight">
          Tempercentiles
        </h1>
        <p className="mt-1 text-sm text-neutral-500">
          Daily temperatures in historical context for 50 US cities
        </p>
      </div>

      {/* Distribution curve */}
      {distributionPoints.length > 0 && (
        <div className="relative">
          <div className="absolute top-0 left-0 z-10">
            <span className="text-2xl font-semibold tabular-nums tracking-tight">
              {outlierCount}
            </span>
            <span className="text-sm text-neutral-500 ml-1.5">
              of {distributionPoints.length} stations outside normal range
            </span>
          </div>
          <DistributionCurve points={distributionPoints} />
        </div>
      )}

      <div>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-medium text-neutral-400 uppercase tracking-wider">
            Stations
          </h2>
          <div className="flex items-center gap-1">
            {WINDOW_OPTIONS.map((opt) => (
              <button
                key={opt.days}
                onClick={() => setSelectedWindow(opt.days)}
                className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                  selectedWindow === opt.days
                    ? "bg-neutral-900 text-white"
                    : "bg-neutral-100 text-neutral-500 hover:bg-neutral-200"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>

        {/* Column headers */}
        <div className="flex items-center px-3 py-1.5 text-xs text-neutral-400">
          <span className="flex-1">Name</span>
          <span className="w-20 text-right">{dateLabel}</span>
          <span className="w-36 text-right">Status</span>
        </div>

        <div className="space-y-0.5">
          {sortedStations.map((s) => {
            const stationInsights = insightMap[s.station_id];
            const todayInsight = stationInsights?.[1];
            const windowInsight = stationInsights?.[selectedWindow];
            const display = windowInsight ? getSeverityDisplay(windowInsight) : null;
            const tempF = todayInsight?.value != null
              ? celsiusToFahrenheit(todayInsight.value)
              : null;

            return (
              <Link
                key={s.station_id}
                href={`/station/${s.station_id}`}
                className="flex items-center rounded-lg px-3 py-2.5 hover:bg-neutral-50 transition-colors group"
              >
                <span className="flex-1 font-medium group-hover:text-blue-600 transition-colors truncate">
                  {s.name}
                </span>
                <span className="w-20 text-right text-sm tabular-nums text-neutral-600">
                  {tempF != null ? `${tempF.toFixed(0)}°F` : "···"}
                </span>
                <span className={`w-36 text-right text-sm ${display?.className ?? "text-neutral-300"}`}>
                  {display?.label ?? "···"}
                </span>
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
}
