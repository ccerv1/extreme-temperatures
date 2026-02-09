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
  { days: 7, label: "Past 7 days" },
  { days: 14, label: "Past 14 days" },
  { days: 30, label: "Past 30 days" },
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

      // Within same severity, group cold then warm
      const dirOrder = (d: string) => d === "cold" ? 0 : d === "warm" ? 1 : 2;
      const dirA = dirOrder(insA.direction);
      const dirB = dirOrder(insB.direction);
      if (dirA !== dirB) return dirA - dirB;

      // Within same severity+direction, sort by percentile distance from 50 (more extreme first)
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
    <div className="space-y-5">
      <div>
        <h1 className="text-3xl font-semibold tracking-tight">
          Tempercentiles
        </h1>
        <p className="mt-1 text-sm text-neutral-500">
          How <em className="not-italic font-medium">extreme</em> are current temperatures? Each city&#8217;s recent temperatures are compared against its historical record for the same period.
        </p>
      </div>

      {/* Summary + window selector */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        {distributionPoints.length > 0 && (
          <div>
            <span className="text-2xl font-semibold tabular-nums tracking-tight">
              {outlierCount}
            </span>
            <span className="text-sm text-neutral-500 ml-1.5">
              of {distributionPoints.length} stations outside normal range
            </span>
          </div>
        )}
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

      {/* Distribution curve */}
      {distributionPoints.length > 0 && (
        <div className="-mt-2">
          <DistributionCurve points={distributionPoints} />
        </div>
      )}

      <div className="-mt-2 rounded-lg border border-neutral-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-neutral-100 bg-neutral-50">
              <th className="px-3 py-1.5 text-left text-xs font-medium text-neutral-400">Name</th>
              <th className="px-3 py-1.5 text-right text-xs font-medium text-neutral-400 w-20">Temp</th>
              <th className="px-3 py-1.5 text-right text-xs font-medium text-neutral-400 w-36">Status</th>
            </tr>
          </thead>
          <tbody>
            {sortedStations.map((s) => {
              const stationInsights = insightMap[s.station_id];
              const todayInsight = stationInsights?.[1];
              const windowInsight = stationInsights?.[selectedWindow];
              const display = windowInsight ? getSeverityDisplay(windowInsight) : null;
              const tempF = todayInsight?.value != null
                ? celsiusToFahrenheit(todayInsight.value)
                : null;

              return (
                <tr key={s.station_id} className="border-b border-neutral-50 last:border-0 hover:bg-neutral-50 transition-colors group">
                  <td className="px-3 py-1.5">
                    <Link
                      href={`/station/${s.station_id}`}
                      className="font-medium text-neutral-900 underline decoration-neutral-300 decoration-dashed underline-offset-2 group-hover:text-blue-600 group-hover:decoration-blue-400 transition-colors"
                    >
                      {s.city}
                    </Link>
                  </td>
                  <td className="px-3 py-1.5 text-right tabular-nums text-neutral-600">
                    {tempF != null ? `${tempF.toFixed(0)}°F` : "···"}
                  </td>
                  <td className={`px-3 py-1.5 text-right ${display?.className ?? "text-neutral-300"}`}>
                    {display?.label ?? "···"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
