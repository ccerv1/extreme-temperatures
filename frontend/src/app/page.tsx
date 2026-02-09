"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import stations from "@/data/stations.json";
import { fetchLatestInsights } from "@/lib/api";
import type { LatestInsight } from "@/types";

const SEVERITY_LABELS: Record<string, Record<string, { label: string; className: string }>> = {
  cold: {
    extreme:      { label: "Extremely Cold", className: "text-blue-700 font-semibold" },
    very_unusual: { label: "Very Cold",      className: "text-blue-600 font-semibold" },
    unusual:      { label: "Unusually Cold",  className: "text-blue-500 font-medium" },
  },
  warm: {
    extreme:      { label: "Extremely Warm", className: "text-red-700 font-semibold" },
    very_unusual: { label: "Very Warm",      className: "text-orange-600 font-semibold" },
    unusual:      { label: "Unusually Warm",  className: "text-amber-600 font-medium" },
  },
};

function getSeverityDisplay(insight: LatestInsight): { label: string; className: string } {
  if (insight.severity === "normal" || insight.severity === "insufficient_data") {
    return { label: "Normal", className: "text-neutral-400" };
  }
  return SEVERITY_LABELS[insight.direction]?.[insight.severity] ?? { label: "Normal", className: "text-neutral-400" };
}

export default function Home() {
  const [insights, setInsights] = useState<Record<string, LatestInsight>>({});

  useEffect(() => {
    fetchLatestInsights()
      .then((items) => {
        const map: Record<string, LatestInsight> = {};
        for (const item of items) {
          map[item.station_id] = item;
        }
        setInsights(map);
      })
      .catch(() => {
        // API not available — show empty state
      });
  }, []);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-semibold tracking-tight">
          Extreme Temperatures
        </h1>
        <p className="mt-1 text-sm text-neutral-500">
          How unusual is this weather compared to history?
        </p>
      </div>

      <div>
        <h2 className="text-sm font-medium text-neutral-400 uppercase tracking-wider mb-3">
          Stations
        </h2>
        <div className="space-y-1">
          {stations.map((s) => {
            const insight = insights[s.station_id];
            const display = insight ? getSeverityDisplay(insight) : null;

            return (
              <Link
                key={s.station_id}
                href={`/station/${s.station_id}`}
                className="flex items-center justify-between rounded-lg px-3 py-2.5 hover:bg-neutral-50 transition-colors group"
              >
                <span className="font-medium group-hover:text-blue-600 transition-colors">
                  {s.name}
                </span>
                {display ? (
                  <span className={`text-sm ${display.className}`}>
                    {display.label}
                  </span>
                ) : (
                  <span className="text-xs text-neutral-300">···</span>
                )}
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
}
