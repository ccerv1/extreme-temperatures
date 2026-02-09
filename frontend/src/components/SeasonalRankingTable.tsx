"use client";

import { Fragment, useState } from "react";
import type { SeasonalRanking } from "@/types";

function formatDelta(delta: number): string {
  if (delta === 0) return "\u2014";
  const sign = delta > 0 ? "+" : "";
  return `${sign}${delta.toFixed(1)}\u00b0F`;
}

function getVisibleIndices(rankings: { is_current: boolean }[], expanded: boolean): Set<number> {
  if (expanded) return new Set(rankings.map((_, i) => i));

  const indices = new Set<number>();
  const currentIdx = rankings.findIndex((r) => r.is_current);

  // Always show first 10
  for (let i = 0; i < Math.min(10, rankings.length); i++) {
    indices.add(i);
  }

  // Show 2 rows around current position
  if (currentIdx >= 0) {
    for (let i = Math.max(0, currentIdx - 2); i <= Math.min(rankings.length - 1, currentIdx + 2); i++) {
      indices.add(i);
    }
  }

  return indices;
}

export default function SeasonalRankingTable({ data }: { data: SeasonalRanking }) {
  const [expanded, setExpanded] = useState(false);
  const { rankings, direction, total_years } = data;

  if (rankings.length === 0) return null;

  const visibleIndices = getVisibleIndices(rankings, expanded);
  const needsExpand = visibleIndices.size < rankings.length && !expanded;
  const isCold = direction === "cold";

  let lastShownIdx = -2;

  return (
    <div>
      <h3 className="text-xs font-medium text-neutral-400 uppercase tracking-wider mb-3">
        How this period compares &mdash; same time of year
      </h3>
      <div className="rounded-lg border border-neutral-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-neutral-100 bg-neutral-50">
              <th className="px-3 py-1.5 text-left text-xs font-medium text-neutral-400 w-14">Rank</th>
              <th className="px-3 py-1.5 text-left text-xs font-medium text-neutral-400 w-16">Year</th>
              <th className="px-3 py-1.5 text-right text-xs font-medium text-neutral-400">Avg Temp</th>
              <th className="px-3 py-1.5 text-right text-xs font-medium text-neutral-400">vs Current</th>
            </tr>
          </thead>
          <tbody>
            {rankings.map((entry, idx) => {
              if (!visibleIndices.has(idx)) {
                lastShownIdx = idx;
                return null;
              }

              const showGap = idx - lastShownIdx > 1 && lastShownIdx >= 0;
              lastShownIdx = idx;

              return (
                <Fragment key={entry.year}>
                  {showGap && (
                    <tr className="border-b border-neutral-50">
                      <td colSpan={4} className="px-3 py-1 text-center text-xs text-neutral-300">
                        &middot;&middot;&middot;
                      </td>
                    </tr>
                  )}
                  <tr
                    className={`border-b border-neutral-50 last:border-0 ${
                      entry.is_current ? "bg-blue-50 font-semibold" : ""
                    }`}
                  >
                    <td className="px-3 py-1.5 tabular-nums">{entry.rank}</td>
                    <td className="px-3 py-1.5 tabular-nums">{entry.year}</td>
                    <td className={`px-3 py-1.5 text-right tabular-nums ${
                      entry.is_current ? (isCold ? "text-blue-700" : "text-red-700") : ""
                    }`}>
                      {entry.value_f.toFixed(1)}&deg;F
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-neutral-500">
                      {formatDelta(entry.delta_f)}
                    </td>
                  </tr>
                </Fragment>
              );
            })}
          </tbody>
        </table>
        {needsExpand && (
          <button
            onClick={() => setExpanded(true)}
            className="w-full px-3 py-2 text-xs text-neutral-400 hover:text-neutral-600 hover:bg-neutral-50 transition-colors border-t border-neutral-100"
          >
            Show all {total_years} years
          </button>
        )}
      </div>
    </div>
  );
}
