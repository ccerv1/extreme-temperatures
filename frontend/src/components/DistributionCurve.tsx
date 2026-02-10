"use client";

import { useMemo } from "react";

interface StationDot {
  percentile: number;
  severity: string;
  direction: string;
}

const DOT_COLORS: Record<string, string> = {
  "cold-extreme": "#1e40af",
  "cold-unusual": "#2563eb",
  "cold-a_bit": "#93c5fd",
  "normal": "#a3a3a3",
  "warm-a_bit": "#fbbf24",
  "warm-unusual": "#f97316",
  "warm-extreme": "#dc2626",
};

// Zone definitions for the color strip + labels below the chart
const ZONES = [
  { from: 0, to: 5, label: "Extremely Cold", bg: "bg-blue-800", text: "text-blue-800" },
  { from: 5, to: 15, label: "Unusually Cold", bg: "bg-blue-500", text: "text-blue-600" },
  { from: 15, to: 35, label: "A Bit Colder", bg: "bg-sky-200", text: "text-sky-500" },
  { from: 35, to: 65, label: "Normal", bg: "bg-neutral-200", text: "text-neutral-400" },
  { from: 65, to: 85, label: "A Bit Warmer", bg: "bg-amber-200", text: "text-amber-600" },
  { from: 85, to: 95, label: "Unusually Warm", bg: "bg-orange-400", text: "text-orange-600" },
  { from: 95, to: 100, label: "Extremely Warm", bg: "bg-red-500", text: "text-red-600" },
];

// Layout
const W = 640;
const H = 150;
const PAD_X = 20;
const BASELINE_Y = H - 2;
const DOT_R = 3.5;
const COL_STEP = 2;
const STACK_STEP = DOT_R * 2 + 0.5;
const SIDE_PAD = `${(PAD_X / W) * 100}%`;

function pctToX(pct: number): number {
  return PAD_X + (pct / 100) * (W - 2 * PAD_X);
}

function getDotColor(percentile: number): string {
  if (percentile < 5) return DOT_COLORS["cold-extreme"];
  if (percentile > 95) return DOT_COLORS["warm-extreme"];
  if (percentile <= 15) return DOT_COLORS["cold-unusual"];
  if (percentile >= 85) return DOT_COLORS["warm-unusual"];
  if (percentile <= 35) return DOT_COLORS["cold-a_bit"];
  if (percentile >= 65) return DOT_COLORS["warm-a_bit"];
  return DOT_COLORS["normal"];
}

export default function DistributionCurve({ points }: { points: StationDot[] }) {
  const dots = useMemo(() => {
    const columns: Record<number, StationDot[]> = {};
    for (const pt of points) {
      const col = Math.max(0, Math.min(100, Math.round(pt.percentile / COL_STEP) * COL_STEP));
      if (!columns[col]) columns[col] = [];
      columns[col].push(pt);
    }

    const result: { x: number; y: number; color: string }[] = [];
    for (const [col, pts] of Object.entries(columns)) {
      const x = pctToX(Number(col));
      for (let i = 0; i < pts.length; i++) {
        result.push({
          x,
          y: BASELINE_Y - DOT_R - i * STACK_STEP,
          color: getDotColor(pts[i].percentile),
        });
      }
    }
    return result;
  }, [points]);

  if (points.length === 0) return null;

  return (
    <div>
      {/* Chart */}
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full overflow-visible block"
        preserveAspectRatio="xMidYMid meet"
      >
        {/* Station dots */}
        {dots.map((d, i) => (
          <circle
            key={i}
            cx={d.x}
            cy={d.y}
            r={DOT_R}
            fill={d.color}
            stroke="white"
            strokeWidth={1.2}
          />
        ))}
      </svg>

      {/* Zone color strip — aligned with the SVG percentile axis */}
      <div
        className="flex gap-px rounded-full overflow-hidden"
        style={{ marginLeft: SIDE_PAD, marginRight: SIDE_PAD }}
      >
        {ZONES.map((z) => (
          <div
            key={z.from}
            className={`h-1.5 ${z.bg}`}
            style={{ flex: z.to - z.from }}
          />
        ))}
      </div>

      {/* Zone labels — each label sits directly under its zone segment */}
      <div
        className="flex mt-1"
        style={{ marginLeft: SIDE_PAD, marginRight: SIDE_PAD }}
      >
        {ZONES.map((z) => {
          const isUnusual = z.label.startsWith("Unusually");
          return (
            <div
              key={z.from}
              className={`text-center text-[9px] leading-tight font-medium ${z.text} ${isUnusual ? "hidden sm:block" : ""}`}
              style={{ flex: z.to - z.from }}
            >
              {z.label}
            </div>
          );
        })}
      </div>
    </div>
  );
}
