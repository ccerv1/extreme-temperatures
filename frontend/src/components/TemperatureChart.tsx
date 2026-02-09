"use client";

import { useState, useRef, useCallback } from "react";
import type { SeriesPoint } from "@/types";

function cToF(c: number | null): number | null {
  return c != null ? c * 9 / 5 + 32 : null;
}

function ordinal(n: number): string {
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

interface Props {
  series: SeriesPoint[];
  stationName: string;
  windowDays: number;
}

export default function TemperatureChart({ series, stationName, windowDays }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<{
    x: number;
    y: number;
    date: string;
    tempF: number;
    percentile: number | null;
  } | null>(null);

  if (series.length === 0) {
    return <div className="h-64 flex items-center justify-center text-neutral-400 text-sm">No data</div>;
  }

  const points = series.map((p) => ({
    date: p.end_date,
    value: cToF(p.value),
    percentile: p.percentile,
    p10: cToF(p.p10),
    p25: cToF(p.p25),
    p50: cToF(p.p50),
    p75: cToF(p.p75),
    p90: cToF(p.p90),
  }));

  const allValues = points.flatMap((p) =>
    [p.value, p.p10, p.p90].filter((v): v is number => v != null)
  );
  const minY = Math.floor(Math.min(...allValues) - 2);
  const maxY = Math.ceil(Math.max(...allValues) + 2);
  const rangeY = maxY - minY || 1;

  const w = 960;
  const h = 320;
  const pad = { top: 20, right: 20, bottom: 40, left: 45 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;

  const xScale = (i: number) => pad.left + (i / Math.max(points.length - 1, 1)) * plotW;
  const yScale = (v: number) => pad.top + plotH - ((v - minY) / rangeY) * plotH;

  const bandPath = (lowKey: "p10" | "p25", highKey: "p75" | "p90") => {
    const validPts = points.filter((p) => p[lowKey] != null && p[highKey] != null);
    if (validPts.length < 2) return "";
    const fwd = validPts.map((p, i) => {
      const idx = points.indexOf(p);
      return `${i === 0 ? "M" : "L"}${xScale(idx)},${yScale(p[highKey]!)}`;
    }).join(" ");
    const bwd = [...validPts].reverse().map((p) => {
      const idx = points.indexOf(p);
      return `L${xScale(idx)},${yScale(p[lowKey]!)}`;
    }).join(" ");
    return `${fwd} ${bwd} Z`;
  };

  const linePath = (key: "value" | "p50") => {
    const validPts = points.filter((p) => p[key] != null);
    return validPts.map((p, i) => {
      const idx = points.indexOf(p);
      return `${i === 0 ? "M" : "L"}${xScale(idx)},${yScale(p[key]!)}`;
    }).join(" ");
  };

  // X-axis labels — roughly every month for a year of data
  const step = Math.max(1, Math.floor(points.length / 12));
  const xLabels = points
    .filter((_, i) => i % step === 0)
    .map((p) => {
      const idx = points.indexOf(p);
      const d = new Date(p.date + "T00:00:00");
      const label = `${d.getMonth() + 1}/${d.getDate()}`;
      return { x: xScale(idx), label };
    });

  const yStep = Math.max(1, Math.ceil(rangeY / 5));
  const yLabels: { y: number; label: string }[] = [];
  for (let v = Math.ceil(minY / yStep) * yStep; v <= maxY; v += yStep) {
    yLabels.push({ y: yScale(v), label: `${v}°` });
  }

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = svgRef.current;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const scaleX = w / rect.width;
      const mouseX = (e.clientX - rect.left) * scaleX;

      // Find the closest point
      let closestIdx = 0;
      let closestDist = Infinity;
      for (let i = 0; i < points.length; i++) {
        const px = xScale(i);
        const dist = Math.abs(px - mouseX);
        if (dist < closestDist) {
          closestDist = dist;
          closestIdx = i;
        }
      }

      const pt = points[closestIdx];
      if (pt.value == null) {
        setHover(null);
        return;
      }

      setHover({
        x: xScale(closestIdx),
        y: yScale(pt.value),
        date: pt.date,
        tempF: pt.value,
        percentile: pt.percentile,
      });
    },
    [points]
  );

  const handleMouseLeave = useCallback(() => setHover(null), []);

  return (
    <div>
      <h3 className="text-sm font-medium text-neutral-700 mb-0.5">
        Average temperature for {stationName}
      </h3>
      <p className="text-xs text-neutral-400 mb-3">
        {windowDays}-day rolling average
      </p>
      <div className="relative">
        <svg
          ref={svgRef}
          viewBox={`0 0 ${w} ${h}`}
          className="w-full"
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
        >
          {/* Grid lines */}
          {yLabels.map((yl) => (
            <line
              key={yl.label}
              x1={pad.left}
              x2={w - pad.right}
              y1={yl.y}
              y2={yl.y}
              stroke="#f5f5f5"
              strokeWidth={1}
            />
          ))}

          {/* 10-90 band */}
          <path d={bandPath("p10", "p90")} fill="rgba(0,113,227,0.05)" />
          {/* 25-75 band */}
          <path d={bandPath("p25", "p75")} fill="rgba(0,113,227,0.10)" />

          {/* Median */}
          <path
            d={linePath("p50")}
            fill="none"
            stroke="rgba(0,0,0,0.15)"
            strokeWidth={1.5}
            strokeDasharray="4 3"
          />

          {/* Current period line */}
          <path
            d={linePath("value")}
            fill="none"
            stroke="#0071e3"
            strokeWidth={2.5}
            strokeLinejoin="round"
          />

          {/* Hover crosshair */}
          {hover && (
            <>
              <line
                x1={hover.x}
                x2={hover.x}
                y1={pad.top}
                y2={pad.top + plotH}
                stroke="#a3a3a3"
                strokeWidth={1}
                strokeDasharray="3 2"
              />
              <circle
                cx={hover.x}
                cy={hover.y}
                r={5}
                fill="#0071e3"
                stroke="white"
                strokeWidth={2}
              />
            </>
          )}

          {/* X labels */}
          {xLabels.map((xl, i) => (
            <text
              key={i}
              x={xl.x}
              y={h - 8}
              textAnchor="middle"
              className="fill-neutral-400"
              fontSize={11}
            >
              {xl.label}
            </text>
          ))}

          {/* Y labels */}
          {yLabels.map((yl, i) => (
            <text
              key={i}
              x={pad.left - 8}
              y={yl.y + 4}
              textAnchor="end"
              className="fill-neutral-400"
              fontSize={11}
            >
              {yl.label}
            </text>
          ))}
        </svg>

        {/* Tooltip */}
        {hover && (
          <div
            className="absolute pointer-events-none bg-white border border-neutral-200 rounded-lg shadow-md px-3 py-2 text-sm"
            style={{
              left: `${(hover.x / w) * 100}%`,
              top: `${(hover.y / h) * 100}%`,
              transform: "translate(-50%, -120%)",
            }}
          >
            <div className="text-xs text-neutral-400">
              {new Date(hover.date + "T00:00:00").toLocaleDateString("en-US", {
                month: "short",
                day: "numeric",
                year: "numeric",
              })}
            </div>
            <div className="font-semibold tabular-nums">
              {hover.tempF.toFixed(1)}°F
            </div>
            {hover.percentile != null && (
              <div className="text-xs text-neutral-500">
                {ordinal(Math.round(hover.percentile))} percentile
              </div>
            )}
          </div>
        )}
      </div>
      <div className="flex gap-4 mt-2 text-xs text-neutral-400">
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded-sm" style={{ background: "rgba(0,113,227,0.10)" }} />
          25th–75th pctl
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded-sm" style={{ background: "rgba(0,113,227,0.05)" }} />
          10th–90th pctl
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-6 border-t border-dashed border-neutral-300" />
          Median
        </span>
      </div>
    </div>
  );
}
