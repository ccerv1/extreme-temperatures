import type { Insight } from "@/types";

function celsiusToFahrenheit(c: number): number {
  return c * 9 / 5 + 32;
}

function periodLabel(windowDays: number): string {
  if (windowDays === 1) return "day";
  if (windowDays <= 3) return `${windowDays} days`;
  if (windowDays <= 7) return "week";
  if (windowDays <= 14) return "two weeks";
  if (windowDays <= 30) return "month";
  if (windowDays <= 90) return "quarter";
  return "period";
}

function daysLabel(windowDays: number): string {
  if (windowDays === 1) return "day";
  return `${windowDays} days`;
}

const COLD_STYLES: Record<string, { text: string; intensity: string }> = {
  extreme:      { text: "text-blue-700",   intensity: "extremely " },
  very_unusual: { text: "text-blue-600",   intensity: "very " },
  unusual:      { text: "text-blue-500",   intensity: "unusually " },
};

const WARM_STYLES: Record<string, { text: string; intensity: string }> = {
  extreme:      { text: "text-red-700",    intensity: "extremely " },
  very_unusual: { text: "text-orange-600", intensity: "very " },
  unusual:      { text: "text-amber-600",  intensity: "unusually " },
};

export default function InsightCard({ insight }: { insight: Insight }) {
  const tempF = insight.value != null ? celsiusToFahrenheit(insight.value) : null;
  const period = periodLabel(insight.window_days);
  const days = daysLabel(insight.window_days);

  const isCold = insight.percentile != null && insight.percentile <= 50;
  const direction = isCold ? "cold" : "warm";
  const styles = isCold ? COLD_STYLES : WARM_STYLES;
  const style = styles[insight.severity];

  let headline: React.ReactNode;
  if (insight.severity === "normal" || insight.severity === "insufficient_data" || !style) {
    headline = <>The last {days} have been near normal.</>;
  } else {
    const coloredPart = `${style.intensity}${direction}`;
    headline = (
      <>
        The last {days} have been{" "}
        <span className={`${style.text} font-bold`}>{coloredPart}</span>.
      </>
    );
  }

  return (
    <div className="space-y-3">
      <h2 className="text-2xl font-semibold tracking-tight leading-tight">
        {headline}
      </h2>

      <p className="text-sm text-neutral-500 leading-relaxed">
        {insight.supporting_line}
      </p>

      {tempF != null && (
        <div className="flex items-baseline gap-4 pt-1">
          <span className="text-4xl font-light tabular-nums tracking-tight">
            {tempF.toFixed(1)}
            <span className="text-lg text-neutral-400">°F</span>
          </span>
          {insight.normal_band && (
            <span className="text-xs text-neutral-400">
              Normal: {celsiusToFahrenheit(insight.normal_band.p25).toFixed(0)}–
              {celsiusToFahrenheit(insight.normal_band.p75).toFixed(0)}°F
            </span>
          )}
        </div>
      )}
    </div>
  );
}
