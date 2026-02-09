import type { StationRecord } from "@/types";

function cToF(c: number): number {
  return c * 9 / 5 + 32;
}

function formatDate(d: string): string {
  const date = new Date(d + "T00:00:00");
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

interface Props {
  records: StationRecord[];
  type: "highest" | "lowest";
}

export default function RecordsTable({ records, type }: Props) {
  const filtered = records.filter((r) => r.record_type === type);
  if (filtered.length === 0) return null;

  const isHot = type === "highest";
  const title = isHot ? "Hottest Streaks" : "Coldest Streaks";
  const accent = isHot ? "text-red-700" : "text-blue-700";
  const headerBg = isHot ? "bg-red-50" : "bg-blue-50";

  return (
    <div className="rounded-lg border border-neutral-200 overflow-hidden">
      <div className={`px-3 py-2 text-sm font-semibold ${accent} ${headerBg}`}>
        {title}
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-neutral-100">
            <th className="px-3 py-1.5 text-left text-xs font-medium text-neutral-400">Duration</th>
            <th className="px-3 py-1.5 text-left text-xs font-medium text-neutral-400">Dates</th>
            <th className="px-3 py-1.5 text-right text-xs font-medium text-neutral-400">Avg Temp</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((r) => (
            <tr key={`${r.window_days}-${r.record_type}`} className="border-b border-neutral-50 last:border-0">
              <td className="px-3 py-1.5 font-medium whitespace-nowrap">
                {r.window_days === 1 ? "1 day" : `${r.window_days} days`}
              </td>
              <td className="px-3 py-1.5 text-neutral-500 whitespace-nowrap">
                {r.window_days === 1
                  ? formatDate(r.start_date)
                  : `${formatDate(r.start_date).replace(/,\s*\d{4}/, "")} – ${formatDate(r.end_date)}`}
              </td>
              <td className={`px-3 py-1.5 text-right font-semibold ${accent}`}>
                {cToF(r.value).toFixed(1)}°F
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
