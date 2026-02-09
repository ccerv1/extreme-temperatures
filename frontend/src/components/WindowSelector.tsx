"use client";

const PRESETS = [
  { days: 3, label: "3 days" },
  { days: 7, label: "7 days" },
  { days: 30, label: "30 days" },
];

const PRESET_DAYS = new Set(PRESETS.map((p) => p.days));

const DROPDOWN_WINDOWS = [5, 10, 14, 21, 28, 45, 60, 75, 90, 180, 365];

interface Props {
  value: number;
  onChange: (days: number) => void;
}

export default function WindowSelector({ value, onChange }: Props) {
  return (
    <div className="flex items-center gap-2">
      {PRESETS.map((p) => (
        <button
          key={p.days}
          onClick={() => onChange(p.days)}
          className={`rounded-full px-3 py-1 text-sm font-medium transition-colors ${
            value === p.days
              ? "bg-neutral-900 text-white"
              : "bg-neutral-100 text-neutral-600 hover:bg-neutral-200"
          }`}
        >
          {p.label}
        </button>
      ))}
      <select
        value={!PRESET_DAYS.has(value) && DROPDOWN_WINDOWS.includes(value) ? value : ""}
        onChange={(e) => e.target.value && onChange(Number(e.target.value))}
        className="rounded-md border border-neutral-200 bg-white px-2 py-1 text-sm text-neutral-600"
      >
        <option value="" disabled>
          More...
        </option>
        {DROPDOWN_WINDOWS.map((w) => (
          <option key={w} value={w}>
            {`${w} days`}
          </option>
        ))}
      </select>
    </div>
  );
}
