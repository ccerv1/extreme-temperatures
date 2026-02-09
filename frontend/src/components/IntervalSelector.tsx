"use client";

const CURRENT_YEAR = new Date().getFullYear();

const INTERVALS = [
  { label: "All", sinceYear: null },
  { label: "25yr", sinceYear: CURRENT_YEAR - 25 + 1 },
  { label: "50yr", sinceYear: CURRENT_YEAR - 50 + 1 },
  { label: "75yr", sinceYear: CURRENT_YEAR - 75 + 1 },
  { label: "100yr", sinceYear: CURRENT_YEAR - 100 + 1 },
];

interface Props {
  value: number | null;
  onChange: (sinceYear: number | null) => void;
  firstObsYear?: number | null;
}

export default function IntervalSelector({ value, onChange, firstObsYear }: Props) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs text-neutral-400 mr-1">Compare vs</span>
      {INTERVALS.map((interval) => {
        const disabled =
          firstObsYear != null &&
          interval.sinceYear != null &&
          interval.sinceYear < firstObsYear;

        return (
          <button
            key={interval.label}
            onClick={() => !disabled && onChange(interval.sinceYear)}
            disabled={disabled}
            className={`rounded-full px-3 py-1 text-sm font-medium transition-colors ${
              value === interval.sinceYear
                ? "bg-neutral-900 text-white"
                : disabled
                  ? "bg-neutral-50 text-neutral-300 cursor-not-allowed"
                  : "bg-neutral-100 text-neutral-600 hover:bg-neutral-200"
            }`}
          >
            {interval.label}
          </button>
        );
      })}
    </div>
  );
}
