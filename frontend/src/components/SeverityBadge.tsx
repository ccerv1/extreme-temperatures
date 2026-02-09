const SEVERITY_CONFIG: Record<string, { label: string; color: string; bg: string }> = {
  extreme: { label: "Extreme", color: "text-red-700", bg: "bg-red-50" },
  very_unusual: { label: "Very Unusual", color: "text-orange-700", bg: "bg-orange-50" },
  unusual: { label: "Unusual", color: "text-amber-700", bg: "bg-amber-50" },
  normal: { label: "Normal", color: "text-neutral-600", bg: "bg-neutral-50" },
  insufficient_data: { label: "Insufficient Data", color: "text-neutral-400", bg: "bg-neutral-50" },
};

export default function SeverityBadge({ severity }: { severity: string }) {
  const config = SEVERITY_CONFIG[severity] ?? SEVERITY_CONFIG.normal;
  return (
    <span
      className={`inline-block rounded-full px-3 py-0.5 text-xs font-medium tracking-wide ${config.color} ${config.bg}`}
    >
      {config.label}
    </span>
  );
}
