import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    import plotly.graph_objects as go
    import numpy as np
    from pathlib import Path
    from datetime import timedelta
    from scipy import stats

    return Path, go, np, pd, stats, timedelta


@app.cell
def _(Path):
    DATA_DIR = Path("data/consolidated")
    return (DATA_DIR,)


@app.cell
def _(DATA_DIR, pd):
    def load_station_data(station_id: str) -> pd.DataFrame:
        pattern = f"weather_{station_id}_*.csv"
        files = list(DATA_DIR.glob(pattern))
        all_data = [pd.read_csv(f, parse_dates=['date']) for f in files]
        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.sort_values('date').drop_duplicates(subset=['date'], keep='first').reset_index(drop=True)
        return combined

    return (load_station_data,)


@app.cell
def _(stats):
    def calculate_percentile(current_avg: float, historical_values) -> float:
        return stats.percentileofscore(historical_values.dropna(), current_avg, kind='rank')

    return (calculate_percentile,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    station_input = mo.ui.text(value="94728", label="Station", full_width=False)
    return (station_input,)


@app.cell
def _(load_station_data, station_input):
    station_id = station_input.value
    df = load_station_data(station_id)
    return df, station_id


@app.cell
def _(df, mo, timedelta):
    _max_date = df['date'].max().date()
    _min_date = df['date'].min().date()
    _default_start = max(_min_date, _max_date - timedelta(days=30))

    date_range_input = mo.ui.date_range(
        start=_min_date,
        stop=_max_date,
        value=(_default_start, _max_date),
        label="Period",
    )
    return (date_range_input,)


@app.cell
def _(date_range_input, df, mo, station_input):
    _years = df['date'].max().year - df['date'].min().year
    _n = f"{len(df):,}"
    mo.vstack([
        mo.md(
            f"<div style='font-size:1.5rem;font-weight:700;letter-spacing:-0.025em;color:#1d1d1f'>Extreme Temperatures</div>"
            f"<div style='font-size:0.8rem;color:#86868b;margin-top:2px'>{_years} years of daily data · {_n} observations</div>"
        ),
        mo.hstack([station_input, date_range_input], justify="start", align="end", gap=1.5, widths=[0.2, 0.5]),
    ], gap=0.75)
    return


@app.cell
def _(date_range_input, df, mo, np, pd):
    range_start, range_end = date_range_input.value
    _days = (range_end - range_start).days
    mo.stop(_days > 90, mo.callout(mo.md("Date range cannot exceed 90 days."), kind="warn"))

    _start_ts = pd.Timestamp(range_start)
    _end_ts = pd.Timestamp(range_end)
    selected_year = _start_ts.year

    current = df[(df['date'] >= _start_ts) & (df['date'] <= _end_ts)].copy()
    current['day_offset'] = (current['date'] - _start_ts).dt.days

    _range_dates = pd.date_range(_start_ts, _end_ts)
    _md_pairs = set(zip(_range_dates.month, _range_dates.day))

    historical = df.copy()
    historical['_month'] = historical['date'].dt.month
    historical['_day'] = historical['date'].dt.day
    historical['_year'] = historical['date'].dt.year
    historical = historical[
        historical.apply(lambda r: (r['_month'], r['_day']) in _md_pairs, axis=1)
        & (historical['_year'] != selected_year)
    ]

    _md_to_offset = {(d.month, d.day): i for i, d in enumerate(_range_dates)}
    historical['day_offset'] = historical.apply(
        lambda r: _md_to_offset.get((r['_month'], r['_day'])), axis=1
    )
    historical = historical.dropna(subset=['day_offset'])
    historical['day_offset'] = historical['day_offset'].astype(int)

    daily_stats = historical.groupby('day_offset')['avg'].agg(
        p10=lambda x: np.percentile(x, 10),
        p25=lambda x: np.percentile(x, 25),
        p50=lambda x: np.percentile(x, 50),
        p75=lambda x: np.percentile(x, 75),
        p90=lambda x: np.percentile(x, 90),
    ).reset_index().sort_values('day_offset')

    date_labels = [d.strftime('%-m/%-d') for d in _range_dates]
    hist_yearly_means = historical.groupby('_year')['avg'].mean()
    current_period_avg = current['avg'].mean()
    earliest_year = int(hist_yearly_means.index.min())
    num_days = _days + 1

    return (
        current,
        current_period_avg,
        daily_stats,
        date_labels,
        earliest_year,
        hist_yearly_means,
        num_days,
        range_end,
        range_start,
        selected_year,
    )


@app.cell
def _(current_period_avg, hist_yearly_means, np):
    # Rank current period's avg against historical yearly averages
    # for the same calendar dates (same methodology as the chart bands).
    _vals = np.sort(hist_yearly_means.dropna().values)
    _n_colder = int(np.sum(_vals < current_period_avg))
    _n_warmer = int(np.sum(_vals > current_period_avg))
    # Ranks include the current year
    rank_cold = _n_colder + 1       # 1 = coldest year for these dates
    rank_warm = _n_warmer + 1       # 1 = warmest year for these dates
    total_years = len(_vals) + 1    # historical + current
    return rank_cold, rank_warm, total_years


@app.cell
def _(
    current,
    daily_stats,
    date_labels,
    go,
    range_end,
    range_start,
    selected_year,
):
    _f = '-apple-system, BlinkMacSystemFont, SF Pro Text, Segoe UI, Roboto, Helvetica Neue, sans-serif'
    _blue = '#0071e3'
    _muted = '#86868b'

    _offsets = daily_stats['day_offset'].tolist()
    _labels = [date_labels[i] if i < len(date_labels) else '' for i in _offsets]

    fig = go.Figure()

    # Bands
    fig.add_trace(go.Scatter(
        x=_offsets + _offsets[::-1],
        y=daily_stats['p10'].tolist() + daily_stats['p90'].tolist()[::-1],
        fill='toself', fillcolor='rgba(0,113,227,0.05)',
        line=dict(width=0), showlegend=True, hoverinfo='skip',
        name='10th–90th pctl',
    ))
    fig.add_trace(go.Scatter(
        x=_offsets + _offsets[::-1],
        y=daily_stats['p25'].tolist() + daily_stats['p75'].tolist()[::-1],
        fill='toself', fillcolor='rgba(0,113,227,0.10)',
        line=dict(width=0), showlegend=True, hoverinfo='skip',
        name='25th–75th pctl',
    ))

    # Median
    fig.add_trace(go.Scatter(
        x=_offsets, y=daily_stats['p50'].tolist(),
        mode='lines', line=dict(color='rgba(0,0,0,0.18)', width=1.5, dash='dot'),
        name='Median',
        hovertemplate='%{text}  Median %{y:.1f}°F<extra></extra>', text=_labels,
    ))

    # Selected period
    fig.add_trace(go.Scatter(
        x=current['day_offset'].tolist(), y=current['avg'].tolist(),
        mode='lines+markers',
        line=dict(color=_blue, width=2.5, shape='spline', smoothing=0.3),
        marker=dict(size=5, color=_blue),
        name=str(selected_year),
        hovertemplate='%{text}  <b>%{y:.1f}°F</b><extra></extra>',
        text=[date_labels[i] if i < len(date_labels) else '' for i in current['day_offset'].tolist()],
    ))

    _step = max(1, len(date_labels) // 8)
    _tick_vals = list(range(0, len(date_labels), _step))
    _tick_text = [date_labels[i] for i in _tick_vals]

    fig.update_layout(
        font=dict(family=_f, size=12, color='#1d1d1f'),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='#fff', font_size=12, font_family=_f, bordercolor='#e5e5ea'),
        legend=dict(
            orientation='h', yanchor='top', y=-0.12, xanchor='center', x=0.5,
            font=dict(size=11, color=_muted),
        ),
        margin=dict(l=44, r=8, t=8, b=56),
        height=380,
        xaxis=dict(
            tickvals=_tick_vals, ticktext=_tick_text,
            tickfont=dict(size=11, color=_muted, family=_f),
            showgrid=False, zeroline=False, showline=False, fixedrange=True,
        ),
        yaxis=dict(
            tickfont=dict(size=11, color=_muted, family=_f),
            ticksuffix='°',
            showgrid=True, gridcolor='rgba(0,0,0,0.04)', gridwidth=1,
            zeroline=False, showline=False, title=None, fixedrange=True,
        ),
        plot_bgcolor='white', paper_bgcolor='white', dragmode=False,
    )

    fig
    return


@app.cell
def _():
    def ordinal(n: int) -> str:
        if 11 <= n % 100 <= 13:
            return f"{n}th"
        return f"{n}{['th','st','nd','rd'][min(n % 10, 4)] if n % 10 < 4 else 'th'}"

    return (ordinal,)


@app.cell
def _(ordinal, current_period_avg, earliest_year, mo, num_days, rank_cold, rank_warm, range_end, range_start, total_years):
    _range = f"{range_start.strftime('%b %-d')} – {range_end.strftime('%b %-d, %Y')}"

    # Use rank to determine warm/cold and description
    _is_warm = rank_warm <= rank_cold

    if _is_warm:
        _rank = rank_warm
        _direction = "warmest"
    else:
        _rank = rank_cold
        _direction = "coldest"

    # Descriptive label based on rank position
    _pct_position = _rank / total_years  # 0 = most extreme, 1 = least extreme
    if _pct_position <= 0.05:
        _word = f"unusually {_direction.replace('est', '')}"
        _color = '#bf2600' if _is_warm else '#0d47a1'
    elif _pct_position <= 0.33:
        _word = f"{_direction.replace('est', 'er')} than average"
        _color = '#e65100' if _is_warm else '#1565c0'
    elif _pct_position <= 0.67:
        _word = "near average"
        _color = '#6e6e73'
    else:
        # Rank says "warmest" but it's a high rank (e.g., 25th warmest of 30) = actually cool
        _direction = "coldest" if _is_warm else "warmest"
        _rank = total_years - _rank + 1
        _is_warm = not _is_warm
        _word = f"{_direction.replace('est', 'er')} than average"
        _color = '#e65100' if _is_warm else '#1565c0'

    mo.md(
        f"<div style='font-size:0.9rem;line-height:1.65;color:#1d1d1f;border-top:1px solid #e5e5ea;padding-top:0.75rem;margin-top:0.25rem'>"
        f"<b>{_range}</b> ({num_days} days) · Average temp <b>{current_period_avg:.1f}°F</b> · "
        f"<span style='color:{_color};font-weight:600'>{_word}</span> "
        f"<span style='color:#86868b'>({ordinal(_rank)} {_direction} of {total_years} years with data for these dates, since {earliest_year})</span>"
        f"</div>"
    )
    return


@app.cell
def _(pd):
    STREAK_WINDOWS = [1, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90]

    def find_extreme_streaks(data: pd.DataFrame, mode: str = "coldest") -> pd.DataFrame:
        """Find the most extreme consecutive N-day stretches.

        Args:
            data: DataFrame with 'date' and 'avg' columns, sorted by date.
            mode: 'coldest' or 'hottest'.

        Returns:
            DataFrame with columns: duration, start, end, avg_temp.
        """
        rows = []
        for w in STREAK_WINDOWS:
            if w > len(data):
                continue
            rolling = data['avg'].rolling(w, min_periods=w).mean()
            if mode == "coldest":
                idx = rolling.idxmin()
            else:
                idx = rolling.idxmax()
            end_date = data.loc[idx, 'date']
            start_date = end_date - pd.Timedelta(days=w - 1)
            avg_temp = rolling.loc[idx]
            rows.append({
                'duration': w,
                'start': start_date,
                'end': end_date,
                'avg_temp': avg_temp,
            })
        return pd.DataFrame(rows)

    return (find_extreme_streaks,)


@app.cell
def _(df, find_extreme_streaks, mo):
    _cold = find_extreme_streaks(df, mode="coldest")
    _hot = find_extreme_streaks(df, mode="hottest")

    def _render_table(streaks, title, accent_color, bg_color):
        header = (
            f"<div style='font-size:0.85rem;font-weight:600;color:{accent_color};"
            f"padding:8px 12px;background:{bg_color};border-radius:8px 8px 0 0;"
            f"letter-spacing:0.02em'>{title}</div>"
        )
        rows_html = ""
        for _, r in streaks.iterrows():
            d = r['duration']
            label = "1 day" if d == 1 else f"{d} days"
            start_str = r['start'].strftime('%b %-d, %Y')
            end_str = r['end'].strftime('%b %-d, %Y')
            dates = start_str if d == 1 else f"{r['start'].strftime('%b %-d')} – {end_str}"
            temp = f"{r['avg_temp']:.1f}°F"
            rows_html += (
                f"<tr>"
                f"<td style='padding:6px 12px;font-weight:500;white-space:nowrap'>{label}</td>"
                f"<td style='padding:6px 12px;color:#6e6e73'>{dates}</td>"
                f"<td style='padding:6px 12px;font-weight:600;text-align:right;color:{accent_color}'>{temp}</td>"
                f"</tr>"
            )
        table = (
            f"{header}"
            f"<table style='width:100%;border-collapse:collapse;font-size:0.82rem;line-height:1.5'>"
            f"<thead><tr style='border-bottom:1px solid #e5e5ea'>"
            f"<th style='padding:6px 12px;text-align:left;color:#86868b;font-weight:500'>Duration</th>"
            f"<th style='padding:6px 12px;text-align:left;color:#86868b;font-weight:500'>Dates</th>"
            f"<th style='padding:6px 12px;text-align:right;color:#86868b;font-weight:500'>Avg Temp</th>"
            f"</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            f"</table>"
        )
        return mo.md(f"<div style='border:1px solid #e5e5ea;border-radius:8px;overflow:hidden'>{table}</div>")

    _cold_table = _render_table(_cold, "Coldest Streaks", "#1565c0", "#e3f2fd")
    _hot_table = _render_table(_hot, "Hottest Streaks", "#bf2600", "#fce4ec")

    mo.vstack([
        mo.md("<div style='font-size:0.8rem;color:#86868b;margin-top:0.5rem'>All-time extreme temperature streaks across the full dataset</div>"),
        mo.hstack([_cold_table, _hot_table], gap=1.5, align="start", widths="equal"),
    ], gap=0.5)
    return


if __name__ == "__main__":
    app.run()
