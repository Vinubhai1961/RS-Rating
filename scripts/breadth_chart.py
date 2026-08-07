#!/usr/bin/env python3
"""
Market Breadth chart generator.

Reads a market_breadth CSV (metrics as rows, dates as columns) and writes PNG
charts that GitHub can display directly -- no HTML needed.

Usage:
    python breadth_chart.py market_breadth_2026.csv
    python breadth_chart.py market_breadth_2026.csv --outdir charts --days 60

Outputs (into --outdir, default "charts"):
    breadth_dashboard.png   4-panel summary (the one to embed in README)
    ma_participation.png    % above 20/50/200 SMA
    net_thrust.png          net stocks up/down 4.5% per day
    momentum_5d.png         stocks +/-20% in 5 days
    highs_lows.png          % within 25% of 52wk high vs low
    README_snippet.md       markdown to paste into your README

Requires: pandas, matplotlib   (pip install pandas matplotlib)
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")            # headless -- required for GitHub Actions
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

# ---------------------------------------------------------------- styling
GREEN, RED, BLUE, GOLD, INK, GRID = "#0a7d43", "#b3261e", "#1f6feb", "#a06000", "#1c1c1c", "#e8e8e8"

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": "#cccccc",
    "axes.labelcolor": INK,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "text.color": INK,
    "xtick.color": "#666666",
    "ytick.color": "#666666",
    "font.size": 10,
    "legend.frameon": False,
    "figure.dpi": 130,
    "savefig.bbox": "tight",
    "savefig.facecolor": "white",
})


# ---------------------------------------------------------------- loading
def load(path, days=None):
    """Return a DataFrame indexed by date, one column per metric."""
    raw = pd.read_csv(path)
    raw = raw.rename(columns={raw.columns[0]: "Metric"}).set_index("Metric")
    df = raw.T
    df.index = pd.to_datetime(df.index, format="%m/%d/%Y", errors="coerce")
    df = df[df.index.notna()].sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")
    if days:
        df = df.tail(days)
    return df


def tidy(ax, df, pct=False):
    ax.set_xlim(df.index.min(), df.index.max())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
    ax.spines[["top", "right"]].set_visible(False)
    if pct:
        ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")


def label_last(ax, df, col, color, name, dy=8):
    """Annotate the final value of a series (dy staggers overlapping labels)."""
    s = df[col].dropna()
    if s.empty:
        return
    ax.annotate(f"{name} {s.iloc[-1]:.1f}",
                xy=(s.index[-1], s.iloc[-1]),
                xytext=(-4, dy), textcoords="offset points",
                ha="right", fontsize=9, fontweight="bold", color=color)


# ---------------------------------------------------------------- panels
def panel_ma(ax, df):
    for col, color, name, dy in [("Above 20SMA %", BLUE, "20-DMA", -18),
                                 ("Above 50SMA %", GREEN, "50-DMA", -32),
                                 ("Above 200SMA %", GOLD, "200-DMA", 10)]:
        if col in df:
            ax.plot(df.index, df[col], color=color, lw=2.2, label=f"% > {name}")
            label_last(ax, df, col, color, name, dy)
    ax.axhline(50, color="#999999", lw=1, ls="--")
    ax.set_title("Participation: % of stocks above moving averages")
    ax.legend(loc="lower left", ncol=3, fontsize=9)
    tidy(ax, df, pct=True)


def panel_thrust(ax, df):
    col = "Net 4.5% Today"
    if col not in df:
        return
    v = df[col]
    ax.bar(df.index, v, color=[GREEN if x >= 0 else RED for x in v], width=0.75, alpha=0.9)
    ax.axhline(0, color="#888888", lw=1)
    ax.set_title("Daily thrust: net stocks up 4.5% minus down 4.5%")
    tidy(ax, df)


def panel_momentum(ax, df):
    if "UP 20% in 5Days" in df:
        ax.plot(df.index, df["UP 20% in 5Days"], color=GREEN, lw=2.2, label="Up 20% in 5 days")
        label_last(ax, df, "UP 20% in 5Days", GREEN, "up")
    if "Down 20% in 5Days" in df:
        ax.plot(df.index, df["Down 20% in 5Days"], color=RED, lw=2.2, label="Down 20% in 5 days")
        label_last(ax, df, "Down 20% in 5Days", RED, "dn")
    ax.set_ylim(bottom=0)
    ax.set_title("Momentum extremes: stocks moving +/-20% in 5 days")
    ax.legend(loc="upper left", fontsize=9)
    tidy(ax, df)


def panel_hl(ax, df):
    if "0-25% 52WKH %" in df:
        ax.plot(df.index, df["0-25% 52WKH %"], color=GREEN, lw=2.2, label="Within 25% of 52wk HIGH")
        label_last(ax, df, "0-25% 52WKH %", GREEN, "high")
    if "0-25% 52WKL %" in df:
        ax.plot(df.index, df["0-25% 52WKL %"], color=RED, lw=2.2, label="Within 25% of 52wk LOW")
        label_last(ax, df, "0-25% 52WKL %", RED, "low")
    ax.set_title("Proximity to 52-week highs vs lows")
    ax.legend(loc="center left", fontsize=9)
    tidy(ax, df, pct=True)


PANELS = [
    ("ma_participation.png", panel_ma),
    ("net_thrust.png", panel_thrust),
    ("momentum_5d.png", panel_momentum),
    ("highs_lows.png", panel_hl),
]


# ---------------------------------------------------------------- output
def subtitle(df):
    a, b = df.index[0].strftime("%d %b %Y"), df.index[-1].strftime("%d %b %Y")
    n = df["Total Stocks"].iloc[-1] if "Total Stocks" in df else None
    tail = f"  |  universe {int(n):,} stocks" if pd.notna(n) else ""
    return f"{len(df)} sessions  |  {a} - {b}{tail}"


def dashboard(df, outdir):
    fig, axes = plt.subplots(2, 2, figsize=(14, 8.5))
    for ax, (_, fn) in zip(axes.ravel(), PANELS):
        fn(ax, df)
    fig.tight_layout(rect=[0, 0, 1, 0.915])
    fig.text(0.008, 0.985, "Market Breadth Dashboard", ha="left", va="top",
             fontsize=20, fontweight="bold")
    fig.text(0.008, 0.935, subtitle(df), ha="left", va="top",
             fontsize=10, color="#666666")
    path = os.path.join(outdir, "breadth_dashboard.png")
    fig.savefig(path)
    plt.close(fig)
    return path


def singles(df, outdir):
    out = []
    for name, fn in PANELS:
        fig, ax = plt.subplots(figsize=(10, 4.6))
        fn(ax, df)
        fig.text(0.005, -0.02, subtitle(df), fontsize=8, color="#888888")
        path = os.path.join(outdir, name)
        fig.savefig(path)
        plt.close(fig)
        out.append(path)
    return out


def summary(df):
    """Plain-text stats printed to the console / Actions log."""
    lines = []
    for col in ["Above 20SMA %", "Above 50SMA %", "Above 200SMA %",
                "0-25% 52WKH %", "0-25% 52WKL %"]:
        if col in df:
            s = df[col].dropna()
            if len(s) > 1:
                lines.append(f"  {col:<18} {s.iloc[0]:6.2f} -> {s.iloc[-1]:6.2f}  "
                             f"({s.iloc[-1] - s.iloc[0]:+.2f})")
    if "Net 4.5% Today" in df:
        v = df["Net 4.5% Today"].dropna()
        lines.append(f"  Net 4.5% positive on {int((v > 0).sum())}/{len(v)} sessions; "
                     f"last = {v.iloc[-1]:+.0f}")
    return "\n".join(lines)


SNIPPET = """## Market Breadth

![Market breadth dashboard](charts/breadth_dashboard.png)

<details>
<summary>Individual charts</summary>

![Above moving averages](charts/ma_participation.png)
![Net 4.5% thrust](charts/net_thrust.png)
![5-day momentum extremes](charts/momentum_5d.png)
![52-week high/low proximity](charts/highs_lows.png)

</details>
"""


def main():
    p = argparse.ArgumentParser(description="Generate market breadth PNG charts.")
    p.add_argument("csv", help="path to the market breadth CSV")
    p.add_argument("--outdir", default="charts", help="output directory (default: charts)")
    p.add_argument("--days", type=int, default=None, help="use only the last N sessions")
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = load(args.csv, args.days)
    if df.empty:
        raise SystemExit("No dated columns parsed - check the CSV header format (MM/DD/YYYY).")

    made = [dashboard(df, args.outdir)] + singles(df, args.outdir)

    snippet = os.path.join(args.outdir, "README_snippet.md")
    with open(snippet, "w") as f:
        f.write(SNIPPET)

    print(subtitle(df))
    print(summary(df))
    print("\nWrote:")
    for m in made + [snippet]:
        print("  " + m)


if __name__ == "__main__":
    main()
