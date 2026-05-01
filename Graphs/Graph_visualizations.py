import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("../Data/0dteX.csv")
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

calls = df[df["isCall"] == 1]
puts  = df[df["isCall"] == 0]

# ── 1. Scatter: delta vs IV, color=isCall, size=vega ──────────────────────────
sample = df.sample(n=min(6000, len(df)), random_state=42)

vega_abs = sample["vega"].abs()
sizes = 10 + 80 * (vega_abs - vega_abs.min()) / (vega_abs.max() - vega_abs.min() + 1e-9)
colors = sample["isCall"].map({1: "#2196F3", 0: "#F44336"})

fig, ax = plt.subplots(figsize=(10, 6))
sc = ax.scatter(
    sample["delta"], sample["iv"],
    c=colors, s=sizes, alpha=0.45, linewidths=0
)
ax.set_xlabel("Delta")
ax.set_ylabel("Implied Volatility")
ax.set_title("Delta vs IV  |  color = Call/Put  |  size = |Vega|")
legend_handles = [
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#2196F3', markersize=9, label='Call'),
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#F44336', markersize=9, label='Put'),
]
ax.legend(handles=legend_handles)
plt.tight_layout()
plt.savefig("../Graphs/scatter_delta_iv.png", dpi=150)
plt.show()

# ── 2. Histogram: option price (Target) distribution by call/put ──────────────
fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(calls["Target"].clip(upper=calls["Target"].quantile(0.99)),
        bins=80, alpha=0.6, color="#2196F3", label="Call", density=True)
ax.hist(puts["Target"].clip(upper=puts["Target"].quantile(0.99)),
        bins=80, alpha=0.6, color="#F44336", label="Put",  density=True)
ax.set_xlabel("Option Price (Target)")
ax.set_ylabel("Density")
ax.set_title("Option Price Distribution  |  Calls vs Puts  (clipped at 99th pct)")
ax.legend()
plt.tight_layout()
plt.savefig("../Graphs/hist_option_price.png", dpi=150)
plt.show()

# ── 3. Line: median IV vs minutes-to-expiry for calls and puts ────────────────
iv_by_exp = (
    df.groupby(["min_to_exp", "isCall"])["iv"]
    .median()
    .reset_index()
    .sort_values("min_to_exp")
)
call_line = iv_by_exp[iv_by_exp["isCall"] == 1]
put_line  = iv_by_exp[iv_by_exp["isCall"] == 0]

fig, ax = plt.subplots(figsize=(11, 5))
ax.plot(call_line["min_to_exp"], call_line["iv"], color="#2196F3", lw=1.5, label="Call")
ax.plot(put_line["min_to_exp"],  put_line["iv"],  color="#F44336", lw=1.5, label="Put")
ax.set_xlabel("Minutes to Expiry")
ax.set_ylabel("Median Implied Volatility")
ax.set_title("Intraday IV Term Structure  |  Median IV by Minutes-to-Expiry")
ax.legend()
plt.tight_layout()
plt.savefig("../Graphs/line_iv_term_structure.png", dpi=150)
plt.show()
