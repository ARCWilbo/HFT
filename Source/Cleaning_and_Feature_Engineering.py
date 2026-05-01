import psycopg
import gc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from scipy.optimize import brentq
from typing import Dict, List, Optional

"""
ts = ns unix (UTC)
"""

DB_CONFIG = "dbname=hft_db user=arcwilbo"

SUB_QUERY = """
    SELECT *
    FROM option_orders
    ORDER BY event_timestamp ASC;
    """

# ── dtype map applied immediately after SQL pull ──────────────────────────────
DTYPE_MAP = {
    "sectype":      "category",
    "ticker":       "category",
    "side":         "int8",
    "operation":    "int8",
    "position":     "int8",
    "size":         "int32",
    "price":        "float32",
    "strike":       "float32",
    "option_right": "category",
    "option_exp":   "category",
}

RISK_FREE_RATE: float = 0.0426          # annualised, update as needed
TRADING_DAYS:   int   = 252


# ══════════════════════════════════════════════════════════════════════════════
#  Black-Scholes helpers (fully vectorised – operate on np.ndarray)
# ══════════════════════════════════════════════════════════════════════════════

def _bs_d1_d2(S: np.ndarray, K: float, T: np.ndarray,
              r: float, sigma: np.ndarray):
    """Return (d1, d2) arrays; guards against T≤0 and sigma≤0."""
    T     = np.maximum(T, 1e-9)
    sigma = np.maximum(sigma, 1e-9)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return d1, d2


def bs_price(S: np.ndarray, K: float, T: np.ndarray,
             r: float, sigma: np.ndarray, is_call: np.ndarray) -> np.ndarray:
    d1, d2 = _bs_d1_d2(S, K, T, r, sigma)
    call   = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    put    = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return np.where(is_call, call, put).astype(np.float32)


def bs_greeks(S: np.ndarray, K: float, T: np.ndarray,
              r: float, sigma: np.ndarray,
              is_call: np.ndarray) -> Dict[str, np.ndarray]:
    """Returns delta, gamma, theta (per day), vega (per 1-pt σ move)."""
    T     = np.maximum(T, 1e-9)
    sigma = np.maximum(sigma, 1e-9)
    d1, d2 = _bs_d1_d2(S, K, T, r, sigma)
    pdf_d1 = norm.pdf(d1)
    sqrt_T = np.sqrt(T)

    # delta
    delta = np.where(is_call, norm.cdf(d1), norm.cdf(d1) - 1).astype(np.float32)

    # gamma (same for call and put)
    gamma = (pdf_d1 / (S * sigma * sqrt_T)).astype(np.float32)

    # theta (annualised → per calendar day)
    theta_call = (
        -S * pdf_d1 * sigma / (2 * sqrt_T)
        - r * K * np.exp(-r * T) * norm.cdf(d2)
    )
    theta_put = (
        -S * pdf_d1 * sigma / (2 * sqrt_T)
        + r * K * np.exp(-r * T) * norm.cdf(-d2)
    )
    theta = (np.where(is_call, theta_call, theta_put) / 365).astype(np.float32)

    # vega (per 1% σ move → divide annualised vega by 100)
    vega = (S * sqrt_T * pdf_d1 / 100).astype(np.float32)

    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}


def compute_iv(market_price: np.ndarray, S: np.ndarray, K: float,
               T: np.ndarray, r: float,
               is_call: np.ndarray) -> np.ndarray:
    """
    Scalar Brent's method per row; falls back to NaN when no root found.
    Fast enough for intraday frame sizes; parallelise with ProcessPoolExecutor
    for very large frames if needed.
    """
    iv = np.full(len(market_price), np.nan, dtype=np.float32)
    for i in range(len(market_price)):
        try:
            def objective(sigma):
                return float(bs_price(
                    np.array([S[i]]), K, np.array([T[i]]),
                    r, np.array([sigma]), np.array([bool(is_call[i])])
                )[0]) - float(market_price[i])
            lo, hi = 1e-4, 20.0
            if objective(lo) * objective(hi) > 0:
                continue
            iv[i] = brentq(objective, lo, hi, xtol=1e-5, maxiter=50)
        except Exception:
            pass
    return iv


# ══════════════════════════════════════════════════════════════════════════════
#  Security
# ══════════════════════════════════════════════════════════════════════════════

class Security:

    def __init__(self, ticker: str, exp: str = None,
                 strike: float = None, right: str = None):
        self.ticker = ticker
        self.exp    = exp
        self.strike = strike
        self.right  = right

    def __str__(self):
        return f"({self.ticker}: {self.right} at {self.strike} on {self.exp})"


# ══════════════════════════════════════════════════════════════════════════════
#  Data_Analysis
# ══════════════════════════════════════════════════════════════════════════════

class Data_Analysis:

    def __init__(self):
        self.main_df: pd.DataFrame       = self._pull_and_cast()
        self.OPT_Security_list: List[Security] = self._unique_contracts("OPT")
        self.STK_Security_list: List[Security] = self._unique_contracts("STK")

    # ── ingest ────────────────────────────────────────────────────────────────

    def _pull_and_cast(self): # -> pd.DataFrame
        """
        Pull from SQL, downcast dtypes immediately to cut RAM usage ~40-50%.
        Uses server-side cursor so the DB streams rows instead of sending
        the entire result-set in one network packet.
        """
        with psycopg.connect(DB_CONFIG) as conn:
            # name= turns this into a server-side cursor (streaming)
            with conn.cursor(name="stream_cur") as cur:
                cur.itersize = 50_000          # fetch 50k rows at a time
                cur.execute(SUB_QUERY)
                cols = [desc[0] for desc in cur.description]
                chunks = []
                while True:
                    rows = cur.fetchmany(50_000)
                    if not rows:
                        break
                    chunks.append(pd.DataFrame(rows, columns=cols))
                df = pd.concat(chunks, ignore_index=True)
                del chunks
                gc.collect()

        if df.empty:
            raise ValueError("SQL Query returned nothing")

        # downcast
        for col, dtype in DTYPE_MAP.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], unit="ns")
        df.set_index("event_timestamp", inplace=True)
        df.sort_index(inplace=True)

        print(f"\nDF Shape: {df.shape}  |  Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB\n")
        return df

    # ── contract discovery ────────────────────────────────────────────────────

    def _unique_contracts(self, sec_type: str) -> List[Security]:
        if sec_type not in ("STK", "OPT"):
            raise ValueError("sec_type must be 'STK' or 'OPT'")

        sec_df = self.main_df[self.main_df["sectype"] == sec_type]

        if sec_type == "STK":
            return [Security(ticker=t) for t in sec_df["ticker"].unique()]

        rows = (sec_df[["ticker", "option_exp", "strike", "option_right"]]
                .drop_duplicates())
        return [
            Security(ticker=r.ticker, exp=r.option_exp,
                     strike=r.strike, right=r.option_right)
            for r in rows.itertuples(index=False)
        ]

    # ── STK order book ────────────────────────────────────────────────────────

    def df_STK_order_book(self, master_df: pd.DataFrame,
                          sec: Security) -> pd.DataFrame:
        df = master_df[
            (master_df["sectype"] == "STK") &
            (master_df["ticker"]  == sec.ticker) &
            (master_df["side"]    != 2)
        ]

        data        = []
        template    = {}
        prev_val    = {}
        add_temp    = True

        for idx, row in df.iterrows():

            if row["operation"] == 0:
                template["event_timestamp"] = idx
                template["ticker"]          = row["ticker"]
                template["sectype"]         = row["sectype"]
                side = "ask" if row["side"] == 0 else "bid"
                template[f"{side}_size_{row['position']}"]  = row["size"]
                template[f"{side}_price_{row['position']}"] = row["price"]

            elif row["operation"] == 1:
                if add_temp:
                    data.append(template)
                    add_temp  = False
                    prev_val  = template.copy()

                side = "ask" if row["side"] == 0 else "bid"
                prev_val[f"{side}_size_{row['position']}"]  = row["size"]
                prev_val[f"{side}_price_{row['position']}"] = row["price"]
                prev_val["event_timestamp"] = idx

                if prev_val.get("bid_price_0", 0) < prev_val.get("ask_price_0", float("inf")):
                    data.append(prev_val.copy())

        if not data:
            return pd.DataFrame()

        stk_df = pd.DataFrame(data)
        # downcast price/size columns
        for c in stk_df.columns:
            if "price" in c:
                stk_df[c] = stk_df[c].astype(np.float32)
            elif "size" in c:
                stk_df[c] = stk_df[c].astype(np.int32)

        stk_df["event_timestamp"] = pd.to_datetime(stk_df["event_timestamp"], unit="ns")
        stk_df.set_index("event_timestamp", inplace=True)
        stk_df.sort_index(inplace=True)
        return stk_df

    # ── OPT order book ────────────────────────────────────────────────────────

    def df_OPT_order_book(self, master_df: pd.DataFrame,
                          OPT_sec: Security) -> pd.DataFrame:
        df = master_df[
            (master_df["sectype"]      == "OPT") &
            (master_df["ticker"]       == OPT_sec.ticker) &
            (master_df["side"]         != 2) &
            (master_df["option_exp"]   == OPT_sec.exp) &
            (master_df["strike"]       == OPT_sec.strike) &
            (master_df["option_right"] == OPT_sec.right)
        ]

        data     = []
        template = {"bid_price": None, "bid_size": 0,
                    "ask_price": None, "ask_size": 0,
                    "event_timestamp": None}

        for idx, row in df.iterrows():
            template["event_timestamp"] = idx

            if row["side"] == 0:            # ask
                if row["size"] != -1:
                    template["ask_size"]  = row["size"]
                else:
                    template["ask_price"] = row["price"]
                    template["ask_size"]  = 0
            elif row["side"] == 1:          # bid
                if row["size"] != -1:
                    template["bid_size"]  = row["size"]
                else:
                    template["bid_price"] = row["price"]
                    template["bid_size"]  = 0

            if (template["bid_size"]  != 0 and
                    template["ask_size"]  != 0 and
                    template["bid_price"] and
                    template["ask_price"] and
                    template["bid_price"] < template["ask_price"]):
                data.append(template.copy())

        if not data:
            return pd.DataFrame()

        opt_df = pd.DataFrame(data, columns=["bid_price", "bid_size",
                                              "ask_price", "ask_size",
                                              "event_timestamp"])
        opt_df[["bid_price", "ask_price"]] = (
            opt_df[["bid_price", "ask_price"]].astype(np.float32))
        opt_df[["bid_size", "ask_size"]] = (
            opt_df[["bid_size", "ask_size"]].astype(np.int32))
        opt_df["event_timestamp"] = pd.to_datetime(
            opt_df["event_timestamp"], unit="ns")
        opt_df.set_index("event_timestamp", inplace=True)
        opt_df.sort_index(inplace=True)
        return opt_df

    # ── trade reconstruction ──────────────────────────────────────────────────

    def df_Trades(self, master_df: pd.DataFrame,
                  sec: Security) -> pd.DataFrame:
        sec_type = "STK" if sec.strike is None else "OPT"

        df = master_df[
            (master_df["ticker"]  == sec.ticker) &
            (master_df["side"]    == 2) &
            (master_df["sectype"] == sec_type)
        ]
        if sec_type == "OPT":
            df = df[
                (df["strike"]       == sec.strike) &
                (df["option_exp"]   == sec.exp) &
                (df["option_right"] == sec.right)
            ]

        data     = []
        template = {"price": None, "size": 0, "event_timestamp": None}

        for idx, row in df.iterrows():
            template["event_timestamp"] = idx

            if row["price"] != -1:
                if template["size"] != 0:
                    data.append(template.copy())
                template["price"] = row["price"]
                template["size"]  = 0
            elif row["size"] != -1:
                template["size"] += row["size"]

        if template["size"] != 0:
            data.append(template.copy())

        if not data:
            return pd.DataFrame()

        df_t = pd.DataFrame(data, columns=["price", "size", "event_timestamp"])
        df_t["price"] = df_t["price"].astype(np.float32)
        df_t["size"]  = df_t["size"].astype(np.int32)
        df_t["event_timestamp"] = pd.to_datetime(
            df_t["event_timestamp"], unit="ns")
        df_t.set_index("event_timestamp", inplace=True)
        return df_t

    # ── rolling VWAP helper ───────────────────────────────────────────────────

    @staticmethod
    def _rolling_vwap(trades_df: pd.DataFrame,
                      window: str = "60s") -> pd.Series:
        """
        Rolling VWAP = Σ(price × size) / Σ(size) over `window`.
        Returns a Series aligned to trades_df.index.
        """
        pv  = (trades_df["price"] * trades_df["size"]).rolling(window).sum()
        vol = trades_df["size"].rolling(window).sum()
        return (pv / vol).astype(np.float32)

    # ── main feature builder ──────────────────────────────────────────────────

    def create_OMM_df(self, master_df: pd.DataFrame,
                      OPT_sec_list: List[Security],
                      STK_sec: Security) -> Optional[pd.DataFrame]:
        """
        Features produced per OPT contract
        ───────────────────────────────────
        Existing
          min_to_exp, isCall, OPT_vol_60s, OPT_spread, OPT_target_std_60s,
          STK_vol_60s, STK_mid_price, STK_spread, strike_STK_residual, Target

        New
          OPT_ob_imbalance  – (bid_size - ask_size) / (bid_size + ask_size)
          STK_ob_imbalance  – same for STK L1
          OPT_vwap_60s      – rolling 60s VWAP on OPT trades
          STK_vwap_60s      – rolling 60s VWAP on STK trades
          OPT_vwap_residual – OPT mid-price minus OPT VWAP
          STK_vwap_residual – STK mid-price minus STK VWAP
          iv                – implied volatility (Brent's method)
          delta, gamma, theta, vega – Black-Scholes Greeks
        """

        if not OPT_sec_list:
            raise ValueError("OPT_sec_list is empty")

        # ── pre-filter master_df to this ticker only ──────────────────────────
        # This single slice is shared by every contract; no per-contract copies.
        ticker_mask = master_df["ticker"] == STK_sec.ticker
        ticker_df   = master_df[ticker_mask]

        # STK book & trades (built once, reused for every OPT contract)
        STK_book_df   = self.df_STK_order_book(master_df=ticker_df, sec=STK_sec)
        STK_trades_df = self.df_Trades(master_df=ticker_df, sec=STK_sec)

        if STK_book_df.empty:
            raise ValueError(f"No STK Book Data for {STK_sec}")
        if STK_trades_df.empty:
            raise ValueError(f"No STK Trade Data for {STK_sec}")

        # STK features computed once ──────────────────────────────────────────
        STK_trades_df["STK_vol_60s"]  = (
            STK_trades_df["size"].rolling("60s").sum().astype(np.float32))
        STK_trades_df["STK_vwap_60s"] = self._rolling_vwap(STK_trades_df)

        STK_book_df["STK_mid_price"]     = (
            (STK_book_df["bid_price_0"] + STK_book_df["ask_price_0"])
            / 2).astype(np.float32)
        STK_book_df["STK_spread"]        = (
            STK_book_df["ask_price_0"] - STK_book_df["bid_price_0"]
        ).astype(np.float32)
        bid_s = STK_book_df["bid_size_0"].astype(float)
        ask_s = STK_book_df["ask_size_0"].astype(float)
        STK_book_df["STK_ob_imbalance"]  = (
            ((bid_s - ask_s) / (bid_s + ask_s).replace(0, np.nan))
            .astype(np.float32))

        stk_trade_cols = ["STK_vol_60s", "STK_vwap_60s"]
        stk_book_cols  = ["STK_mid_price", "STK_spread", "STK_ob_imbalance"]

        dfs = []

        for OPT_sec in (s for s in OPT_sec_list if s.ticker == STK_sec.ticker):

            OPT_book_df   = self.df_OPT_order_book(
                master_df=ticker_df, OPT_sec=OPT_sec)
            OPT_trades_df = self.df_Trades(
                master_df=ticker_df, sec=OPT_sec)

            if OPT_book_df.empty or OPT_trades_df.empty:
                continue

            min_ts: pd.Timestamp = OPT_book_df.index[0]

            # ── Target ───────────────────────────────────────────────────────
            OPT_book_df["Target"] = (
                (OPT_book_df["bid_price"] + OPT_book_df["ask_price"])
                / 2).round(2).astype(np.float32)

            # ── OPT existing features ─────────────────────────────────────────
            exp_dt = (pd.to_datetime(OPT_sec.exp, format="%Y%m%d")
                      + pd.Timedelta(hours=21, minutes=15))
            OPT_book_df["min_to_exp"] = (
                (exp_dt - OPT_book_df.index).total_seconds() / 60
            ).round().astype(np.float32)

            OPT_book_df["isCall"] = np.int8(OPT_sec.right == "C")

            min_ts = max(min_ts,
                         OPT_trades_df.index[0] + pd.Timedelta(seconds=60))
            OPT_trades_df["OPT_vol_60s"] = (
                OPT_trades_df["size"].rolling("60s").sum().astype(np.float32))
            OPT_book_df = pd.merge_asof(
                OPT_book_df,
                OPT_trades_df[["OPT_vol_60s"]],
                left_index=True, right_index=True, direction="backward")

            OPT_book_df["OPT_spread"] = (
                (OPT_book_df["ask_price"] - OPT_book_df["bid_price"])
                .round(2).astype(np.float32))

            OPT_book_df["OPT_target_std_60s"] = (
                OPT_book_df["Target"].rolling("60s").std()
                .round(2).astype(np.float32))

            # ── OPT new features ──────────────────────────────────────────────

            # order book imbalance
            b = OPT_book_df["bid_size"].astype(float)
            a = OPT_book_df["ask_size"].astype(float)
            OPT_book_df["OPT_ob_imbalance"] = (
                ((b - a) / (b + a).replace(0, np.nan))
                .astype(np.float32))

            # VWAP
            OPT_trades_df["OPT_vwap_60s"] = self._rolling_vwap(OPT_trades_df)
            OPT_book_df = pd.merge_asof(
                OPT_book_df,
                OPT_trades_df[["OPT_vwap_60s"]],
                left_index=True, right_index=True, direction="backward")
            OPT_book_df["OPT_vwap_residual"] = (
                OPT_book_df["Target"] - OPT_book_df["OPT_vwap_60s"]
            ).astype(np.float32)

            # ── STK features merged in ────────────────────────────────────────
            min_ts = max(min_ts,
                         STK_trades_df.index[0] + pd.Timedelta(seconds=60))

            OPT_book_df = pd.merge_asof(
                OPT_book_df,
                STK_trades_df[stk_trade_cols],
                left_index=True, right_index=True, direction="backward")
            OPT_book_df = pd.merge_asof(
                OPT_book_df,
                STK_book_df[stk_book_cols],
                left_index=True, right_index=True, direction="backward")

            OPT_book_df["strike_STK_residual"] = (
                OPT_sec.strike - OPT_book_df["STK_mid_price"]
            ).round(2).astype(np.float32)

            OPT_book_df["STK_vwap_residual"] = (
                OPT_book_df["STK_mid_price"] - OPT_book_df["STK_vwap_60s"]
            ).astype(np.float32)

            # ── Greeks & IV ───────────────────────────────────────────────────
            S       = OPT_book_df["STK_mid_price"].to_numpy(dtype=np.float64)
            K       = float(OPT_sec.strike)
            T       = np.maximum(
                OPT_book_df["min_to_exp"].to_numpy(dtype=np.float64)
                / (TRADING_DAYS * 390),   # convert minutes → year fraction
                1e-9
            )
            is_call = OPT_book_df["isCall"].to_numpy(dtype=bool)

            # IV  (use mid-price as market price proxy)
            mid = OPT_book_df["Target"].to_numpy(dtype=np.float64)
            iv  = compute_iv(mid, S, K, T, RISK_FREE_RATE, is_call)
            OPT_book_df["iv"] = iv

            # Greeks (use IV where available; fall back to 20% flat vol)
            sigma_for_greeks = np.where(np.isnan(iv), 0.20, iv)
            greeks = bs_greeks(S, K, T, RISK_FREE_RATE,
                               sigma_for_greeks, is_call)
            for name, arr in greeks.items():
                OPT_book_df[name] = arr

            # ── final trim ────────────────────────────────────────────────────
            OPT_book_df = OPT_book_df[OPT_book_df.index > min_ts]

            FINAL_COLS = [
                # existing
                "min_to_exp", "isCall",
                "OPT_vol_60s", "OPT_spread", "OPT_target_std_60s",
                "STK_vol_60s", "STK_mid_price", "STK_spread",
                "strike_STK_residual",
                # new
                "OPT_ob_imbalance", "STK_ob_imbalance",
                "OPT_vwap_60s", "STK_vwap_60s",
                "OPT_vwap_residual", "STK_vwap_residual",
                "iv", "delta", "gamma", "theta", "vega",
                # label
                "Target",
            ]
            OPT_book_df = OPT_book_df[FINAL_COLS].dropna()

            if not OPT_book_df.empty:
                dfs.append(OPT_book_df)

                n = min(len(OPT_book_df), 1_000)
                plt.figure(figsize=(10, 6))
                plt.scatter(OPT_book_df["strike_STK_residual"].iloc[:n],
                            OPT_book_df["Target"].iloc[:n], alpha=0.3)
                plt.xlabel("Strike − STK Price")
                plt.ylabel("Option Mid Price")
                plt.title(f"{OPT_sec}  Option Price vs Moneyness")
                plt.tight_layout()
                plt.show()

            # ── free per-contract memory before next iteration ────────────────
            del OPT_book_df, OPT_trades_df
            gc.collect()

        # ── free shared STK objects ────────────────────────────────────────────
        del STK_book_df, STK_trades_df, ticker_df
        gc.collect()

        if not dfs:
            return None

        result = pd.concat(dfs).sort_index().drop_duplicates()
        del dfs
        gc.collect()
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    da = Data_Analysis()

    df_analyze = da.create_OMM_df(
        master_df    = da.main_df,
        OPT_sec_list = da.OPT_Security_list,
        STK_sec      = da.STK_Security_list[0],
    )

    if df_analyze is not None:
        subset = df_analyze[df_analyze["min_to_exp"] < 400]
        subset = subset.drop(columns= ['OPT_vwap_60s', 'STK_vwap_60s' ,'OPT_vwap_residual', 'STK_vwap_residual'])
        subset.to_csv("../Data/0dteX.csv", index=False)
        print(df_analyze.info())
        print(df_analyze.describe())