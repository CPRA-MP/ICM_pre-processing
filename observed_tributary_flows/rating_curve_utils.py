import re
import numpy as np
import pandas as pd

# ---------------- basic formatting / palette ----------------
COLORS_DEFAULT = {
    "obs_hist": "#1f77b4",
    "obs_post": "#ff7f0e",
    "one_to_one": "#990099",
    "rc_line": "#ff00ff",
}

def f2(x):
    return f"{x:.2f}" if np.isfinite(x) else "NA"

def f0(x):
    return f"{x:.0f}" if np.isfinite(x) else "NA"

def obs_colors(palette_mode="two-tone", colors=None):
    c = COLORS_DEFAULT if colors is None else colors
    hist = c["obs_hist"]
    post = c["obs_post"] if palette_mode == "two-tone" else c["obs_hist"]
    return hist, post

def axis_minmax(arr, pad_frac=0.03):
    a = np.asarray(arr, float)
    a = a[np.isfinite(a)]
    if a.size == 0:
        return None, None
    lo, hi = a.min(), a.max()
    span = hi - lo if hi > lo else 1.0
    pad = pad_frac * span
    return lo - pad, hi + pad

# ---------------- dataframe / time helpers ----------------
def as_idx(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure dataframe is indexed by datetime (Date column if present)."""
    if df is None:
        return pd.DataFrame()
    if "Date" in df.columns:
        out = df.copy()
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.set_index("Date").sort_index()
        return out
    out = df.copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out.sort_index()
    return out

def daily_from_series(s: pd.Series, start: str, end: str) -> pd.Series:
    s = s.groupby(s.index.normalize()).mean()
    return s.reindex(pd.date_range(start, end, freq="D"))

# ---------------- RC parsing / eval ----------------
def rc_kind(rc_str: str) -> str:
    s = (rc_str or "").strip()
    has_q = re.search(r"\bQ\b", s) is not None
    has_h = re.search(r"\bH\b", s) is not None or re.search(r"\bh\b", s) is not None
    if has_h and not has_q:
        return "H"
    if has_q and not has_h:
        return "Q"
    if has_h and has_q:
        return "HQ"
    return "unknown"

def infer_corr_type(rc_str: str) -> str:
    s = (rc_str or "").lower()
    has_h = "h" in s
    has_q = "q" in s
    if has_h and not has_q:
        return "Q-H"
    if has_q and not has_h:
        return "Q-Q"
    if has_h and has_q:
        return "Q-H"
    return "--"

def is_valid_rc(s: str) -> bool:
    f = (s or "").strip().lower()
    return bool(f) and f not in ("na", "linear interpolation")

def eval_rc(rc_str, H, Q):
    """
    Evaluate RC expression using vectors H and Q.
    Falls back to scalar loop if vector eval fails.
    """
    try:
        y = eval(rc_str, {"np": np}, {"H": H, "h": H, "Q": Q})
        y = np.array(y, dtype=float)
    except Exception:
        out = []
        for h, q in zip(H, Q):
            try:
                out.append(float(eval(rc_str, {"np": np}, {"H": float(h), "h": float(h), "Q": float(q)})))
            except Exception:
                out.append(np.nan)
        y = np.array(out, dtype=float)
    y[~np.isfinite(y)] = np.nan
    return y

def safe_eval_rc_scalar(rc_str: str, h_val: float, q_val: float) -> float:
    try:
        return float(eval(str(rc_str), {"__builtins__": {}}, {"np": np, "H": float(h_val), "h": float(h_val), "Q": float(q_val)}))
    except Exception:
        return np.nan

# ---------------- stats ----------------
def compute_extended_stats(obs, pred):
    mask = np.isfinite(obs) & np.isfinite(pred)
    o, p = np.asarray(obs)[mask], np.asarray(pred)[mask]
    N = len(o)
    R = np.corrcoef(o, p)[0, 1] if N >= 3 else np.nan
    R2 = R**2 if np.isfinite(R) else np.nan
    RMSE = np.sqrt(np.mean((p - o) ** 2)) if N > 0 else np.nan
    denom = np.mean(np.abs(o)) if N > 0 else np.nan
    RMSE_pct = 100 * RMSE / denom if denom else np.nan
    Bias_pct = 100 * np.mean(p - o) / denom if denom else np.nan
    ss_res = np.sum((o - p) ** 2)
    ss_tot = np.sum((o - np.mean(o)) ** 2)
    NSE = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
    return {
        "N": N,
        "R": R,
        "R2": R2,
        "RMSE_pct": RMSE_pct,
        "NSE": NSE,
        "Bias_pct": Bias_pct
    }

def pearson_r(a: pd.Series, b: pd.Series, start: str, end: str):
    a = a.loc[start:end]
    b = b.loc[start:end]
    m = a.notna() & b.notna()
    if m.sum() < 3:
        return np.nan
    return float(np.corrcoef(a[m].to_numpy(), b[m].to_numpy())[0, 1])

# ---------------- series builders used in evaluation ----------------
def originals_daily_Q(df: pd.DataFrame, full_start: str, full_end: str) -> pd.Series:
    df = as_idx(df)
    method = df.get("Discharge_filled_method", pd.Series(index=df.index, dtype=object)).astype(str).str.strip().str.lower()
    original = (method.eq("") | method.eq("original")) & df.get("Discharge").notna()
    s = pd.to_numeric(df.loc[original, "Discharge"], errors="coerce")
    s = s.groupby(s.index.normalize()).mean()
    return s.reindex(pd.date_range(full_start, full_end, freq="D"))

def donor_daily_series(df: pd.DataFrame, kind: str, full_start: str, full_end: str) -> pd.Series:
    df = as_idx(df)
    idx = pd.date_range(full_start, full_end, freq="D")
    if kind == "H":
        if "Stage" not in df.columns:
            return pd.Series(index=idx, dtype=float)
        h = pd.to_numeric(df["Stage"], errors="coerce").groupby(df.index.normalize()).mean()
        return h.reindex(idx)
    else:
        if "Discharge" not in df.columns:
            return pd.Series(index=idx, dtype=float)
        method = df.get("Discharge_filled_method", pd.Series(index=df.index, dtype=object)).astype(str).str.strip().str.lower()
        original = (method.eq("") | method.eq("original")) & df["Discharge"].notna()
        qd = pd.to_numeric(df.loc[original, "Discharge"], errors="coerce")
        qd = qd.groupby(qd.index.normalize()).mean()
        return qd.reindex(idx)

# ---------------- gap filling ----------------
def find_gaps(is_na: np.ndarray):
    n = len(is_na)
    i = 0
    gaps = []
    while i < n:
        if is_na[i]:
            s = i
            while i < n and is_na[i]:
                i += 1
            gaps.append((s, i))
        else:
            i += 1
    return gaps

def fill_long_gaps_with_rc(y: pd.Series, H: pd.Series, Qd: pd.Series, rc_str: str, short_gap_days=3, tag_name="Rating Curve"):
    """
    Fill only long gaps (> short_gap_days) using RC.
    RC may use H/h and/or Q.
    """
    y = y.copy()
    tags = pd.Series("", index=y.index, dtype=object)

    H_i = H.interpolate(limit=short_gap_days, limit_direction="both", limit_area="inside")
    Q_i = Qd.interpolate(limit=short_gap_days, limit_direction="both", limit_area="inside")

    needs_h = "h" in rc_str.lower()
    needs_q = "q" in rc_str.lower()

    for s, e in find_gaps(y.isna().to_numpy()):
        if (e - s) <= short_gap_days:
            continue
        for i in range(s, e):
            if pd.isna(y.iat[i]):
                h = H_i.iat[i]
                q = Q_i.iat[i]
                if needs_h and pd.isna(h):
                    continue
                if needs_q and pd.isna(q):
                    continue
                yh = safe_eval_rc_scalar(rc_str, h, q)
                if np.isfinite(yh):
                    y.iat[i] = yh
                    tags.iat[i] = tag_name
    return y, tags

def fill_short_then_long_interp(y: pd.Series, tags: pd.Series, short_gap_days=3):
    y = y.copy()
    tags = tags.copy()

    y1 = y.interpolate(method="linear", limit=short_gap_days, limit_area="inside")
    m_short = y.isna() & y1.notna()
    y[m_short] = y1[m_short]
    tags.loc[m_short] = "Interpolated"

    y2 = y.interpolate(method="linear", limit_area="inside", limit_direction="both")
    m_long = y.isna() & y2.notna()
    y[m_long] = y2[m_long]
    tags.loc[m_long] = "Long Gap Interp"

    tags.loc[y.isna()] = "Unknown"
    return y, tags