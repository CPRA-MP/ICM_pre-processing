import pandas as pd
import numpy as np
import re


def fetch_usgs_data_dataretrieval(site_no, start_date=None, end_date=None, parameter_codes=None):
    """
    USGS fetch:
    - Always tries daily means (DV) for full date range.
    - For missing dates/values, fills with instantaneous (IV) daily means.
    - Sanitizes known USGS sentinel values before unit conversion and IV aggregation.
    - Also sanitizes known post-conversion sentinel magnitudes after merging (belt-and-suspenders).
    Returns: DataFrame with columns for Date, Discharge, Stage (m), Station, Discharge_source, Stage_source.
    Units:
      - Discharge converted to m3/s
      - Stage converted to meters
    """
    from dataretrieval import nwis

    # Known raw sentinel magnitudes used by some feeds to indicate missing/error
    RAW_SENTINELS = {
        -1000000, -999999, -99999, -9999,
         1000000,  999999,  99999,  9999,
    }
    # Known converted sentinel magnitudes (after unit conversion), used to scrub any that slip through
    # Discharge: cfs -> m3/s; common factors include rounded, exact, and observed variants
    Q_CONVERTED_SENTINELS = [
        -1000000 * 0.0283168,
        -1000000 * 0.028316846592,
        -1000000 * 0.02831677168,
    ]
    # Stage: ft -> m
    H_CONVERTED_SENTINELS = [
        -1000000 * 0.3048,
    ]
    # Tolerances for matching converted sentinel magnitudes
    Q_SENTINEL_TOL = 0.1   # m3/s
    H_SENTINEL_TOL = 0.1   # m

    def _sanitize_raw_usgs(s: pd.Series) -> pd.Series:
        """Coerce numeric and replace raw sentinel constants with NaN."""
        x = pd.to_numeric(s, errors="coerce")
        return x.mask(x.isin(RAW_SENTINELS))

    def _sanitize_near_targets(s: pd.Series, targets: list[float], tol: float) -> pd.Series:
        """Replace values within ±tol of any target with NaN."""
        x = pd.to_numeric(s, errors="coerce")
        if x.empty:
            return x
        mask = pd.Series(False, index=x.index)
        for t in targets:
            mask |= (x.sub(t).abs() <= tol)
        return x.mask(mask)

    parameter_codes = parameter_codes or ["00060", "00065"]
    print(f"Reading in station {site_no} ...")

    # --- 1. Fetch daily values (DV) ---
    try:
        df_dv, meta_dv = nwis.get_dv(site=site_no, parameterCd=parameter_codes, start=start_date, end=end_date)
        df_dv = df_dv.reset_index().rename(columns={"datetime": "Date"})
        discharge_col_dv = next((c for c in df_dv.columns if c.startswith("00060") and "_Mean" in c), None)
        stage_col_dv     = next((c for c in df_dv.columns if c.startswith("00065") and "_Mean" in c), None)
        df_dv["Date"] = pd.to_datetime(df_dv["Date"]).dt.tz_localize(None)

        # Sanitize before unit conversion
        if discharge_col_dv:
            q_raw = _sanitize_raw_usgs(df_dv[discharge_col_dv])
            df_dv["Discharge"] = q_raw * 0.0283168  # cfs -> m3/s
        else:
            df_dv["Discharge"] = np.nan

        if stage_col_dv:
            h_raw = _sanitize_raw_usgs(df_dv[stage_col_dv])
            df_dv["Stage"] = h_raw * 0.3048  # ft -> m
        else:
            df_dv["Stage"] = np.nan

        df_dv["Discharge_source"] = "daily"
        df_dv["Stage_source"] = "daily"
        df_dv["Station"] = site_no
    except Exception as e:
        print(f"  -> ERROR fetching daily values for {site_no}: {e}")
        df_dv = pd.DataFrame(columns=["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"])

    # --- 2. Identify missing dates ---
    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    if not df_dv.empty and "Date" in df_dv.columns:
        dv_dates = pd.to_datetime(df_dv["Date"]).dt.tz_localize(None)
        missing_dates = all_dates.difference(dv_dates)
        na_dates_series = pd.Series(pd.to_datetime(df_dv.loc[df_dv["Discharge"].isna() | df_dv["Stage"].isna(), "Date"]).dt.tz_localize(None))
        all_missing_dates = pd.Index(pd.concat([pd.Series(missing_dates), na_dates_series]).drop_duplicates().sort_values())
    else:
        all_missing_dates = all_dates

    # --- 3. Fetch instantaneous values (IV) for missing dates, aggregate to daily mean ---
    if len(all_missing_dates) > 0:
        print(f"  -> Fetching instantaneous data for {len(all_missing_dates)} missing days...")
        try:
            df_iv, meta_iv = nwis.get_iv(
                site=site_no,
                parameterCd=parameter_codes,
                start=all_missing_dates.min().strftime("%Y-%m-%d"),
                end=all_missing_dates.max().strftime("%Y-%m-%d")
            )
            if not df_iv.empty:
                df_iv = df_iv.reset_index()
                df_iv["Date"] = pd.to_datetime(df_iv["datetime"]).dt.tz_localize(None).dt.normalize()
                discharge_col_iv = next((c for c in df_iv.columns if c.startswith("00060")), None)
                stage_col_iv     = next((c for c in df_iv.columns if c.startswith("00065")), None)

                # Sanitize IV before aggregating so sentinels don't pollute means
                if discharge_col_iv:
                    df_iv[discharge_col_iv] = _sanitize_raw_usgs(df_iv[discharge_col_iv])
                if stage_col_iv:
                    df_iv[stage_col_iv] = _sanitize_raw_usgs(df_iv[stage_col_iv])

                agg_dict = {}
                if discharge_col_iv:
                    agg_dict[discharge_col_iv] = "mean"
                if stage_col_iv:
                    agg_dict[stage_col_iv] = "mean"
                df_iv_daily = df_iv.groupby("Date").agg(agg_dict).reset_index() if agg_dict else pd.DataFrame(columns=["Date"])

                # Convert after aggregation
                if discharge_col_iv and discharge_col_iv in df_iv_daily.columns:
                    df_iv_daily["Discharge"] = pd.to_numeric(df_iv_daily[discharge_col_iv], errors="coerce") * 0.0283168
                else:
                    df_iv_daily["Discharge"] = np.nan
                if stage_col_iv and stage_col_iv in df_iv_daily.columns:
                    df_iv_daily["Stage"] = pd.to_numeric(df_iv_daily[stage_col_iv], errors="coerce") * 0.3048
                else:
                    df_iv_daily["Stage"] = np.nan

                df_iv_daily["Discharge_source"] = "inst"
                df_iv_daily["Stage_source"] = "inst"
                df_iv_daily["Station"] = site_no
            else:
                df_iv_daily = pd.DataFrame(columns=["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"])
        except Exception as e:
            print(f"  -> ERROR fetching instantaneous values for {site_no}: {e}")
            df_iv_daily = pd.DataFrame(columns=["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"])
    else:
        df_iv_daily = pd.DataFrame(columns=["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"])

    # --- 4. Merge daily and IV (skip empty frames) ---
    frames = []
    if not df_dv.empty:
        frames.append(df_dv)
    if not df_iv_daily.empty:
        frames.append(df_iv_daily)

    if frames:
        df_out = pd.concat(frames, ignore_index=True)
        df_out = df_out.sort_values("Date").drop_duplicates(subset="Date", keep="first")
    else:
        df_out = pd.DataFrame(columns=["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"])

    # --- 5. Finalize columns and perform post-conversion sentinel scrubbing ---
    df_out = df_out[["Date", "Discharge", "Stage", "Station", "Discharge_source", "Stage_source"]]

    # Scrub any converted sentinel magnitudes that may have slipped through
    if "Discharge" in df_out.columns:
        df_out["Discharge"] = _sanitize_near_targets(df_out["Discharge"], Q_CONVERTED_SENTINELS, Q_SENTINEL_TOL)
    if "Stage" in df_out.columns:
        df_out["Stage"] = _sanitize_near_targets(df_out["Stage"], H_CONVERTED_SENTINELS, H_SENTINEL_TOL)

    return df_out


def reindex_to_full_range(df, start_date, end_date):
    """
    Reindex DataFrame to a full daily date range between start_date and end_date.
    Returns a frame with Date index named 'Date'.
    """
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        df = df.set_index("Date")
    full_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = df.reindex(full_range)
    df.index.name = "Date"
    return df


def find_gaps(is_na):
    """
    Find consecutive gaps in a boolean array indicating missing values.
    Returns: two lists (gap_starts, gap_ends) where each gap is [start, end)
    """
    gap_starts, gap_ends = [], []
    n = len(is_na)
    i = 0
    while i < n:
        if is_na[i]:
            start = i
            while i < n and is_na[i]:
                i += 1
            end = i
            gap_starts.append(start)
            gap_ends.append(end)
        else:
            i += 1
    return gap_starts, gap_ends


def fill_short_gaps(df, discharge_col="Discharge", short_gap_days=3):
    """
    Fill short gaps (<= short_gap_days) in the discharge column by interpolation.
    - Operates on a copy with reset_index() so Date becomes a column if it was the index.
    - Labels filled rows with 'interpolated'.
    """
    df = df.reset_index()
    if "Discharge_filled_method" not in df.columns:
        df["Discharge_filled_method"] = ""
    is_na = df[discharge_col].isna().values
    gap_starts, gap_ends = find_gaps(is_na)
    interp_mask = np.zeros(len(df), dtype=bool)
    for start, end in zip(gap_starts, gap_ends):
        gap_len = end - start
        if gap_len <= short_gap_days:
            interp_mask[start:end] = True
    interp_series = df[discharge_col].interpolate(limit_area='inside', limit_direction='both')
    apply_mask = interp_mask & df[discharge_col].isna()
    df.loc[apply_mask, discharge_col] = interp_series[apply_mask]
    df.loc[apply_mask, "Discharge_filled_method"] = "interpolated"
    return df


def fill_long_gaps_with_rating_curve(
    df, discharge_col, stage_col, meta, station_name, flow_data, short_gap_days=3
):
    """
    Fill long gaps (> short_gap_days) in the discharge column using a rating curve.

    Rules:
      - Only apply if a donor station is provided in meta["Station ID used for rating curve"] and available.
      - Applies RC only to long gaps (> short_gap_days).
      - Uses donor station for ALL variables present in the RC (Q and h/H).
      - Smooths donor short gaps (<= short_gap_days) in-memory for Q and Stage to avoid RC skips.
      - Labels target rows filled by RC as 'rating_curve_long_gap'.
      - Tracks donor provenance on RC-filled days:
          RC_donor_station, RC_donor_q_source, RC_donor_stage_source ('original' or 'short_interp')
          RC_stage_source is also written for compatibility and equals RC_donor_stage_source.
    """
    # Normalize target df and ensure Date column
    df = df.copy().reset_index()
    if "Date" not in df.columns and "index" in df.columns:
        df = df.rename(columns={"index": "Date"})
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    else:
        raise ValueError("fill_long_gaps_with_rating_curve expects a Date index or Date column")

    rc_str = (meta.get("Rating Curve (Python Format)", "") or "").strip()
    rc_norm = rc_str.lower()

    # Skip if not a usable formula (e.g., 'linear interpolation' or empty)
    if (rc_norm in {"", "none", "na", "n/a"} or "interpol" in rc_norm or not re.search(r"\b(Q|h|H)\b", rc_str)):
        return df

    # REQUIRE a donor station for RC
    ref_station_id = (meta.get("Station ID used for rating curve", "") or "").strip()
    if not ref_station_id:
        return df

    # Load donor data
    ref_df = flow_data.get(ref_station_id)
    if ref_df is None or ref_df.empty:
        return df

    # Normalize donor to Date index
    ref_df = ref_df.copy()
    if "Date" in ref_df.columns:
        ref_df["Date"] = pd.to_datetime(ref_df["Date"]).dt.tz_localize(None).dt.normalize()
        ref_df = ref_df.set_index("Date")
    else:
        ref_df.index = pd.to_datetime(ref_df.index).tz_localize(None).normalize()

    # In-memory short-gap prefill for donor Q and Stage, with provenance
    if "Discharge" in ref_df.columns:
        q = pd.to_numeric(ref_df["Discharge"], errors="coerce")
        q_prefill = q.interpolate(limit_area="inside", limit_direction="both", limit=short_gap_days)
        ref_df["Discharge_prefill"] = q_prefill
        ref_df["Donor_Q_source"] = np.where(q.notna(), "original",
                                     np.where(q_prefill.notna(), "short_interp", None))
    if "Stage" in ref_df.columns:
        h = pd.to_numeric(ref_df["Stage"], errors="coerce")
        h_prefill = h.interpolate(limit_area="inside", limit_direction="both", limit=short_gap_days)
        ref_df["Stage_prefill"] = h_prefill
        ref_df["Donor_H_source"] = np.where(h.notna(), "original",
                                     np.where(h_prefill.notna(), "short_interp", None))

    # Ensure method/provenance columns on target
    for col in ["Discharge_filled_method", "RC_donor_station", "RC_donor_q_source",
                "RC_donor_stage_source", "RC_stage_source"]:
        if col not in df.columns:
            df[col] = ""

    # Identify long gaps in target
    is_na = df[discharge_col].isna().values
    gap_starts, gap_ends = find_gaps(is_na)
    needed = set(re.findall(r"\b(Q|h|H)\b", rc_str))

    # If RC needs H but donor lacks Stage, skip RC
    if (("h" in needed) or ("H" in needed)) and "Stage_prefill" not in ref_df.columns:
        return df
    # If RC needs Q but donor lacks Discharge, skip RC
    if ("Q" in needed) and "Discharge_prefill" not in ref_df.columns:
        return df

    for start, end in zip(gap_starts, gap_ends):
        if end - start <= short_gap_days:
            continue  # RC only for long gaps

        for idx in range(start, end):
            date = df.loc[idx, "Date"]  # correct date lookup

            eval_kwargs = {}
            donor_q_src_val = ""
            donor_h_src_val = ""

            # Q from donor (with short-gap prefill)
            if "Q" in needed:
                if date not in ref_df.index:
                    continue
                q_ref = ref_df.at[date, "Discharge_prefill"]
                if pd.notna(q_ref):
                    eval_kwargs["Q"] = float(q_ref)
                    donor_q_src_val = (ref_df.at[date, "Donor_Q_source"] or "") if "Donor_Q_source" in ref_df.columns else ""
                else:
                    continue

            # h/H from donor (with short-gap prefill)
            if "h" in needed or "H" in needed:
                if date not in ref_df.index:
                    continue
                h_ref = ref_df.at[date, "Stage_prefill"]
                if pd.notna(h_ref):
                    eval_kwargs["h"] = float(h_ref)
                    eval_kwargs["H"] = float(h_ref)
                    donor_h_src_val = (ref_df.at[date, "Donor_H_source"] or "") if "Donor_H_source" in ref_df.columns else ""
                else:
                    continue

            # Evaluate RC
            try:
                discharge_val = eval(rc_str, {"np": np}, eval_kwargs)
                df.loc[idx, discharge_col] = discharge_val
                df.loc[idx, "Discharge_filled_method"] = "rating_curve_long_gap"
                df.loc[idx, "RC_donor_station"] = ref_station_id
                df.loc[idx, "RC_donor_q_source"] = donor_q_src_val
                df.loc[idx, "RC_donor_stage_source"] = donor_h_src_val
                df.loc[idx, "RC_stage_source"] = donor_h_src_val  # for compatibility with any plotting
            except Exception as e:
                print(
                    f"Rating curve failed for {station_name} on {getattr(date, 'date', lambda: 'NA')()}: {e}\n"
                    f"  rc_str={rc_str}\n  eval_kwargs={eval_kwargs}"
                )

    return df


def fill_remaining_long_gaps_with_interp(df, discharge_col="Discharge", short_gap_days=3):
    """
    Fill remaining long gaps (> short_gap_days) in the discharge column by interpolation.
    Labels with 'interpolated_long_gap'.
    """
    df = df.reset_index()
    is_na = df[discharge_col].isna().values
    gap_starts, gap_ends = find_gaps(is_na)
    interp_series = df[discharge_col].interpolate(limit_area='inside', limit_direction='both')
    if "Discharge_filled_method" not in df.columns:
        df["Discharge_filled_method"] = ""
    for start, end in zip(gap_starts, gap_ends):
        if end - start > short_gap_days:
            for idx in range(start, end):
                if pd.isna(df.loc[idx, discharge_col]) and pd.notna(interp_series[idx]):
                    df.loc[idx, discharge_col] = interp_series[idx]
                    df.loc[idx, "Discharge_filled_method"] = "interpolated_long_gap"
    return df


def label_unfillable(df, discharge_col="Discharge"):
    """
    Label entries in the discharge column that could not be filled.
    """
    if "Discharge_filled_method" not in df.columns:
        df["Discharge_filled_method"] = ""
    mask = df[discharge_col].isna() & (df["Discharge_filled_method"] == "")
    df.loc[mask, "Discharge_filled_method"] = "unfillable_long_missing"
    return df


def gap_filling_pipeline_with_metadata(
    df,
    meta,
    station_name,
    flow_data,
    discharge_col="Discharge",
    stage_col="Stage",
    short_gap_days=3
):
    """
    Gap-filling order:
      1) Rating curve for long gaps (> short_gap_days) using donor's Q and donor's Stage (if RC needs H)
      2) Interpolate remaining short gaps (<= short_gap_days) at the target
      3) Interpolate remaining long gaps (> short_gap_days) at the target
      4) Label unfillable
    """
    # 1) Use rating curve only on long gaps
    df = fill_long_gaps_with_rating_curve(
        df, discharge_col, stage_col, meta, station_name, flow_data, short_gap_days=short_gap_days
    )

    # 2) Interpolate any short gaps left
    df = fill_short_gaps(df, discharge_col=discharge_col, short_gap_days=short_gap_days)

    # 3) Interpolate any remaining long gaps
    df = fill_remaining_long_gaps_with_interp(df, discharge_col=discharge_col, short_gap_days=short_gap_days)

    # 4) Label still-missing values
    df = label_unfillable(df, discharge_col=discharge_col)

    return df

def clean_name(name):
    """Return a filename-safe version of a station name."""
    return str(name).replace(" ", "_").replace(",", "").replace("/", "_")

def save_to_csv(df, filepath):
    # Processes on copy of DataFrame
    df_out = df.copy()

    # Remove extraneous index columns, if present
    for extra_col in ["level_0", "index"]:
        if extra_col in df_out.columns:
            df_out = df_out.drop(columns=[extra_col])

    # Rename for output only
    if "Discharge" in df_out.columns:
        df_out = df_out.rename(columns={"Discharge": "Discharge (m3/s)"})

    # Keep only specific comments, only if they exist
    cols_to_save = ["Date", "Discharge (m3/s)", "Discharge_filled_method"]
    df_out = df_out.loc[:, [c for c in cols_to_save if c in df_out.columns]]

    # Print to confirm columns being saved
    print("Saving columns:", df_out.columns.tolist(), "to", filepath)
    df_out.to_csv(filepath, index=False, encoding="utf-8")


def plot_filling_methods(
    df,
    station_name,
    station_id,
    discharge_col="Discharge",
    method_col="Discharge_filled_method",
    date_col="Date",
    figsize=(16, 6),
    outdir=None,
    dpi=300,
    fmt="png",
):
    """
    Plot discharge values colored by filling method and save to disk.
    """
    import os
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    df = df.copy()

    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    else:
        if df.index.name == date_col or date_col in getattr(df.index, "names", []):
            df = df.reset_index()
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        else:
            raise KeyError(f"'{date_col}' not found in columns or index of DataFrame")

    df = df.dropna(subset=[date_col])

    if outdir is None:
        outdir = "plots"

    os.makedirs(outdir, exist_ok=True)

    safe_station = (
        str(station_name)
        .replace(" ", "_")
        .replace(",", "")
        .replace("/", "_")
    )

    fname = f"{safe_station}_{station_id}_discharge.{fmt}"
    save_path = os.path.join(outdir, fname)

    method_info = {
        "original": {"color": "black", "label": "Original"},
        "interpolated": {"color": "deepskyblue", "label": "Interpolated"},
        "rating_curve_long_gap": {"color": "orange", "label": "Rating Curve"},
        "interpolated_long_gap": {"color": "purple", "label": "Long Gap Interp"},
        "unfillable_long_missing": {"color": "red", "label": "Unfillable"},
    }

    fig, ax = plt.subplots(figsize=figsize)
    handles = []
    labels = []

    for method, info in method_info.items():
        mask = df[method_col].astype(str) == method
        if not mask.any():
            continue

        if method == "unfillable_long_missing":
            sc = ax.scatter(
                df.loc[mask, date_col],
                np.zeros(int(mask.sum())),
                color=info["color"],
                label=info["label"],
                s=40,
                alpha=0.9,
                marker="x",
                zorder=5,
            )
        else:
            sc = ax.scatter(
                df.loc[mask, date_col],
                df.loc[mask, discharge_col],
                color=info["color"],
                label=info["label"],
                s=20,
                alpha=0.9,
            )

        handles.append(sc)
        labels.append(info["label"])

    ax.set_xlabel("Date")
    ax.set_ylabel("Discharge (m3/s)")
    ax.set_title(f"{station_name} ({station_id})")

    if handles:
        ax.legend(handles, labels, loc="upper right", markerscale=2, fontsize=14)

    fig.tight_layout()
    fig.savefig(save_path, dpi=dpi)
    plt.close(fig)

    print(f"Plot saved to {save_path}")
    return save_path

    
    