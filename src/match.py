import pandas as pd
from rapidfuzz import fuzz

def _date_diff_days(a, b) -> int:
    return abs((pd.to_datetime(a) - pd.to_datetime(b)).days)

def exact_match(ledger: pd.DataFrame, sub: pd.DataFrame, date_window_days: int) -> pd.DataFrame:
    merged = ledger.merge(
        sub,
        on=["amount", "description_norm"],
        suffixes=("_l", "_s"),
        how="inner"
    )
    merged["date_diff_days"] = merged.apply(lambda r: _date_diff_days(r["date_l"], r["date_s"]), axis=1)
    merged = merged.loc[merged["date_diff_days"] <= date_window_days].copy()

    merged = merged.sort_values(["row_id_l", "date_diff_days"])
    merged = merged.drop_duplicates(subset=["row_id_l"], keep="first")

    merged["match_type"] = "exact"
    merged["similarity"] = 100
    return merged

def weighted_key_match(unmatched_ledger: pd.DataFrame,
                       unmatched_sub: pd.DataFrame,
                       date_window_days: int,
                       amount_tolerance: float,
                       min_similarity: int,
                       field_weights: dict) -> pd.DataFrame:
    """
    Deterministic-ish pass that uses similarity + business key boosts to pick a single best match.
    Still rules-first: it writes match_type='weighted_keys' with full scoring.
    """
    if unmatched_ledger.empty or unmatched_sub.empty:
        return pd.DataFrame()

    rows = []
    sub = unmatched_sub.copy()
    sub["date_dt"] = pd.to_datetime(sub["date"], errors="coerce")

    led = unmatched_ledger.copy()
    led["date_dt"] = pd.to_datetime(led["date"], errors="coerce")

    for _, l in led.iterrows():
        candidates = sub.copy()

        candidates["date_diff_days"] = (candidates["date_dt"] - l["date_dt"]).abs().dt.days
        candidates = candidates.loc[candidates["date_diff_days"] <= date_window_days].copy()
        if candidates.empty:
            continue

        candidates["amount_diff"] = (candidates["amount"] - l["amount"]).abs()
        candidates = candidates.loc[candidates["amount_diff"] <= amount_tolerance].copy()
        if candidates.empty:
            continue

        candidates["similarity"] = candidates["description_norm"].apply(
            lambda s: fuzz.token_set_ratio(l["description_norm"], s)
        )
        candidates = candidates.loc[candidates["similarity"] >= min_similarity].copy()
        if candidates.empty:
            continue

        # boosts
        boosts = pd.Series([0] * len(candidates), index=candidates.index)

        def add_boost(col: str, w: int) -> pd.Series:
            if col not in candidates.columns or col not in led.columns:
                return pd.Series([0] * len(candidates), index=candidates.index)
            lval = str(l.get(col, "")).strip()
            return (candidates[col].astype(str).fillna("").str.strip() == lval).astype(int) * w

        for col, w in field_weights.items():
            boosts = boosts + add_boost(col, int(w))

        candidates["boost"] = boosts
        candidates["score"] = candidates["similarity"] + candidates["boost"]

        # choose best candidate
        candidates = candidates.sort_values(["score", "similarity", "amount_diff", "date_diff_days"],
                                            ascending=[False, False, True, True])
        best = candidates.iloc[0]

        rows.append({
            "row_id_l": l["row_id"],
            "row_id_s": best["row_id"],
            "date_l": l["date"],
            "date_s": best["date"],
            "amount": l["amount"],
            "description_norm": l["description_norm"],
            "date_diff_days": int(best["date_diff_days"]),
            "amount_diff": float(best["amount_diff"]),
            "similarity": int(best["similarity"]),
            "boost": int(best["boost"]),
            "score": float(best["score"]),
            "match_type": "weighted_keys",
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()

def tolerance_match(unmatched_ledger: pd.DataFrame,
                    unmatched_sub: pd.DataFrame,
                    date_window_days: int,
                    amount_tolerance: float,
                    min_similarity: int) -> pd.DataFrame:
    rows = []
    for _, l in unmatched_ledger.iterrows():
        candidates = unmatched_sub.copy()
        candidates["date_diff_days"] = candidates["date"].apply(lambda d: _date_diff_days(l["date"], d))
        candidates = candidates.loc[candidates["date_diff_days"] <= date_window_days].copy()
        if candidates.empty:
            continue

        candidates["amount_diff"] = (candidates["amount"] - l["amount"]).abs()
        candidates = candidates.loc[candidates["amount_diff"] <= amount_tolerance].copy()
        if candidates.empty:
            continue

        candidates["similarity"] = candidates["description_norm"].apply(
            lambda s: fuzz.token_set_ratio(l["description_norm"], s)
        )
        candidates = candidates.loc[candidates["similarity"] >= min_similarity].copy()
        if candidates.empty:
            continue

        candidates = candidates.sort_values(["similarity", "amount_diff", "date_diff_days"], ascending=[False, True, True])
        best = candidates.iloc[0]

        rows.append({
            "row_id_l": l["row_id"],
            "row_id_s": best["row_id"],
            "date_l": l["date"],
            "date_s": best["date"],
            "amount": l["amount"],
            "description_norm": l["description_norm"],
            "date_diff_days": int(best["date_diff_days"]),
            "amount_diff": float(best["amount_diff"]),
            "similarity": int(best["similarity"]),
            "match_type": "tolerance",
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()
