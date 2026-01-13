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
