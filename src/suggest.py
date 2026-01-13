import pandas as pd
from rapidfuzz import fuzz

def _has_col(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns

def build_suggestions(unmatched_ledger: pd.DataFrame,
                      unmatched_sub: pd.DataFrame,
                      date_window_days: int,
                      amount_tolerance: float,
                      min_similarity: int,
                      top_k: int,
                      field_weights: dict) -> pd.DataFrame:
    if unmatched_ledger.empty or unmatched_sub.empty:
        return pd.DataFrame()

    rows = []
    sub = unmatched_sub.copy()

    # precompute for speed
    sub["date_dt"] = pd.to_datetime(sub["date"], errors="coerce")
    led = unmatched_ledger.copy()
    led["date_dt"] = pd.to_datetime(led["date"], errors="coerce")

    for _, l in led.iterrows():
        candidates = sub.copy()

        # date window filter
        candidates["date_diff_days"] = (candidates["date_dt"] - l["date_dt"]).abs().dt.days
        candidates = candidates.loc[candidates["date_diff_days"] <= date_window_days].copy()
        if candidates.empty:
            continue

        # amount tolerance filter
        candidates["amount_diff"] = (candidates["amount"] - l["amount"]).abs()
        candidates = candidates.loc[candidates["amount_diff"] <= amount_tolerance].copy()
        if candidates.empty:
            continue

        # text similarity
        candidates["similarity"] = candidates["description_norm"].apply(
            lambda s: fuzz.token_set_ratio(l["description_norm"], s)
        )
        candidates = candidates.loc[candidates["similarity"] >= min_similarity].copy()
        if candidates.empty:
            continue

        # business-key boosts (only if columns exist)
        boost = 0

        def add_boost(col: str, w: int) -> pd.Series:
            if col not in candidates.columns or col not in led.columns:
                return pd.Series([0] * len(candidates), index=candidates.index)
            return (candidates[col].astype(str).fillna("") == str(l.get(col, "")).strip()).astype(int) * w

        boosts = pd.Series([0] * len(candidates), index=candidates.index)

        for col, w in field_weights.items():
            boosts = boosts + add_boost(col, int(w))

        candidates["boost"] = boosts
        candidates["score"] = candidates["similarity"] + candidates["boost"]

        # pick top K
        candidates = candidates.sort_values(["score", "similarity", "amount_diff", "date_diff_days"],
                                            ascending=[False, False, True, True]).head(top_k)

        rank = 1
        for _, c in candidates.iterrows():
            reason_parts = [
                f"sim={int(c['similarity'])}",
                f"amt_diff={float(c['amount_diff']):.2f}",
                f"date_diff={int(c['date_diff_days'])}d",
            ]
            if int(c.get("boost", 0)) > 0:
                reason_parts.append(f"boost={int(c['boost'])}")

            rows.append({
                "ledger_row_id": l["row_id"],
                "subledger_row_id": c["row_id"],
                "rank": rank,
                "score": float(c["score"]),
                "similarity": int(c["similarity"]),
                "amount": float(l["amount"]),
                "amount_diff": float(c["amount_diff"]),
                "ledger_date": l["date"],
                "subledger_date": c["date"],
                "ledger_description": l.get("description", ""),
                "subledger_description": c.get("description", ""),
                "reason": "; ".join(reason_parts)
            })
            rank += 1

    return pd.DataFrame(rows)
