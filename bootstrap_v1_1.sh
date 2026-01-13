#!/usr/bin/env bash
set -e

mkdir -p config

cat > config/recon_config.json << 'JSON'
{
  "date_window_days": 2,
  "amount_tolerance": 0.50,
  "min_similarity": 85,
  "top_k_suggestions": 3,
  "field_weights": {
    "vendor": 5,
    "dept": 3,
    "gl": 4,
    "app": 8
  }
}
JSON

cat > src/rules.py << 'PY'
import json
from dataclasses import dataclass
from typing import Dict, Any

@dataclass(frozen=True)
class Rules:
    date_window_days: int
    amount_tolerance: float
    min_similarity: int
    top_k_suggestions: int
    field_weights: Dict[str, int]

def load_rules(path: str = "config/recon_config.json") -> Rules:
    with open(path, "r") as f:
        raw: Dict[str, Any] = json.load(f)

    return Rules(
        date_window_days=int(raw.get("date_window_days", 2)),
        amount_tolerance=float(raw.get("amount_tolerance", 0.50)),
        min_similarity=int(raw.get("min_similarity", 85)),
        top_k_suggestions=int(raw.get("top_k_suggestions", 3)),
        field_weights=dict(raw.get("field_weights", {})),
    )
PY

cat > src/suggest.py << 'PY'
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
PY

# Update run_agent.py to load rules and write suggestions.csv
cat > src/run_agent.py << 'PY'
from config import ReconConfig
from ingest import load_csv
from standardize import standardize
from match import exact_match, tolerance_match
from report import write_outputs
from rules import load_rules
from suggest import build_suggestions
import pandas as pd
import os

def main() -> None:
    cfg = ReconConfig()
    rules = load_rules()

    ledger_raw = load_csv(cfg.ledger_path)
    sub_raw = load_csv(cfg.subledger_path)

    ledger, ex_l = standardize(ledger_raw, "ledger")
    sub, ex_s = standardize(sub_raw, "subledger")
    exceptions = pd.concat([ex_l, ex_s], ignore_index=True)

    em = exact_match(ledger, sub, rules.date_window_days)

    matched_ledger_ids = set(em["row_id_l"].tolist()) if len(em) else set()
    matched_sub_ids = set(em["row_id_s"].tolist()) if len(em) else set()

    ul = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    us = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    tm = tolerance_match(ul, us, rules.date_window_days, rules.amount_tolerance, rules.min_similarity)
    if len(tm):
        matched_ledger_ids |= set(tm["row_id_l"].tolist())
        matched_sub_ids |= set(tm["row_id_s"].tolist())

    matched = pd.concat([em, tm], ignore_index=True) if len(tm) else em

    unmatched_ledger = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    unmatched_subledger = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    # suggestions for leftovers (AI-ready artifact)
    suggestions = build_suggestions(
        unmatched_ledger=unmatched_ledger,
        unmatched_sub=unmatched_subledger,
        date_window_days=rules.date_window_days,
        amount_tolerance=rules.amount_tolerance,
        min_similarity=rules.min_similarity,
        top_k=rules.top_k_suggestions,
        field_weights=rules.field_weights
    )

    write_outputs(cfg.outputs_dir, matched, unmatched_ledger, unmatched_subledger, exceptions)

    os.makedirs(cfg.outputs_dir, exist_ok=True)
    suggestions_path = os.path.join(cfg.outputs_dir, "suggestions.csv")
    suggestions.to_csv(suggestions_path, index=False)

    print(f"Wrote outputs to {cfg.outputs_dir}/")
    print(f"Matched: {len(matched)} | Unmatched ledger: {len(unmatched_ledger)} | Unmatched subledger: {len(unmatched_subledger)} | Exceptions: {len(exceptions)}")
    print(f"Suggestions: {len(suggestions)} rows -> {suggestions_path}")

if __name__ == "__main__":
    main()
PY

echo "v1.1 bootstrap complete."
