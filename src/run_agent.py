from config import ReconConfig
from ingest import load_csv
from standardize import standardize
from match import exact_match, weighted_key_match, tolerance_match
from report import write_outputs
from rules import load_rules
from suggest import build_suggestions
import pandas as pd
import os

def main() -> None:
    cfg = ReconConfig()
    rules = load_rules()

    ledger_raw = load_csv(cfg.ledger_path, source='ledger')
    sub_raw = load_csv(cfg.subledger_path, source='subledger')

    ledger, ex_l = standardize(ledger_raw, "ledger")
    sub, ex_s = standardize(sub_raw, "subledger")
    exceptions = pd.concat([ex_l, ex_s], ignore_index=True)

    # Pass 1: exact
    em = exact_match(ledger, sub, rules.date_window_days)

    matched_ledger_ids = set(em["row_id_l"].tolist()) if len(em) else set()
    matched_sub_ids = set(em["row_id_s"].tolist()) if len(em) else set()

    ul = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    us = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    # Pass 2: weighted business keys (deterministic scoring)
    wk = weighted_key_match(
        ul, us,
        date_window_days=rules.date_window_days,
        amount_tolerance=rules.amount_tolerance,
        min_similarity=rules.min_similarity,
        field_weights=rules.field_weights
    )

    if len(wk):
        matched_ledger_ids |= set(wk["row_id_l"].tolist())
        matched_sub_ids |= set(wk["row_id_s"].tolist())

    ul2 = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    us2 = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    # Pass 3: tolerance
    tm = tolerance_match(ul2, us2, rules.date_window_days, rules.amount_tolerance, rules.min_similarity)
    if len(tm):
        matched_ledger_ids |= set(tm["row_id_l"].tolist())
        matched_sub_ids |= set(tm["row_id_s"].tolist())

    matched = pd.concat([em, wk, tm], ignore_index=True) if len(wk) or len(tm) else em

    unmatched_ledger = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    unmatched_subledger = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

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
    print("Match breakdown:", matched["match_type"].value_counts().to_dict() if len(matched) else {})

if __name__ == "__main__":
    main()
