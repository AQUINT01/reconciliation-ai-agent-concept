from config import ReconConfig
from ingest import load_csv
from standardize import standardize
from match import exact_match, tolerance_match
from report import write_outputs
import pandas as pd

def main() -> None:
    cfg = ReconConfig()

    ledger_raw = load_csv(cfg.ledger_path)
    sub_raw = load_csv(cfg.subledger_path)

    ledger, ex_l = standardize(ledger_raw, "ledger")
    sub, ex_s = standardize(sub_raw, "subledger")
    exceptions = pd.concat([ex_l, ex_s], ignore_index=True)

    em = exact_match(ledger, sub, cfg.date_window_days)

    matched_ledger_ids = set(em["row_id_l"].tolist()) if len(em) else set()
    matched_sub_ids = set(em["row_id_s"].tolist()) if len(em) else set()

    ul = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    us = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    tm = tolerance_match(ul, us, cfg.date_window_days, cfg.amount_tolerance, cfg.min_similarity)
    if len(tm):
        matched_ledger_ids |= set(tm["row_id_l"].tolist())
        matched_sub_ids |= set(tm["row_id_s"].tolist())

    matched = pd.concat([em, tm], ignore_index=True) if len(tm) else em

    unmatched_ledger = ledger.loc[~ledger["row_id"].isin(matched_ledger_ids)].copy()
    unmatched_subledger = sub.loc[~sub["row_id"].isin(matched_sub_ids)].copy()

    write_outputs(cfg.outputs_dir, matched, unmatched_ledger, unmatched_subledger, exceptions)

    print(f"Wrote outputs to {cfg.outputs_dir}/")
    print(f"Matched: {len(matched)} | Unmatched ledger: {len(unmatched_ledger)} | Unmatched subledger: {len(unmatched_subledger)} | Exceptions: {len(exceptions)}")

if __name__ == "__main__":
    main()
