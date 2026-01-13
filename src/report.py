import os
import json
import pandas as pd

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_outputs(outputs_dir: str,
                  matched: pd.DataFrame,
                  unmatched_ledger: pd.DataFrame,
                  unmatched_sub: pd.DataFrame,
                  exceptions: pd.DataFrame) -> None:
    ensure_dir(outputs_dir)

    matched.to_csv(os.path.join(outputs_dir, "matched.csv"), index=False)
    unmatched_ledger.to_csv(os.path.join(outputs_dir, "unmatched_ledger.csv"), index=False)
    unmatched_sub.to_csv(os.path.join(outputs_dir, "unmatched_subledger.csv"), index=False)
    exceptions.to_csv(os.path.join(outputs_dir, "exceptions.csv"), index=False)

    summary = {
        "matched_rows": int(len(matched)),
        "unmatched_ledger_rows": int(len(unmatched_ledger)),
        "unmatched_subledger_rows": int(len(unmatched_sub)),
        "exceptions_rows": int(len(exceptions)),
        "match_breakdown": matched["match_type"].value_counts().to_dict() if len(matched) else {},
    }

    with open(os.path.join(outputs_dir, "recon_summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
