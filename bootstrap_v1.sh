#!/usr/bin/env bash
set -e

mkdir -p src outputs

cat > requirements.txt << 'REQ'
pandas>=2.0.0
python-dateutil>=2.8.2
rapidfuzz>=3.0.0
REQ

cat > src/config.py << 'PY'
from dataclasses import dataclass

@dataclass(frozen=True)
class ReconConfig:
    ledger_path: str = "data/raw/ledger.csv"
    subledger_path: str = "data/raw/subledger.csv"
    outputs_dir: str = "outputs"

    date_window_days: int = 2          # allowed date difference for matching
    amount_tolerance: float = 0.50     # allowed amount difference for tolerance match
    min_similarity: int = 85           # 0-100 RapidFuzz threshold
PY

cat > src/utils.py << 'PY'
import re
import pandas as pd

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9\\s]", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s

def coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date

def coerce_amount(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")
PY

cat > src/ingest.py << 'PY'
import pandas as pd

REQUIRED_COLS = ["date", "amount", "description"]

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path}: {missing}")
    return df
PY

cat > src/standardize.py << 'PY'
import pandas as pd
from utils import normalize_text, coerce_date, coerce_amount

def standardize(df: pd.DataFrame, source: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()

    out["date"] = coerce_date(out["date"])
    out["amount"] = coerce_amount(out["amount"])
    out["description_norm"] = out["description"].apply(normalize_text)

    out["source"] = source

    bad_mask = out["date"].isna() | out["amount"].isna() | (out["description_norm"] == "")
    exceptions = out.loc[bad_mask].copy()
    exceptions["exception_reason"] = ""
    exceptions.loc[out["date"].isna(), "exception_reason"] += "bad_date;"
    exceptions.loc[out["amount"].isna(), "exception_reason"] += "bad_amount;"
    exceptions.loc[out["description_norm"] == "", "exception_reason"] += "bad_description;"

    clean = out.loc[~bad_mask].copy()

    clean["row_id"] = range(1, len(clean) + 1)
    exceptions["row_id"] = range(1, len(exceptions) + 1)

    return clean, exceptions
PY

cat > src/match.py << 'PY'
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
PY

cat > src/report.py << 'PY'
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
PY

cat > src/run_agent.py << 'PY'
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
PY

echo "Bootstrap complete."
