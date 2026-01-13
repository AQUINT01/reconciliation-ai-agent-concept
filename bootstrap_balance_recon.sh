#!/usr/bin/env bash
set -e

mkdir -p src config data/raw outputs

# Balance recon config: maps your two files to shared standardized fields
cat > config/balance_column_map.json << 'JSON'
{
  "left": {
    "path": "data/raw/left.csv",
    "id": ["Edmunds Grant Account Id"],
    "name": ["Grant account name *"],
    "adopted_budget": ["Adopted grant amount *"],
    "balance_appropriation": ["Current appropriation balance to date *"],
    "balance_revenue": ["Current revenue balance to date *"]
  },
  "right": {
    "path": "data/raw/right.csv",
    "id": ["Edmunds Id"],
    "name": ["Grant Name", "Edmunds.Description"],
    "adopted_budget": ["Edmunds.Adopted Budget"],
    "amended_budget": ["Edmunds.Amended Budget"],
    "encumbered": ["Edmunds.Encumbered"],
    "expended_ytd": ["Edmunds.Expended YTD"],
    "balance": ["Edmunds.Balance"]
  }
}
JSON

cat > src/balance_mapping.py << 'PY'
import json
import pandas as pd
from typing import Dict, List, Optional

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _norm(cand)
        if key in cols:
            return cols[key]
    return None

def load_balance_map(path: str = "config/balance_column_map.json") -> dict:
    with open(path, "r") as f:
        return json.load(f)

def apply_balance_mapping(df: pd.DataFrame, side: str, cfg: Dict) -> pd.DataFrame:
    side_cfg = cfg.get(side)
    if not side_cfg:
        raise ValueError(f"Missing side '{side}' in balance_column_map.json")

    out = pd.DataFrame()

    # required
    id_col = _find_column(df, side_cfg.get("id", []))
    if not id_col:
        raise ValueError(f"Missing required ID column for side '{side}'. Check mapping config.")
    out["id"] = df[id_col].astype(str).str.strip()

    name_col = _find_column(df, side_cfg.get("name", []))
    out["name"] = df[name_col] if name_col else ""

    # optional numeric metrics
    metric_keys = [k for k in side_cfg.keys() if k not in ("path", "id", "name")]
    for mk in metric_keys:
        src = _find_column(df, side_cfg.get(mk, []))
        if src:
            out[mk] = pd.to_numeric(df[src], errors="coerce")
        else:
            out[mk] = pd.NA

    return out
PY

cat > src/run_balance_recon.py << 'PY'
import os
import json
import pandas as pd
from balance_mapping import load_balance_map, apply_balance_mapping

def main() -> None:
    cfg = load_balance_map()

    left_path = cfg["left"]["path"]
    right_path = cfg["right"]["path"]

    left_raw = pd.read_csv(left_path)
    right_raw = pd.read_csv(right_path)
    left_raw.columns = [c.strip() for c in left_raw.columns]
    right_raw.columns = [c.strip() for c in right_raw.columns]

    left = apply_balance_mapping(left_raw, "left", cfg).copy()
    right = apply_balance_mapping(right_raw, "right", cfg).copy()

    # Normalize ID casing/spacing
    left["id"] = left["id"].str.upper().str.strip()
    right["id"] = right["id"].str.upper().str.strip()

    # Dedupe on id (keep first non-null-ish row)
    left = left.drop_duplicates(subset=["id"], keep="first")
    right = right.drop_duplicates(subset=["id"], keep="first")

    # Full outer join so we can see missing on either side
    merged = left.merge(right, on="id", how="outer", suffixes=("_left", "_right"), indicator=True)

    # Compute variances for metrics that exist on both sides
    metrics_left = [c.replace("_left", "") for c in merged.columns if c.endswith("_left")]
    metrics_right = [c.replace("_right", "") for c in merged.columns if c.endswith("_right")]
    shared_metrics = sorted(set(metrics_left).intersection(metrics_right))
    shared_metrics = [m for m in shared_metrics if m not in ("name",)]  # name handled separately

    for m in shared_metrics:
        lcol = f"{m}_left"
        rcol = f"{m}_right"
        merged[f"{m}_variance"] = merged[lcol] - merged[rcol]

    # Flags
    merged["status"] = merged["_merge"].map({
        "both": "matched_on_id",
        "left_only": "missing_on_right",
        "right_only": "missing_on_left"
    })

    os.makedirs("outputs", exist_ok=True)
    out_csv = "outputs/balance_variance.csv"
    merged.to_csv(out_csv, index=False)

    summary = {
        "left_rows": int(len(left)),
        "right_rows": int(len(right)),
        "matched_on_id": int((merged["status"] == "matched_on_id").sum()),
        "missing_on_left": int((merged["status"] == "missing_on_left").sum()),
        "missing_on_right": int((merged["status"] == "missing_on_right").sum()),
        "shared_metrics": shared_metrics
    }
    with open("outputs/balance_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("Wrote outputs/balance_variance.csv and outputs/balance_summary.json")
    print(summary)

if __name__ == "__main__":
    main()
PY

echo "Balance recon bootstrap complete."
