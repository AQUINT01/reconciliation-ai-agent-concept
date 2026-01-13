#!/usr/bin/env bash
set -e

mkdir -p config

# Example mapping for two sources. You will edit these keys to match your real exports.
cat > config/column_map.json << 'JSON'
{
  "ledger": {
    "date": ["date", "txn_date", "transaction_date", "post_date"],
    "amount": ["amount", "amt", "transaction_amount"],
    "description": ["description", "memo", "details", "transaction_description"],
    "vendor": ["vendor", "payee", "vendor_name"],
    "dept": ["dept", "department", "department_name"],
    "gl": ["gl", "gl_code", "object_code", "account_code"],
    "app": ["app", "app#", "application", "grant_app", "grant_id"]
  },
  "subledger": {
    "date": ["date", "txn_date", "transaction_date", "post_date"],
    "amount": ["amount", "amt", "transaction_amount"],
    "description": ["description", "memo", "details", "transaction_description"],
    "vendor": ["vendor", "payee", "vendor_name"],
    "dept": ["dept", "department", "department_name"],
    "gl": ["gl", "gl_code", "object_code", "account_code"],
    "app": ["app", "app#", "application", "grant_app", "grant_id"]
  }
}
JSON

cat > src/mapping.py << 'PY'
import json
import pandas as pd
from typing import Dict, List, Optional

REQUIRED = ["date", "amount", "description"]
OPTIONAL = ["vendor", "dept", "gl", "app"]

def _norm(s: str) -> str:
    return str(s).strip().lower()

def load_column_map(path: str = "config/column_map.json") -> dict:
    with open(path, "r") as f:
        return json.load(f)

def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_norm(c): c for c in df.columns}
    for cand in candidates:
        key = _norm(cand)
        if key in cols:
            return cols[key]
    return None

def apply_mapping(df: pd.DataFrame, source: str, column_map: Dict) -> pd.DataFrame:
    """
    Returns a new DF with standardized column names (date, amount, description, vendor, dept, gl, app),
    pulling from the first matching header found in the source export.
    """
    src_map = column_map.get(source)
    if not src_map:
        raise ValueError(f"No column mapping found for source='{source}' in config/column_map.json")

    out = pd.DataFrame()

    # required
    missing_required = []
    for std in REQUIRED:
        found = _find_column(df, src_map.get(std, []))
        if not found:
            missing_required.append(std)
        else:
            out[std] = df[found]
    if missing_required:
        raise ValueError(f"Missing required standardized fields for {source}: {missing_required}. "
                         f"Check config/column_map.json and your input headers.")

    # optional
    for std in OPTIONAL:
        found = _find_column(df, src_map.get(std, []))
        if found:
            out[std] = df[found]

    return out
PY

# Update ingest.py to use mapping
cat > src/ingest.py << 'PY'
import pandas as pd
from mapping import load_column_map, apply_mapping

def load_csv(path: str, source: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    cmap = load_column_map()
    return apply_mapping(df, source=source, column_map=cmap)
PY

# Update run_agent.py to pass source into loader
python3 - << 'PY'
from pathlib import Path
p = Path("src/run_agent.py")
txt = p.read_text()

txt = txt.replace("from ingest import load_csv", "from ingest import load_csv")
txt = txt.replace("ledger_raw = load_csv(cfg.ledger_path)", "ledger_raw = load_csv(cfg.ledger_path, source='ledger')")
txt = txt.replace("sub_raw = load_csv(cfg.subledger_path)", "sub_raw = load_csv(cfg.subledger_path, source='subledger')")

p.write_text(txt)
PY

echo "v1.3 bootstrap complete."
