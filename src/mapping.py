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
