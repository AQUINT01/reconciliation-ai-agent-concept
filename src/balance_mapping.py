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
