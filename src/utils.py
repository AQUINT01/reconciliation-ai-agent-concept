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
