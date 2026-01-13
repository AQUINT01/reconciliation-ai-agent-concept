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
