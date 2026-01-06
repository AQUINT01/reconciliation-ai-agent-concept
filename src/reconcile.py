import pandas as pd

def load_ledger(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
    df["debit"] = pd.to_numeric(df["debit"], errors="coerce").fillna(0.0)
    df["credit"] = pd.to_numeric(df["credit"], errors="coerce").fillna(0.0)
    df["month"] = df["posting_date"].dt.to_period("M").astype(str)
    df["net"] = df["debit"] - df["credit"]
    return df

def reconcile_by_fund_month(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["fund_id", "month"], as_index=False)
          .agg(
              total_debit=("debit", "sum"),
              total_credit=("credit", "sum"),
              net=("net", "sum")
          )
    )
