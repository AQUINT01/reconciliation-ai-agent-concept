from pathlib import Path
from reconcile import load_ledger, reconcile_by_fund_month

BASE = Path(__file__).resolve().parents[1]

def main():
    ledger_path = BASE / "data" / "sample_ledger.csv"
    output_path = BASE / "outputs" / "sample_recon_report.json"

    df = load_ledger(str(ledger_path))
    recon = reconcile_by_fund_month(df)

    output_path.write_text(recon.to_json(orient="records", indent=2))
    print(f"Report written to {output_path}")

if __name__ == "__main__":
    main()
