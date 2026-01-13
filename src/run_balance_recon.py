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
