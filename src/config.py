from dataclasses import dataclass

@dataclass(frozen=True)
class ReconConfig:
    ledger_path: str = "data/raw/ledger.csv"
    subledger_path: str = "data/raw/subledger.csv"
    outputs_dir: str = "outputs"

    date_window_days: int = 2          # allowed date difference for matching
    amount_tolerance: float = 0.50     # allowed amount difference for tolerance match
    min_similarity: int = 85           # 0-100 RapidFuzz threshold
