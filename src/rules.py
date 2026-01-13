import json
from dataclasses import dataclass
from typing import Dict, Any

@dataclass(frozen=True)
class Rules:
    date_window_days: int
    amount_tolerance: float
    min_similarity: int
    top_k_suggestions: int
    field_weights: Dict[str, int]

def load_rules(path: str = "config/recon_config.json") -> Rules:
    with open(path, "r") as f:
        raw: Dict[str, Any] = json.load(f)

    return Rules(
        date_window_days=int(raw.get("date_window_days", 2)),
        amount_tolerance=float(raw.get("amount_tolerance", 0.50)),
        min_similarity=int(raw.get("min_similarity", 85)),
        top_k_suggestions=int(raw.get("top_k_suggestions", 3)),
        field_weights=dict(raw.get("field_weights", {})),
    )
