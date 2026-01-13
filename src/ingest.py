import pandas as pd
from mapping import load_column_map, apply_mapping

def load_csv(path: str, source: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    cmap = load_column_map()
    return apply_mapping(df, source=source, column_map=cmap)
