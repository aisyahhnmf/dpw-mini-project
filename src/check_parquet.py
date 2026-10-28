import pandas as pd
from . import config as C
for name, p in (("RAW", C.RAW_PARQUET), ("VALID", C.VALID_BATCH_PARQUET), ("REJECTED", C.REJECTED_BATCH_PARQUET)):
    df = pd.read_parquet(p)
    print(f"== {name}: {p.name} | {p.stat().st_size/1024:.1f} KB | baris={len(df)} | kolom={len(df.columns)}")
    print(df.dtypes.to_string()); print(df.head(3).to_string()); print()
rej = pd.read_parquet(C.REJECTED_BATCH_PARQUET)
print("Distribusi alasan penolakan (per field):"); print(rej["reject_fields"].value_counts().to_string())
