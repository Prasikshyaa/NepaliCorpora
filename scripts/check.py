from pathlib import Path
import pandas as pd

# --------------------------
# Configuration
# --------------------------
# Path to a specific cleaned parquet file
PARQUET_FILE = Path("data/processed/huggingface/raygx_Nepali-Text-Corpus/cleaned_0001.parquet")

# Number of rows to preview
ROWS_TO_PREVIEW = 5

# --------------------------
# Check file
# --------------------------
if not PARQUET_FILE.exists():
    raise FileNotFoundError(f"File does not exist: {PARQUET_FILE}")

# --------------------------
# Display settings for full text
# --------------------------
pd.set_option("display.max_colwidth", None)

# --------------------------
# Read and preview
# --------------------------
df = pd.read_parquet(PARQUET_FILE)

print(f"\n{'='*80}\nFile: {PARQUET_FILE.name}\n{'='*80}")

for i, row in df.head(ROWS_TO_PREVIEW).iterrows():
    print(f"Row {i}:")
    print(f"Text        : {row['text']}")
    print(f"Source      : {row.get('source', None)}")
    print(f"Dataset Name: {row.get('dataset_name', None)}")
    print("-"*80)
