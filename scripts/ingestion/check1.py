import pandas as pd
import sys
import io

# Force UTF-8 encoding for terminal output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Read the Parquet file
df = pd.read_parquet('data/raw/scraped/onlinekhabar/articles.parquet')

print(f'Total articles: {len(df)}\n')

print('--- First Article ---')
row = df.iloc[0][['title', 'word_count', 'category']]
for col, val in row.items():
    print(f"{col}: {val}")

print('\nContent preview:')
content = df.iloc[0]['content']
print(content[:500])  # Just print first 500 characters
