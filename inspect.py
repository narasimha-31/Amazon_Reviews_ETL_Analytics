import gzip
import json

path = r'data/raw/ucsd/kcore_5.json.gz'

with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
    for i, line in enumerate(f):
        record = json.loads(line)
        print(f"--- Record {i+1} ---")
        for key, value in record.items():
            print(f"  {key}: {str(value)[:80]}")
        print()
        if i >= 2:
            break
