import gzip
import zipfile
import csv
import json
import io

# ─────────────────────────────────────────────────────
print("=" * 60)
print("SOURCE 1: UCSD 5-core (JSON.gz)")
print("=" * 60)

ucsd_path = r'data/raw/ucsd/kcore_5.json.gz'
ucsd_fields = {}

with gzip.open(ucsd_path, 'rt', encoding='utf-8', errors='replace') as f:
    for i, line in enumerate(f):
        record = json.loads(line)
        if i == 0:
            print("COLUMNS FOUND IN REAL FILE:")
            for key, value in record.items():
                ucsd_fields[key] = type(value).__name__
                print(f"  {key}: {str(value)[:60]}  (type: {type(value).__name__})")
        print(f"\n--- Record {i+1} ---")
        for key, value in record.items():
            print(f"  {key}: {str(value)[:80]}")
        if i >= 2:
            break

# ─────────────────────────────────────────────────────
print("\n\n" + "=" * 60)
print("SOURCE 2: Kaggle Electronics (TSV.zip)")
print("=" * 60)

kaggle_path = r'data/raw/kaggle/amazon_reviews_us_Electronics_v1_00.tsv.zip'
kaggle_fields = {}

with zipfile.ZipFile(kaggle_path, 'r') as zf:
    tsv_name = next(n for n in zf.namelist() if n.endswith('.tsv'))
    with io.TextIOWrapper(zf.open(tsv_name), encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        print("COLUMNS FOUND IN REAL FILE:")
        for i, row in enumerate(reader):
            if i == 0:
                for key, value in row.items():
                    kaggle_fields[key] = type(value).__name__
                    print(f"  {key}: {str(value)[:60]}  (type: {type(value).__name__})")
            print(f"\n--- Record {i+1} ---")
            for key, value in row.items():
                print(f"  {key}: {str(value)[:80]}")
            if i >= 2:
                break

# ─────────────────────────────────────────────────────
print("\n\n" + "=" * 60)
print("FIELD COMPARISON — what both sources actually have")
print("=" * 60)

all_fields = sorted(set(list(ucsd_fields.keys()) + list(kaggle_fields.keys())))
print(f"\n{'Field':<25} {'In UCSD?':<15} {'In Kaggle?':<15} {'Type UCSD':<12} {'Type Kaggle'}")
print("-" * 80)
for field in all_fields:
    in_ucsd   = "YES" if field in ucsd_fields   else "---"
    in_kaggle = "YES" if field in kaggle_fields else "---"
    type_ucsd   = ucsd_fields.get(field,   "---")
    type_kaggle = kaggle_fields.get(field, "---")
    print(f"  {field:<23} {in_ucsd:<15} {in_kaggle:<15} {type_ucsd:<12} {type_kaggle}")

print("\n\nFields ONLY in UCSD:")
for f in ucsd_fields:
    if f not in kaggle_fields:
        print(f"  {f}: {ucsd_fields[f]}")

print("\nFields ONLY in Kaggle:")
for f in kaggle_fields:
    if f not in ucsd_fields:
        print(f"  {f}: {kaggle_fields[f]}")

print("\nFields in BOTH sources:")
for f in ucsd_fields:
    if f in kaggle_fields:
        print(f"  {f}")