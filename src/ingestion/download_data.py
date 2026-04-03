"""
download_data.py — Phase 1, Step 1
Downloads raw data files from UCSD and AWS sources.

Usage:
    python src/ingestion/download_data.py --source all
    python src/ingestion/download_data.py --source ucsd
    python src/ingestion/download_data.py --source aws
    python src/ingestion/download_data.py --source ucsd --category Electronics

Files land in: data/raw/ucsd/   and   data/raw/aws/
Nothing is extracted or modified — files stay as .gz
"""

import argparse
import hashlib
import sys
from pathlib import Path

import requests
from tqdm import tqdm

# ── Allow running from project root ──────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import UCSD_CATEGORIES, AWS_FILES, DATA_DIR, LOG_DIR, DOWNLOAD_TIMEOUT
from src.utils.logger import get_logger

logger = get_logger("download_data", LOG_DIR)


# ─────────────────────────────────────────────────────────────
def download_file(url: str, dest_path: Path, label: str) -> bool:
    """
    Stream-download a file with a progress bar.
    Skips download if the file already exists and is non-empty.
    Returns True on success, False on failure.
    """
    if dest_path.exists() and dest_path.stat().st_size > 0:
        size_mb = dest_path.stat().st_size / 1_048_576
        logger.info(f"SKIP — already exists ({size_mb:.1f} MB): {dest_path.name}")
        return True

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {label} → {dest_path.name}")

    try:
        response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()

        total_bytes = int(response.headers.get("content-length", 0))
        chunk_size  = 1024 * 1024  # 1 MB chunks

        with open(dest_path, "wb") as f, tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            desc=dest_path.name,
            ncols=80,
        ) as bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

        final_size = dest_path.stat().st_size / 1_048_576
        logger.info(f"Downloaded {final_size:.1f} MB → {dest_path.name}")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"FAILED {label}: {e}")
        # Remove partial file to avoid confusing the loader later
        if dest_path.exists():
            dest_path.unlink()
        return False


# ─────────────────────────────────────────────────────────────
def download_ucsd(category_filter: str = None):
    """Download UCSD Amazon Reviews 2023 files."""
    ucsd_dir = DATA_DIR / "ucsd"
    categories = (
        {category_filter: UCSD_CATEGORIES[category_filter]}
        if category_filter
        else UCSD_CATEGORIES
    )

    logger.info(f"=== UCSD Download: {list(categories.keys())} ===")
    results = {}

    for category, url in categories.items():
        filename  = f"{category}.jsonl.gz"
        dest_path = ucsd_dir / filename
        success   = download_file(url, dest_path, f"UCSD/{category}")
        results[category] = "OK" if success else "FAILED"

    _print_summary("UCSD", results)
    return results


def download_aws(category_filter: str = None):
    """Download AWS Customer Review TSV files."""
    aws_dir = DATA_DIR / "aws"
    files = (
        {category_filter: AWS_FILES[category_filter]}
        if category_filter and category_filter in AWS_FILES
        else AWS_FILES
    )

    logger.info(f"=== AWS Download: {list(files.keys())} ===")
    results = {}

    for category, url in files.items():
        filename  = Path(url).name         # keep original filename
        dest_path = aws_dir / filename
        success   = download_file(url, dest_path, f"AWS/{category}")
        results[category] = "OK" if success else "FAILED"

    _print_summary("AWS", results)
    return results


def _print_summary(source: str, results: dict):
    ok     = sum(1 for v in results.values() if v == "OK")
    failed = sum(1 for v in results.values() if v != "OK")
    logger.info(f"{'─'*50}")
    logger.info(f"{source} download complete — {ok} OK, {failed} FAILED")
    for k, v in results.items():
        logger.info(f"  {'✓' if v == 'OK' else '✗'}  {k}: {v}")
    logger.info(f"{'─'*50}")


# ─────────────────────────────────────────────────────────────
def inspect_raw_files():
    """
    After download: print a quick inventory of what landed in data/raw/.
    No extraction — just file names, sizes, and a sanity check.
    """
    logger.info("=== Raw File Inventory ===")
    total_gb = 0.0

    for folder in [DATA_DIR / "ucsd", DATA_DIR / "aws"]:
        if not folder.exists():
            continue
        logger.info(f"\n{folder.relative_to(DATA_DIR.parent.parent)}/")
        for f in sorted(folder.iterdir()):
            size_mb = f.stat().st_size / 1_048_576
            total_gb += size_mb / 1024
            logger.info(f"  {f.name:<55} {size_mb:>8.1f} MB")

    logger.info(f"\nTotal on disk: {total_gb:.2f} GB")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Amazon review raw data files")
    parser.add_argument(
        "--source",
        choices=["ucsd", "aws", "all"],
        default="all",
        help="Which dataset to download (default: all)",
    )
    parser.add_argument(
        "--category",
        default=None,
        help="Optional: download only one category (e.g. Electronics)",
    )
    args = parser.parse_args()

    if args.source in ("ucsd", "all"):
        download_ucsd(args.category)

    if args.source in ("aws", "all"):
        download_aws(args.category)

    inspect_raw_files()
