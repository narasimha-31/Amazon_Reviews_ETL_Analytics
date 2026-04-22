"""
Download UCSD 2023 asin-to-category mapping.
Single file: asin2category.json from HuggingFace repo.
"""

from huggingface_hub import hf_hub_download

print("Downloading asin2category.json (1.25 GB)...")
local_path = hf_hub_download(
    "McAuley-Lab/Amazon-Reviews-2023",
    "asin2category.json",
    repo_type="dataset",
    local_dir="data/raw/ucsd"
)
print(f"Done — saved to {local_path}")