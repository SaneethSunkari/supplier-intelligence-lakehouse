from __future__ import annotations

import zipfile
from pathlib import Path
from urllib.request import urlretrieve


DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/shahriarkabir/procurement-kpi-analysis-dataset"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "kaggle"
ZIP_PATH = RAW_DIR / "procurement-kpi-analysis-dataset.zip"
TARGET_PATH = RAW_DIR / "procurement_kpi.csv"


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    urlretrieve(DATASET_URL, ZIP_PATH)
    with zipfile.ZipFile(ZIP_PATH) as archive:
        source_name = "Procurement KPI Analysis Dataset.csv"
        archive.extract(source_name, RAW_DIR)
    extracted = RAW_DIR / source_name
    extracted.replace(TARGET_PATH)
    ZIP_PATH.unlink(missing_ok=True)
    print(f"Wrote {TARGET_PATH}")


if __name__ == "__main__":
    main()
