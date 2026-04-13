"""
Silver layer data quality checks.
Runs after silver transforms, before gold transforms.
Raises ValueError if any check fails — halts the pipeline.
"""

import os
import pandas as pd

from etl.config_loader import load_config

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_SOIL = os.path.join(BASE_DIR, "data", "silver", "soil_readings")

config     = load_config()
thresholds = config["thresholds"]

MIN_ROW_COUNT   = 1000
REQUIRED_COLS   = ["site_name", "site_id", "event_time", "depth_cm", "measure_type", "soil_value"]
VALID_DEPTHS    = {10, 20, 30, 40, 50, 60, 70, 80}
VALID_MEASURES  = {"Moisture", "Temperature", "Salinity"}


def _fail(msg):
    print(f"[FAIL] {msg}")
    raise ValueError(f"Silver validation failed: {msg}")


def _pass(msg):
    print(f"[PASS] {msg}")


def validate():
    print("[INFO] Running silver data quality checks...")

    df = pd.read_parquet(SILVER_SOIL)

    # ----------------------------------------------------------------
    # 1. Minimum row count
    # ----------------------------------------------------------------
    row_count = len(df)
    if row_count < MIN_ROW_COUNT:
        _fail(f"Row count too low: {row_count} (expected >= {MIN_ROW_COUNT})")
    _pass(f"Row count: {row_count:,}")

    # ----------------------------------------------------------------
    # 2. Required columns present
    # ----------------------------------------------------------------
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        _fail(f"Missing columns: {missing_cols}")
    _pass(f"All required columns present")

    # ----------------------------------------------------------------
    # 3. No null site names or site IDs
    # ----------------------------------------------------------------
    null_sites = df["site_name"].isna().sum()
    if null_sites > 0:
        _fail(f"Null site_name values: {null_sites}")
    _pass(f"No null site names")

    null_ids = df["site_id"].isna().sum()
    if null_ids > 0:
        _fail(f"Null site_id values: {null_ids}")
    _pass(f"No null site IDs")

    # ----------------------------------------------------------------
    # 4. Valid measure types only
    # ----------------------------------------------------------------
    invalid_measures = set(df["measure_type"].dropna().unique()) - VALID_MEASURES
    if invalid_measures:
        _fail(f"Unexpected measure_type values: {invalid_measures}")
    _pass(f"Measure types valid: {set(df['measure_type'].unique())}")

    # ----------------------------------------------------------------
    # 5. Valid depth values only
    # ----------------------------------------------------------------
    invalid_depths = set(df["depth_cm"].dropna().unique()) - VALID_DEPTHS
    if invalid_depths:
        _fail(f"Unexpected depth_cm values: {invalid_depths}")
    _pass(f"Depth values valid")

    # ----------------------------------------------------------------
    # 6. Moisture values in range
    # ----------------------------------------------------------------
    moisture = df[df["measure_type"] == "Moisture"]["soil_value"]
    out_of_range = ((moisture < thresholds["moisture_min"]) | (moisture > thresholds["moisture_max"])).sum()
    if out_of_range > 0:
        _fail(f"Moisture out of range [{thresholds['moisture_min']}, {thresholds['moisture_max']}]: {out_of_range} rows")
    _pass(f"Moisture values in range")

    # ----------------------------------------------------------------
    # 7. No duplicate rows (same site + time + depth + measure)
    # ----------------------------------------------------------------
    key_cols = ["site_id", "event_time", "depth_cm", "measure_type", "soil_texture"]
    dupes = df.duplicated(subset=key_cols).sum()
    if dupes > 0:
        _fail(f"Duplicate rows found: {dupes}")
    _pass(f"No duplicate rows")

    # ----------------------------------------------------------------
    # 8. At least 10 distinct sites
    # ----------------------------------------------------------------
    site_count = df["site_name"].nunique()
    if site_count < 10:
        _fail(f"Too few distinct sites: {site_count} (expected >= 10)")
    _pass(f"Distinct sites: {site_count}")

    print("\nAll silver quality checks passed")


if __name__ == "__main__":
    validate()
