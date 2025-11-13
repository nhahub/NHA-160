"""
etl_cleaning.py
Generic ETL cleaning pipeline using pandas.

Usage:
    python etl_cleaning.py --input /path/to/test_Data.xlsx --output_dir /path/to/outdir

The script:
 - reads Excel/CSV
 - normalizes column names
 - drops empty columns
 - drops duplicates
 - trims strings & normalizes empty strings to NaN
 - converts numeric-like columns to numeric
 - detects & parses date-like columns
 - fills missing values according to simple rules (configurable)
 - removes totally-empty rows
 - exports cleaned CSV/Excel/Parquet
 - prints & saves a small report (json)
"""

import pandas as pd
import numpy as np
import argparse
import json
import os
from datetime import datetime
from dateutil import parser as dateparser

# -----------------------
# Utility functions
# -----------------------


def normalize_colname(col: str) -> str:
    """Normalize column name: strip, lower, replace spaces/special chars with underscore."""
    c = str(col).strip().lower()
    c = c.replace(" ", "_")
    # replace non-alnum/_ with underscore
    c = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in c)
    # collapse multiple underscores
    while "__" in c:
        c = c.replace("__", "_")
    return c.strip("_")


def read_input(path: str) -> pd.DataFrame:
    """Read Excel or CSV depending on extension. Returns DataFrame."""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xls", ".xlsx"]:
        return pd.read_excel(path, engine="openpyxl")
    elif ext == ".csv":
        return pd.read_csv(path)
    else:
        raise ValueError("Unsupported file type: " + ext)

# -----------------------
# Cleaning steps as functions
# -----------------------


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]
    return df


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df.drop(columns=empty_cols, inplace=True)
    return df


def drop_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    print(f"dropped {before-after} exact duplicate rows")
    return df


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        # strip whitespace, convert empty strings to NaN
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)
        df.loc[df[c] == "", c] = np.nan
    return df


def convert_numeric_columns(df: pd.DataFrame, threshold=0.6) -> pd.DataFrame:
    """
    Convert object columns that look numeric into numeric.
    threshold: fraction of sample values that must be numeric-like to convert.
    """
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object":
            sample = df[c].dropna().astype(str).head(200)
            if len(sample) == 0:
                continue
            # remove common thousands separators and spaces
            s_clean = sample.str.replace(",", "").str.replace(" ", "")
            numeric_like = s_clean.str.match(r"^-?\d+(\.\d+)?$").sum()
            if numeric_like / len(sample) >= threshold:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(
                    ",", "").str.strip(), errors="coerce")
    return df


def detect_and_parse_dates(df: pd.DataFrame, min_fraction=0.6) -> (pd.DataFrame, list):
    """
    Detect date-like columns and parse them to datetime.
    Returns (df, parsed_columns)
    """
    df = df.copy()
    parsed_cols = []
    for c in df.columns:
        # heuristic: column name contains 'date' OR many sample values parse as dates
        if "date" in c:
            try:
                parsed = pd.to_datetime(
                    df[c], errors="coerce", infer_datetime_format=True)
                if parsed.notna().sum() > 0:
                    df[c] = parsed
                    parsed_cols.append(c)
                    continue
            except Exception:
                pass
        # otherwise sample parsing
        sample = df[c].dropna().astype(str).head(200)
        if len(sample) >= 10:
            parsed = pd.to_datetime(
                sample, errors="coerce", infer_datetime_format=True)
            if parsed.notna().sum() / len(sample) >= min_fraction:
                # parse whole column
                df[c] = pd.to_datetime(
                    df[c], errors="coerce", infer_datetime_format=True)
                parsed_cols.append(c)
    return df, parsed_cols


def fill_missing_values(df: pd.DataFrame,
                        numeric_strategy="median",
                        categorical_fill="Unknown",
                        date_fill=None,
                        numeric_exceptions: dict = None) -> pd.DataFrame:
    """
    Fill missing values:
      - numeric_strategy: 'median' or 'mean' or a dict {col: value}
      - categorical_fill: string to fill object columns
      - date_fill: if provided, fill datetime columns with this value (datetime or 'today' etc.)
      - numeric_exceptions: dict of per-column fill values
    """
    df = df.copy()
    numeric_exceptions = numeric_exceptions or {}

    # numeric columns
    num_cols = df.select_dtypes(include=["number"]).columns
    for c in num_cols:
        if c in numeric_exceptions:
            df[c] = df[c].fillna(numeric_exceptions[c])
            continue
        if numeric_strategy == "median":
            med = df[c].median(skipna=True)
            if pd.notna(med):
                df[c] = df[c].fillna(med)
        elif numeric_strategy == "mean":
            m = df[c].mean(skipna=True)
            if pd.notna(m):
                df[c] = df[c].fillna(m)
        elif isinstance(numeric_strategy, dict):
            if c in numeric_strategy:
                df[c] = df[c].fillna(numeric_strategy[c])
        # else leave as is

    # object columns
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].fillna(categorical_fill)

    # datetime columns
    dt_cols = df.select_dtypes(
        include=["datetime64[ns]", "datetime64"]).columns
    if date_fill is not None:
        if isinstance(date_fill, str) and date_fill.lower() == "today":
            date_fill_val = pd.to_datetime(datetime.utcnow())
        else:
            date_fill_val = pd.to_datetime(date_fill)
        for c in dt_cols:
            df[c] = df[c].fillna(date_fill_val)

    return df


def drop_rows_all_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)
    df.dropna(how="all", inplace=True)
    after = len(df)
    print(f"dropped {before-after} entirely-empty rows")
    return df

# -----------------------
# Reporting & saving
# -----------------------


def generate_report(df_original: pd.DataFrame, df_clean: pd.DataFrame, parsed_date_cols: list) -> dict:
    report = {
        "original_shape": df_original.shape,
        "cleaned_shape": df_clean.shape,
        "columns_original": list(df_original.columns),
        "columns_clean": list(df_clean.columns),
        "parsed_date_columns": parsed_date_cols,
        "missing_counts_after": df_clean.isna().sum().sort_values(ascending=False).to_dict(),
        "sample_head": df_clean.head(5).to_dict(orient="records")
    }
    return report


def save_outputs(df: pd.DataFrame, out_dir: str, base_name="cleaned_data"):
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"{base_name}.csv")
    excel_path = os.path.join(out_dir, f"{base_name}.xlsx")
    parquet_path = os.path.join(out_dir, f"{base_name}.parquet")
    df.to_csv(csv_path, index=False)
    # Excel writer
    df.to_excel(excel_path, index=False)
    # Parquet (fast binary)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print("parquet save failed (pyarrow may be missing).", e)
        parquet_path = None
    return {"csv": csv_path, "excel": excel_path, "parquet": parquet_path}

# -----------------------
# Main pipeline
# -----------------------


def run_pipeline(input_path: str, output_dir: str, config: dict = None):
    """
    config: dict with options like:
      - numeric_strategy
      - categorical_fill
      - date_fill
      - numeric_exceptions
    """
    config = config or {}
    print("reading:", input_path)
    df_orig = read_input(input_path)
    print("original shape:", df_orig.shape)

    df = normalize_columns(df_orig)
    df = drop_empty_columns(df)
    df = drop_exact_duplicates(df)
    df = trim_string_columns(df)
    df = convert_numeric_columns(df, threshold=config.get(
        "numeric_detection_threshold", 0.6))
    df, parsed_date_cols = detect_and_parse_dates(
        df, min_fraction=config.get("date_detection_fraction", 0.6))
    df = fill_missing_values(df,
                             numeric_strategy=config.get(
                                 "numeric_strategy", "median"),
                             categorical_fill=config.get(
                                 "categorical_fill", "Unknown"),
                             date_fill=config.get("date_fill", None),
                             numeric_exceptions=config.get("numeric_exceptions", None))
    df = drop_rows_all_null(df)

    report = generate_report(df_orig, df, parsed_date_cols)
    out_paths = save_outputs(df, output_dir, base_name=config.get(
        "output_base_name", "cleaned_data"))

    # save report json
    report_path = os.path.join(output_dir, config.get(
        "report_name", "cleaning_report.json"))
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, default=str, ensure_ascii=False, indent=2)

    print("saved outputs to:", out_paths)
    print("report saved to:", report_path)
    return {"dataframe": df, "report": report, "paths": out_paths, "report_path": report_path}


# -----------------------
# Command-line interface
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL cleaning pipeline")
    parser.add_argument("--input", "-i", required=True,
                        help="Input file (Excel or CSV)")
    parser.add_argument("--output_dir", "-o", required=True,
                        help="Directory to save outputs")
    parser.add_argument("--date_fill", default=None,
                        help="Date fill value (e.g., 'today' or '2021-01-01')")
    args = parser.parse_args()

    cfg = {
        "numeric_strategy": "median",
        "categorical_fill": "Unknown",
        "date_fill": args.date_fill,
        "numeric_detection_threshold": 0.6,
        "date_detection_fraction": 0.6,
        "output_base_name": "cleaned_data",
        "report_name": "cleaning_report.json"
    }
    run_pipeline(args.input, args.output_dir, cfg)
