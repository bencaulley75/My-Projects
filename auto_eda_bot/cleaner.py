import pandas as pd
import numpy as np


# -----------------------------------
# 1. AUTO CLEAN (Basic Cleaning Only)
# -----------------------------------
def auto_clean(df):

    # Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Remove duplicates
    df = df.drop_duplicates()

    # Trim whitespace
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].str.strip()

    # Convert date columns
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Handle missing values (but DO NOT drop columns automatically)
    for col in df.select_dtypes(include='number'):
        if df[col].isnull().mean() < 0.4:
            df[col] = df[col].fillna(df[col].median())

    for col in df.select_dtypes(include='object'):
        df[col] = df[col].fillna("Unknown")

    # Fix object types
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].astype(str)

    return df


# -----------------------------------
# 2. DETECT PROBLEM COLUMNS
# -----------------------------------
def detect_problem_columns(df):

    problem_columns = {
        "high_missing": [],
        "single_value": [],
        "possible_id": []
    }

    for col in df.columns:

        if df[col].isnull().mean() > 0.9:
            problem_columns["high_missing"].append(col)

        if df[col].nunique() <= 1:
            problem_columns["single_value"].append(col)

        if "id" in col.lower():
            problem_columns["possible_id"].append(col)

    return problem_columns


# -----------------------------------
# 3. DETECT OUTLIERS (IQR)
# -----------------------------------
def detect_outliers(df):

    outlier_info = {}
    numeric_cols = df.select_dtypes(include='number').columns

    for col in numeric_cols:

        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        outliers = df[(df[col] < lower) | (df[col] > upper)]

        if len(outliers) > 0:
            outlier_info[col] = {
                "count": len(outliers),
                "lower_bound": lower,
                "upper_bound": upper
            }

    return outlier_info


# -----------------------------------
# 4. APPLY OUTLIER TREATMENT
# -----------------------------------
def apply_outlier_treatment(df, outlier_info, method="cap"):

    for col, values in outlier_info.items():

        lower = values["lower_bound"]
        upper = values["upper_bound"]

        if method == "cap":
            df[col] = df[col].clip(lower, upper)

        elif method == "remove":
            df = df[(df[col] >= lower) & (df[col] <= upper)]

    return df