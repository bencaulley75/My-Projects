import pandas as pd
import numpy as np

def auto_clean(df):

    # -------------------------
    # 1. Standardize Column Names
    # -------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # -------------------------
    # 2. Remove Duplicates
    # -------------------------
    df = df.drop_duplicates()

    # -------------------------
    # 3. Trim Whitespace in Strings
    # -------------------------
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].str.strip()

    # -------------------------
    # 4. Convert Date Columns Automatically
    # -------------------------
    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # -------------------------
    # 5. Handle Missing Values
    # -------------------------
    for col in df.select_dtypes(include='number'):
        if df[col].isnull().mean() < 0.4:
            df[col] = df[col].fillna(df[col].median())
        else:
            df = df.drop(columns=[col])

    for col in df.select_dtypes(include='object'):
        df[col] = df[col].fillna("Unknown")

    # -------------------------
    # 6. Remove Columns with One Unique Value
    # -------------------------
    for col in df.columns:
        if df[col].nunique() == 1:
            df = df.drop(columns=[col])

    # -------------------------
    # 7. Outlier Handling (IQR Capping)
    # -------------------------
    numeric_cols = df.select_dtypes(include='number').columns

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        
        df[col] = np.where(df[col] < lower, lower, df[col])
        df[col] = np.where(df[col] > upper, upper, df[col])

    # -------------------------
    # 8. Fix Mixed Object Columns for Streamlit
    # -------------------------
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].astype(str)
    return df