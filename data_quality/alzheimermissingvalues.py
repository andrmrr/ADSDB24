# -*- coding: utf-8 -*-
"""AlzheimerMissingValues"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb

import datetime
import os
import json

from util import *

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
alz = con.execute(f"SELECT * FROM alz_texas_2017_2024_10_31_19_44_38").fetchdf()

print(alz.columns)

alz_modified = alz.copy()
for col in alz_modified.columns:
    has_null = alz_modified[col].isnull().any()
    if has_null:
        print(f"{col}: {alz_modified[col].isnull().sum()}")

# impute missing values for Data_Value, Data_Value_Alt, Low_Confidence_Limit and High_Confidence_Limit with -1
alz_modified["Data_Value"].fillna(-1, inplace=True)
alz_modified["Data_Value_Alt"].fillna(-1, inplace=True)
alz_modified["Low_Confidence_Limit"].fillna(-1, inplace=True)
alz_modified["High_Confidence_Limit"].fillna(-1, inplace=True)

for col in alz_modified.columns:
    has_null = alz_modified[col].isnull().any()
    if has_null:
        print(f"{col}: {alz_modified[col].isnull().sum()}")

print(alz_modified["Data_Value_Footnote_Symbol"].unique())
print(alz_modified["Data_Value_Footnote"].unique())

# drop Data_Value_Footnote_Symbol
alz_modified.drop(["Data_Value_Footnote_Symbol"], axis=1, inplace=True)

# impute Data_Value_Footnote with string "NONE"
alz_modified["Data_Value_Footnote"].fillna("NONE", inplace=True)

print(alz_modified["StratificationCategory2"].unique())
print(alz_modified["Stratification2"].unique())

# impute StratificationCategory2 and Stratification2 with string "NONE"
alz_modified["StratificationCategory2"].fillna("NONE", inplace=True)
alz_modified["Stratification2"].fillna("NONE", inplace=True)