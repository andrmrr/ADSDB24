# -*- coding: utf-8 -*-
"""Alzeheimer_all"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb

import datetime
import os
import json

from util import *

"""Load the data"""

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
alz = con.execute(f"SELECT * FROM alz_texas_2017_2024_10_31_19_44_38").fetchdf()

print(alz.columns)

"""Profiling"""

def profiling(df, show_plots):
  # print(df[:4])
  print(df.shape)
  print(df.info())
  print(df.describe())
  print(df.describe(include='object'))

  if show_plots:
    # plot the counts of categorical features
    categorical_features = df.select_dtypes(include=["object", "category"]).columns
    for col in categorical_features:
      sns.displot(df, x=col, kde=True)
      plt.show()

    # plot the distributions of numerical features
    numerical_features = df.select_dtypes(include="number").columns
    for col in numerical_features:
        sns.histplot(df[col], kde=True)
        plt.title(f'Distribution of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.show()

"""Data Quality"""

profiling(alz, True)

#drop the first column
alz_modified = alz.drop("Unnamed: 0", axis=1)

"""## Missing values"""

print(alz_modified.shape)

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

"""## Feature type conversion"""

# convert numerical to categorical
alz_modified["YearStart"] = alz_modified["YearStart"].astype('category')
alz_modified["YearEnd"] = alz_modified["YearEnd"].astype('category')

"""## Removing redundant features"""

print(alz_modified["Data_Value_Unit"].unique())
print(alz_modified["DataValueTypeID"].unique())
print(alz_modified["Data_Value_Type"].unique())

# drop Data_Value_Unit, DataValueTypeID and Data_Value_Type columns
alz_modified.drop(["Data_Value_Unit", "DataValueTypeID"], axis=1, inplace=True)

# check if Data_Value and Data_Value_Alt are identical, and if so remove Data_Value_Alt
if (alz_modified["Data_Value"] == alz_modified["Data_Value_Alt"]).all():
    alz_modified.drop("Data_Value_Alt", axis=1, inplace=True)

# Remove all single value columns and save them in a dictionary
alz_metadata = {}
for col in alz.columns:
    unique_count = len(alz[col].unique())
    if unique_count <= 1:
        print(f"{col}: {unique_count}")
        print(f"---> Value: {alz[col][0]}")
        alz_metadata[col] = alz[col][0]
    print(alz_metadata)

print(alz_metadata_path)

ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
alz_metadata_path, alz_metadata_ext = alz_metadata_path.split(".")[0], alz_metadata_path.split(".")[-1]
alz_metadata_full_path = os.path.join(alz_metadata_path + ts + "." + alz_metadata_ext)
with open(alz_metadata_full_path, "w") as f:
    json.dump(alz_metadata, f, cls=NpEncoder)

alz_modified.drop(["LocationAbbr", "LocationDesc", "Geolocation", "LocationID"], axis=1, inplace=True)
print(alz_modified.columns)

"""We get the same value in all rows for the following columns with following values: LocationAbbr - TX, LocationDesc - Texas, StratificationCategory1 - Age Group, Geolocation - POINT(-99.42677021 31.82724041), LocationID - 48 and StratificationCategoryID1 - AGE.
Location related features are chosen in advance, so we can take them out and save the static values, which hold true for the entire dataset. Datasource will also be moved to metadata and removed from the dataset.
We will leave the stratification features in for now.

Check for duplicates
"""

print(alz_modified.shape[0])
alz_modified.drop_duplicates(inplace=True)
print(alz_modified.shape[0])

"""No normalization, scaling or spellchecking is necessary for this dataset"""

profiling(alz_modified, False)