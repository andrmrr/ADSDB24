# -*- coding: utf-8 -*-
"""data preparation"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.impute import KNNImputer

import duckdb
import json
import os

base_dir = "/content/drive/MyDrive/ADSDB24"
temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
dataset_folder = "datasets_specific"

"""DuckDB"""
duckdb_folder = "duckdb_database"
duckdb_formatted = os.path.join(duckdb_folder, "formatted_zone.db")
duckdb_trusted = os.path.join(duckdb_folder, "trusted_zone.db")
duckdb_exploitation = os.path.join(duckdb_folder, "exploitation_zone.db")
duckdb_sandbox = os.path.join(duckdb_folder, "sandbox_zone.db")

"""Metadata"""
metadata_folder = "metadata"
formatted_metadata = "formatted_metadata.json"
alz_metadata_fname = "alzheimer_metadata.json"
chr_metadata_fname = "chronic_disease_indicators_metadata.json"
analytical_sandbox_metadata = "analytical_sandbox_metadata.json"

data_abbrev = ["alz", "chr"]

"""Check the table in the sandbox"""

con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
tables = con_sb.execute("SHOW TABLES").fetchall()
for table in tables:
  print(table[0])
con_sb.close()

con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
analytical_df = con_sb.execute("SELECT * FROM combined_analytical_table").fetchdf()
con_sb.close()

"""### One hot encoding of categorical columns"""

analytical_df = analytical_df.drop(["StratificationCategory"], axis=1)
analytical_df = analytical_df.reset_index()

categorical_cols = analytical_df.select_dtypes(include=['object']).columns
# print(categorical_cols)

categorical_df = analytical_df[categorical_cols]
print(categorical_df)

for col in categorical_cols:
  # take a list of possible values for a list feature
  df_expanded = categorical_df[categorical_df[col].notna()]
  # train_expanded_lot_features = train_expanded["Characteristics.LotFeatures"]
  cat_features = df_expanded[col].unique()
  print(cat_features)

  # add binary columns for each feature
  for feat in cat_features:
      analytical_df[feat] = df_expanded[col].apply(lambda x: 1 if x == feat else 0)

  # print(df_expanded.head(10))
  analytical_df = analytical_df.drop([col], axis=1)
  # analytical_df = pd.concat([analytical_df, df_expanded])

print(analytical_df.columns)

"""### Missing values

Remove rows with missing target variable
"""

print(analytical_df.shape)
analytical_df = analytical_df[analytical_df["QTarget"] != -1]
print(analytical_df.shape)

"""Print the number of missing values for each question (-1 is missing)"""

print(analytical_df.shape)
counts = (analytical_df == -1).sum()
counts_gr0 = counts[counts > 100]
print(counts_gr0)

"""\Remove columns with more than 20 missing values"""

columns_to_drop = counts > 20
analytical_df = analytical_df.loc[:, ~columns_to_drop]
print(analytical_df.shape)

analytical_df = analytical_df.replace(-1, np.nan)
analytical_df = analytical_df.reset_index()

"""### Split into train and test before imputing anything and then impute both train and test, but using only the train set information"""

train, test = train_test_split(analytical_df, test_size=0.1, random_state=17)

"""Impute all other missing values using MIMMI

MIMMI does not execute, because it exceeds available RAM
"""

"""Try KNN imputation"""

knn_imputer = KNNImputer(n_neighbors=3)
train_imputed = pd.DataFrame(knn_imputer.fit_transform(train), columns=train.columns)
test_imputed = pd.DataFrame(knn_imputer.transform(test), columns=test.columns)

print(train_imputed.isnull().sum())
print(test_imputed.isnull().sum())

con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
con_sb.execute("DROP TABLE IF EXISTS train")
con_sb.execute("DROP TABLE IF EXISTS test")
con_sb.execute("DROP TABLE IF EXISTS train_data")
con_sb.execute("DROP TABLE IF EXISTS test_data")
con_sb.execute("CREATE TABLE train_data AS SELECT * FROM train_imputed")
con_sb.execute("CREATE TABLE test_data AS SELECT * FROM test_imputed")
con_sb.close()

train_imputed.to_csv(os.path.join(base_dir, "train_preprocessed.csv"))
test_imputed.to_csv(os.path.join(base_dir, "test_preprocessed.csv"))