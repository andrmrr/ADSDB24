"""Data Preparation"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.impute import KNNImputer

import duckdb
import json
import os

from util import *

# Categorical variables and one-hot encoding
def prep_categorical(analytical_df):
  analytical_df = analytical_df.drop(["StratificationCategory"], axis=1)
  analytical_df = analytical_df.reset_index()

  # One-hot encoding of categorical columns
  categorical_cols = analytical_df.select_dtypes(include=['object']).columns
  categorical_df = analytical_df[categorical_cols]
  for col in categorical_cols:
    df_expanded = categorical_df[categorical_df[col].notna()]
    cat_features = df_expanded[col].unique()
  for feat in cat_features:
      analytical_df[feat] = df_expanded[col].apply(lambda x: 1 if x == feat else 0)
  analytical_df = analytical_df.drop([col], axis=1)
  return analytical_df

# Missing values
def prep_missing(analytical_df):
  counts = (analytical_df == -1).sum()

  # Remove columns with more than 20 missing values
  columns_to_drop = counts > 20
  analytical_df = analytical_df.loc[:, ~columns_to_drop]
  analytical_df = analytical_df.replace(-1, np.nan)
  analytical_df = analytical_df.reset_index()

  return analytical_df

def prep_missing_knn(train, test):
  knn_imputer = KNNImputer(n_neighbors=3)
  train_imputed = pd.DataFrame(knn_imputer.fit_transform(train), columns=train.columns)
  test_imputed = pd.DataFrame(knn_imputer.transform(test), columns=test.columns)
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


""" Main """
def data_preparation_execute():
  con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
  analytical_df = con_sb.execute("SELECT * FROM combined_analytical_table").fetchdf()
  con_sb.close()

  analytical_df = prep_categorical(analytical_df)
  analytical_df = prep_missing(analytical_df)

  train, test = train_test_split(analytical_df, test_size=0.1, random_state=17)
  analytical_df = prep_missing_knn(train, test)

