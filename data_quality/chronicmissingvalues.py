# -*- coding: utf-8 -*-
"""ChronicMissingValues"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb

import datetime
import os
import json

base_dir = "/content/drive/MyDrive/ADSDB24"

"""DuckDB"""
duckdb_folder = "duckdb_database"
duckdb_formatted = os.path.join(duckdb_folder, "formatted_zone.db")
duckdb_trusted = os.path.join(duckdb_folder, "trusted_zone.db")
duckdb_exploitation = os.path.join(duckdb_folder, "exploitation_zone.db")

"""Metadata"""
metadata_folder = "metadata"
formatted_metadata = "formatted_metadata.json"
alz_metadata_fname = "alzheimer_metadata.json"
chr_metadata_fname = "chronic_disease_indicators_metadata.json"
chr_metadata_path = os.path.join(base_dir, metadata_folder, chr_metadata_fname)

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
tables = con.execute("SHOW TABLES").fetchall()
ret_str = "Tables in the formatted database:\n"
for table in tables:
    print(table[0])
con.close()

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
chr = con.execute(f"SELECT * FROM chr_texas_2017_2024_10_31_19_44_38").fetchdf()
chr_modified = chr.copy()

for col in chr_modified.columns:
    has_null = chr_modified[col].isnull().any()
    if has_null:
        print(f"{col}: {chr_modified[col].isnull().sum()}")

# drop columns with all empty rows
print(f"Total rows: {chr_modified.shape[0]}")
to_be_dropped = []
for col in chr_modified.columns:
  has_null = chr_modified[col].isnull().any()
  if has_null and chr_modified[col].isnull().sum() == chr_modified.shape[0]:
      to_be_dropped.append(col)
      print(f"{col}: {chr_modified[col].isnull().sum()}")

  chr_modified = chr_modified.drop(to_be_dropped, axis=1)

# impute missing values with -1 for the numerical features
chr_modified["DataValue"].fillna(-1, inplace=True)
chr_modified["DataValueAlt"].fillna(-1, inplace=True)
chr_modified["LowConfidenceLimit"].fillna(-1, inplace=True)
chr_modified["HighConfidenceLimit"].fillna(-1, inplace=True)

print(chr_modified["DataValueFootnoteSymbol"].unique())
print(chr_modified["DatavalueFootnote"].unique())
print(chr_modified["DataValueUnit"].unique())

# impute missing values with -1 for DatavalueFootnote and DataValueUnit
chr_modified["DatavalueFootnote"].fillna("NONE", inplace=True)
chr_modified["DataValueUnit"].fillna("NONE", inplace=True)

# drop DataValueFootnoteSymbol column
chr_modified.drop(["DataValueFootnoteSymbol"], axis=1, inplace=True)