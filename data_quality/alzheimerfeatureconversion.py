# -*- coding: utf-8 -*-
"""AlzheimerFeatureConversion.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1UdVm5phmMJ600eigmGS-wzfqbqsHW36z
"""

from google.colab import drive
drive.mount('/content/drive')

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
alz_metadata_path = os.path.join(base_dir, metadata_folder, alz_metadata_fname)

# custom JSON encoder used for persisting metadata as a JSON file
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
tables = con.execute("SHOW TABLES").fetchall()
ret_str = "Tables in the formatted database:\n"
for table in tables:
    print(table[0])
con.close()

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
alz = con.execute(f"SELECT * FROM alz_texas_2017_2024_10_31_19_44_38").fetchdf()

print(alz.columns)
alz_modified = alz.copy()

# convert numerical to categorical
alz_modified["YearStart"] = alz_modified["YearStart"].astype('category')
alz_modified["YearEnd"] = alz_modified["YearEnd"].astype('category')

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

ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
alz_metadata_path, alz_metadata_ext = alz_metadata_path.split(".")[0], alz_metadata_path.split(".")[-1]
alz_metadata_full_path = os.path.join(alz_metadata_path + ts + "." + alz_metadata_ext)
with open(alz_metadata_full_path, "w") as f:
    json.dump(alz_metadata, f, cls=NpEncoder)

alz_modified.drop(["LocationAbbr", "LocationDesc", "Geolocation", "LocationID"], axis=1, inplace=True)
print(alz_modified.columns)