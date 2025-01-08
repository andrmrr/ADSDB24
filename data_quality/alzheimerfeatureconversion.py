# -*- coding: utf-8 -*-
"""AlzheimerFeatureConversion"""

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