# -*- coding: utf-8 -*-
"""ChronicFeatureConversion"""

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
chr = con.execute(f"SELECT * FROM chr_texas_2017_2024_10_31_19_44_38").fetchdf()
chr_modified = chr.copy()

# convert numerical to categorical
chr_modified["YearStart"] = chr_modified["YearStart"].astype('category')
chr_modified["YearEnd"] = chr_modified["YearEnd"].astype('category')

print(chr_modified[chr_modified["DataValue"] == "No"])

# treat datavalue as numerical
# remove all rows with non-numerical values
print(chr_modified.shape)
chr_modified.drop(chr_modified[chr_modified["DataValue"] == "No"].index, axis=0, inplace=True)
chr_modified.drop(chr_modified[chr_modified["DataValue"] == "Yes"].index, axis=0, inplace=True)
chr_modified.drop(chr_modified[chr_modified["DataValue"] == "Category 2 - State had commercial host liability with major limitations"].index, axis=0, inplace=True)
print(chr_modified.shape)

chr_modified["DataValue"] = chr_modified["DataValue"].astype('float64')

# check if Data_Value and Data_Value_Alt are identical, and if so remove Data_Value_Alt
if (chr_modified["DataValue"] == chr_modified["DataValueAlt"]).all():
    chr_modified.drop("DataValueAlt", axis=1, inplace=True)

for col in chr_modified.columns:
    has_null = chr_modified[col].isnull().any()
    if has_null:
        print(f"{col}: {chr_modified[col].isnull().sum()}")

# Remove all single value columns and save them in a json file
chr_metadata = {}
for col in chr_modified.columns:
    unique_count = len(chr_modified[col].unique())
    if unique_count <= 1:
        print(f"{col}: {unique_count}")
        print(f"---> Value: {chr_modified[col][0]}")
        chr_metadata[col] = chr_modified[col][0]
print(chr_metadata)

ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
chr_metadata_path, chr_metadata_ext = chr_metadata_path.split(".")[:-1], chr_metadata_path.split(".")[-1]
chr_metadata_path = ".".join(chr_metadata_path)
chr_metadata_full_path = os.path.join(chr_metadata_path + ts + "." + chr_metadata_ext)
with open(chr_metadata_full_path, "w") as f:
    json.dump(chr_metadata, f, cls=NpEncoder)

chr_modified.drop(["LocationAbbr", "LocationDesc", "GeoLocation", "LocationID"], axis=1, inplace=True)
print(chr_modified.columns)

"""We get the same value in all rows for the following columns with following values: LocationAbbr - TX, LocationDesc - Texas, StratificationCategory1 - Age Group, Geolocation - POINT(-99.42677021 31.82724041), LocationID - 48 and StratificationCategoryID1 - AGE.
Location related features are chosen in advance, so we can take them out and save the static values, which hold true for the entire dataset. Datasource will also be moved to metadata and removed from the dataset.
We will leave the stratification features in for now.

"""