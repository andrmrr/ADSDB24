""" Chronic diseases dataset preprocessing """

import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import datetime
import os
import json

# from spellchecker import SpellChecker

from dataprofiling import profiling
from util import *

def chr_preprocess(chr):
    # profiling(chr, True)
    chr_metadata_path = os.path.join(base_dir, metadata_folder, chr_metadata_fname)
    #drop the first column
    chr_modified = chr.drop("Unnamed: 0", axis=1)

    """Missing values"""
    print(chr_modified.shape)

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

    chr_modified["DataValue"].fillna(-1, inplace=True)
    chr_modified["DataValueAlt"].fillna(-1, inplace=True)
    chr_modified["LowConfidenceLimit"].fillna(-1, inplace=True)
    chr_modified["HighConfidenceLimit"].fillna(-1, inplace=True)

    print(chr_modified["DataValueFootnoteSymbol"].unique())
    print(chr_modified["DatavalueFootnote"].unique())
    print(chr_modified["DataValueUnit"].unique())

    chr_modified["DatavalueFootnote"].fillna("NONE", inplace=True)
    chr_modified["DataValueUnit"].fillna("NONE", inplace=True)

    chr_modified.drop(["DataValueFootnoteSymbol"], axis=1, inplace=True)

    """Feature type conversion"""

    # convert numerical to categorical
    chr_modified["YearStart"] = chr_modified["YearStart"].astype('category')
    chr_modified["YearEnd"] = chr_modified["YearEnd"].astype('category')

    print(chr_modified[chr_modified["DataValue"] == "No"])

    # treat datavalue as numerical
    print(chr_modified.shape)
    chr_modified.drop(chr_modified[chr_modified["DataValue"] == "No"].index, axis=0, inplace=True)
    chr_modified.drop(chr_modified[chr_modified["DataValue"] == "Yes"].index, axis=0, inplace=True)
    chr_modified.drop(chr_modified[chr_modified["DataValue"] == "Category 2 - State had commercial host liability with major limitations"].index, axis=0, inplace=True)
    print(chr_modified.shape)

    chr_modified["DataValue"] = chr_modified["DataValue"].astype('float64')

    """Remove redundant features"""

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

    """Check for duplicates"""

    print(chr_modified.shape[0])
    chr_modified.drop_duplicates(inplace=True)
    print(chr_modified.shape[0])

    profiling(chr_modified, False)

    return chr_modified