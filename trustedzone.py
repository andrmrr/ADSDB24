# -*- coding: utf-8 -*-
"""TrustedZone"""
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb

import datetime
import os
import json

base_dir = "/content/drive/MyDrive/ADSDB24"
temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
dataset_folder = "datasets_specific"

# # 2017 version
dataset1_fname = "alz_2017.csv"
dataset2_fname = "chr_2017.csv"
# 2021 version
# dataset1_fname = "alz_2021.csv"
# dataset2_fname = "chr_2021.csv"

dataset1 = os.path.join(base_dir, dataset_folder, dataset1_fname)
dataset2 = os.path.join(base_dir, dataset_folder, dataset2_fname)
dataset1_name = "alzheimer"
dataset2_name = "chronic_disease_indicators"
dataset1_abbrev = "alz"
dataset2_abbrev = "chr"

datasets = [
    dataset1, dataset2
]

dataset_names = {
    dataset1_fname: dataset1_name,
    dataset2_fname: dataset2_name
}

dataset_abbrev = {
    dataset1_fname: dataset1_abbrev,
    dataset2_fname: dataset2_abbrev
}

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

con_for = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
tables = con_for.execute("SHOW TABLES").fetchall()
for table in tables:
  print(table[0])

"""
Integrate incoming tables into one"""

def integrate_new_tables(con_for, ds):
  # get all tables corresponding to each dataset
  new_tables = []
  if os.path.exists(os.path.join(base_dir, metadata_folder, formatted_metadata)):
    with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "r") as f:
      fmetadata = json.load(f)
      all_new_tables = fmetadata["new_tables"]
      new_tables = [table for table in all_new_tables if table.startswith(dataset_abbrev[os.path.basename(ds)])]
      all_new_tables = [table for table in all_new_tables if table not in new_tables]

  if len(new_tables) == 0:
     return None
  # no integration needed, just update the metadata
  elif len(new_tables) == 1:
    with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "w") as f:
      json.dump({"new_tables": all_new_tables}, f)
      tb = new_tables[0]
      return con_for.execute(f"SELECT * FROM {new_tables[0]}").fetchdf()

  # if actual integration is needed, i.e. there are multiple new tables
  df = con_for.execute(f"SELECT * FROM {new_tables[0]}").fetchdf()
  for i in range(1, len(new_tables)):
    main_schema = set(df.columns)
    df2 = con_for.execute(f"SELECT * FROM {new_tables[i]}").fetchdf()
    schema2 = set(df2.columns)

    #check if the schemas match and add missing columns
    if main_schema != schema2:
      missing_main_schema = schema2.difference(main_schema)
      for col in missing_main_schema:
        df[col] = pd.Series(dtype=df2.dtypes[col])
      missing_schema2 = main_schema.difference(schema2)
      for col in missing_schema2:
        df2[col] = pd.Series(dtype=df.dtypes[col])

    # append the new dataframe to the main one
    df = pd.concat([df, df2], axis=0, ignore_index=True)

  # update the metadata
  with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "w") as f:
    json.dump({"new_tables": all_new_tables}, f)

  return df

"""Combine integrated and preprocessed tables to the existing table in the trusted zone"""

def add_new_data(con_tru, ds, df):
  tables = con_tru.execute("SHOW TABLES").fetchall()
  tables = [table[0] for table in tables]
  tru_name = dataset_names[os.path.basename(ds)]
  if tru_name not in tables:
    con_tru.sql(f"CREATE TABLE {tru_name} AS SELECT * FROM df")
  else:
    con_tru.sql(f"""
              INSERT INTO {tru_name}
              SELECT * FROM df
          """)
  print(f"Data has been added to the {tru_name} table in the trusted zone.")
  return con_tru.execute(f"SELECT * FROM {tru_name}").fetchdf()

"""#Preprocessing

Profiling
"""

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

"""## Alzheimer"""

# con_for = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
# alz = integrate_new_tables(con_for, dataset1)
# print(alz.columns)
# con_for.close()

def alz_preprocess(alz):
  # profiling(alz, True)
  alz_metadata_path = os.path.join(base_dir, metadata_folder, alz_metadata_fname)
  #drop the first column
  # alz_modified = alz.drop("Unnamed: 0", axis=1)
  alz_modified = alz.drop("RowId", axis=1)

  """Missing values"""

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

  # impute string "NONE" for missing values of Data_Value_Footnote
  alz_modified["Data_Value_Footnote"].fillna("NONE", inplace=True)

  print(alz_modified["StratificationCategory2"].unique())
  print(alz_modified["Stratification2"].unique())

  # impute string "NONE" for missing values of StratificationCategory2, Stratification2
  alz_modified["StratificationCategory2"].fillna("NONE", inplace=True)
  alz_modified["Stratification2"].fillna("NONE", inplace=True)

  """Feature type conversion"""

  # convert numerical to categorical
  alz_modified["YearStart"] = alz_modified["YearStart"].astype('category')
  alz_modified["YearEnd"] = alz_modified["YearEnd"].astype('category')

  """Removing redundant features"""

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
  for col in alz_modified.columns:
      unique_count = len(alz_modified[col].unique())
      if unique_count <= 1:
          print(f"{col}: {unique_count}")
          print(f"---> Value: {alz_modified[col][0]}")
          alz_metadata[col] = alz_modified[col][0]
          alz_modified = alz_modified.drop([col], axis=1)

      print(alz_metadata)

  ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
  alz_metadata_path, alz_metadata_ext = alz_metadata_path.split(".")[:-1], alz_metadata_path.split(".")[-1]
  alz_metadata_path = ".".join(alz_metadata_path)
  alz_metadata_full_path = os.path.join(alz_metadata_path + ts + "." + alz_metadata_ext)
  with open(alz_metadata_full_path, "w") as f:
      json.dump(alz_metadata, f, cls=NpEncoder)

  # Handle location
  # There are 4 location-based features that reference the same thing. We keep LocationDesc (state name) and Geolocation as latitude and longitude
  alz_modified.drop(["LocationAbbr", "LocationID"], axis=1, inplace=True)
  alz_modified["Longitude"] = alz_modified["Geolocation"].str.extract(r"^POINT \((-?\d+\.\d+) -?\d+\.\d+\)$")
  alz_modified["Latitude"] = alz_modified["Geolocation"].str.extract(r"^POINT \(-?\d+\.\d+ (-?\d+\.\d+)\)$")
  alz_modified["Longitude"] = alz_modified["Longitude"].astype("float64")
  alz_modified["Latitude"] = alz_modified["Latitude"].astype("float64")
  alz_modified = alz_modified.drop(["Geolocation"], axis=1)

  # alz_modified.drop(["LocationAbbr", "LocationDesc", "Geolocation", "LocationID"], axis=1, inplace=True)
  # print(alz_modified.columns)
  # """We get the same value in all rows for the following columns with following values: LocationAbbr - TX, LocationDesc - Texas, StratificationCategory1 - Age Group, Geolocation - POINT(-99.42677021 31.82724041), LocationID - 48 and StratificationCategoryID1 - AGE.
  # Location related features are chosen in advance, so we can take them out and save the static values, which hold true for the entire dataset. Datasource will also be moved to metadata and removed from the dataset.
  # We will leave the stratification features in for now.
  # """

  """Check for duplicates"""

  print(alz_modified.shape[0])
  alz_modified.drop_duplicates(inplace=True)
  print(alz_modified.shape[0])
  print(alz_modified.columns)

  """No normalization, scaling or spellchecking is necessary for this dataset"""
  # profiling(alz_modified, False)
  return alz_modified

"""## Chronic disease indicators"""

con_for = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
chr = integrate_new_tables(con_for, dataset2)
print(chr.columns)
con_for.close()

def chr_preprocess(chr):
    # profiling(chr, True)
    chr_metadata_path = os.path.join(base_dir, metadata_folder, chr_metadata_fname)
    #drop the first column
    # chr_modified = chr.drop("Unnamed: 0", axis=1)
    chr_modified = chr.copy()

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

    """Feature type conversion"""

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
            chr_modified = chr_modified.drop([col], axis=1)
    print(chr_metadata)

    ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    chr_metadata_path, chr_metadata_ext = chr_metadata_path.split(".")[:-1], chr_metadata_path.split(".")[-1]
    chr_metadata_path = ".".join(chr_metadata_path)
    chr_metadata_full_path = os.path.join(chr_metadata_path + ts + "." + chr_metadata_ext)
    with open(chr_metadata_full_path, "w") as f:
        json.dump(chr_metadata, f, cls=NpEncoder)

    # There are 4 location-based features that reference the same thing. We keep LocationDesc (state name) and Geolocation as latitude and longitude
    chr_modified.drop(["LocationAbbr", "LocationID"], axis=1, inplace=True)
    chr_modified["Longitude"] = chr_modified["GeoLocation"].str.extract(r"^POINT \((-?\d+\.\d+) -?\d+\.\d+\)$")
    chr_modified["Latitude"] = chr_modified["GeoLocation"].str.extract(r"^POINT \(-?\d+\.\d+ (-?\d+\.\d+)\)$")
    chr_modified["Longitude"] = chr_modified["Longitude"].astype("float64")
    chr_modified["Latitude"] = chr_modified["Latitude"].astype("float64")
    chr_modified = chr_modified.drop(["GeoLocation"], axis=1)

    # chr_modified.drop(["LocationAbbr", "LocationDesc", "GeoLocation", "LocationID"], axis=1, inplace=True)
    # print(chr_modified.columns)
    # """We get the same value in all rows for the following columns with following values: LocationAbbr - TX, LocationDesc - Texas, StratificationCategory1 - Age Group, Geolocation - POINT(-99.42677021 31.82724041), LocationID - 48 and StratificationCategoryID1 - AGE.
    # Location related features are chosen in advance, so we can take them out and save the static values, which hold true for the entire dataset. Datasource will also be moved to metadata and removed from the dataset.
    # We will leave the stratification features in for now.
    # """

    """Check for duplicates"""

    print(chr_modified.shape[0])
    chr_modified.drop_duplicates(inplace=True)
    print(chr_modified.shape[0])

    # profiling(chr_modified, False)

    return chr_modified

"""Preprocess the data - conduct Data Quality processes"""

def preprocess(ds, df):
  if dataset_names[os.path.basename(ds)] == dataset1_name:
    return alz_preprocess(df)
  elif dataset_names[os.path.basename(ds)] == dataset2_name:
    return chr_preprocess(df)
  else:
    raise Exception(f"Unknown dataset: {ds}")

"""Main for Trusted Zone"""

con_for = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
con_tru = duckdb.connect(os.path.join(base_dir, duckdb_trusted))
for ds in datasets:
  df = integrate_new_tables(con_for, ds)
  if df is None:
    continue
  df = preprocess(ds, df)
  if df is None:
    continue
  df = add_new_data(con_tru, ds, df)
  df = df.drop_duplicates()
con_for.close()
con_tru.close()

con_tru = duckdb.connect(os.path.join(base_dir, duckdb_trusted))
tables = con_tru.execute("SHOW TABLES").fetchall()
for table in tables:
  print(table[0])
con_tru.close()

"""## For testing"""

con_tru = duckdb.connect(os.path.join(base_dir, duckdb_trusted))
tables = con_tru.execute("SHOW TABLES").fetchall()
for table in tables:
  df = con_tru.execute(f"SELECT * FROM {table[0]}").fetchdf()
  print(df.columns)
con_tru.close()

# con_tru = duckdb.connect(os.path.join(base_dir, duckdb_trusted))
# tables = con_tru.execute("SHOW TABLES").fetchall()
# for table in tables:
#   print(table[0])
#   con_tru.execute(f"DROP TABLE IF EXISTS {table[0]}")
# con_tru.close()

# json_items = ["chr_2017_2024_12_17_22_34_57", "chr_2021_2024_12_17_22_36_03", "alz_2017_2024_12_17_22_34_57", "alz_2021_2024_12_17_22_36_03"]
# with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "w") as f:
#       json.dump({"new_tables": json_items}, f)

# with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "r") as f:
#   fmetadata = json.load(f)
#   print(fmetadata)