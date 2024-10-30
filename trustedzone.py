# -*- coding: utf-8 -*-
"""TrustedZone.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1nWE6PA3ZCfN6JpFL7skYrbRaQ6sl8Opo
"""

from google.colab import drive
drive.mount('/content/drive')

pip install pyspellchecker

!pip install --quiet duckdb-engine
!pip install --quiet pandas

"""# Trusted zone


"""

import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import time
import datetime
import os
import json

from spellchecker import SpellChecker

base_dir = "/content/drive/MyDrive/ADSDB24"
temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
if not os.path.isdir(os.path.join(base_dir, "metadata")):
  os.mkdir(os.path.join(base_dir, "metadata"))
alz_metadata_path = os.path.join(base_dir, "metadata", "alzheimer_metadata.json")
chr_metadata_path = os.path.join(base_dir, "metadata", "chronic_disease_indicators_metadata.json")
duckdb_db_trusted = os.path.join(base_dir, "duckdb_database", "trusted_zone_test.db")
duckdb_db_formatted = os.path.join(base_dir, "duckdb_database", "formatted_zone.db")

datasource1 = os.path.join(base_dir, "datasets_original/Alzheimer_s_Disease_and_Healthy_Aging_Data.csv")
datasource1_name = "alzheimer"
datasource2 = os.path.join(base_dir, "datasets_original/U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")
datasource2_name = "chronic_disease_indicators"

datasource1tmp = os.path.join(base_dir, "tmp_dir/Air_Quality.csv")
datasource2tmp = os.path.join(base_dir, "tmp_dir/Death_rates_for_suicide__by_sex__race__Hispanic_origin__and_age__United_States.csv")

datasources = {
    "Air_Quality.csv": datasource1_name,
    "Death_rates_for_suicide__by_sex__race__Hispanic_origin__and_age__United_States.csv": datasource2_name,
}

"""Temporary load mock data to table"""

# # Function to get the latest dataset file in the folder
# def get_latest_dataset(folder):
#     files = [f for f in os.listdir(folder) if f.endswith('.csv')]
#     if files:
#       # Get the newest file by creation time
#         latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(folder, f)))
#         return latest_file
#     else:
#         return None

# # Function for adding new dataset if there is a new one
# def add_new_dataset(ds):
#   with duckdb.connect(duckdb_db_trusted) as con:

#       curr_timestamp = time.strftime("%d%m%Y_%H")
#       # Parse the dataset name (remove the .csv extension)
#       dataset_name = datasources[ds]

#       # Create new table name by adding the timestamp to dataset name
#       table_name = f"{dataset_name}_{curr_timestamp}"

#       # dataset_path = os.path.join(persistent_landing, dataset_name, ds)
#       dataset_path = os.path.join(base_dir, "tmp_dir", ds)
#       df = pd.read_csv(dataset_path)

#       # Create a new table in database
#       con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")
#       print(f"Table '{table_name}' has been created in the database.")

# add_new_dataset("Air_Quality.csv")

"""Checking for new datasets in persistent landing zone and updating them to the database"""

with duckdb.connect(duckdb_db_formatted) as con:
  con.sql("SHOW ALL TABLES").show()

"""##Data ingestion"""

# TODO Proveri da li ima novih tabela u formatted zone
def load_trusted(table):
  with duckdb.connect(duckdb_db_formatted) as con:
    df = con.sql("SELECT * FROM " + table).df()
    # print(df.info())
    # con.sql("SELECT * FROM alzheimer_16102024_18").show()
    return df



alz = load_trusted("alzheimer")
chr = load_trusted("chronic_diseases")

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

"""## Data Profiling"""

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

"""#Alzheimer"""

profiling(alz, True)

#drop the first column
alz_modified = alz.drop("Unnamed: 0", axis=1)
alz_modified.columns

"""##Missing values"""

for col in alz_modified.columns:
  has_null = alz_modified[col].isnull().any()
  if has_null:
    print(f"{col}: {alz_modified[col].isnull().sum()}")

# impute missing values for Data_Value, Data_Value_Alt, Low_Confidence_Limit and High_Confidence_Limit with -1
alz_modified = alz
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

alz_modified["Data_Value_Footnote"].fillna("NONE", inplace=True)

print(alz_modified["StratificationCategory2"].unique())
print(alz_modified["Stratification2"].unique())

alz_modified["StratificationCategory2"].fillna("NONE", inplace=True)
alz_modified["Stratification2"].fillna("NONE", inplace=True)

"""##Removing redundant features"""

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
# print(alz['RowId'].unique())
print(alz_metadata)

ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
alz_metadata_full_path = os.path.join(alz_metadata_path.split(".")[0] + ts + "." + alz_metadata_path.split(".")[1])
with open(alz_metadata_full_path, "w") as f:
    json.dump(alz_metadata, f, cls=NpEncoder)

alz_modified.drop(["LocationAbbr", "LocationDesc", "Datasource", "Geolocation", "LocationID"], axis=1, inplace=True)
print(alz_modified.columns)

"""We get the same value in all rows for the following columns with following values: LocationAbbr - TX, LocationDesc - Texas, Datasource - BRFSS, StratificationCategory1 - Age Group, Geolocation - POINT(-99.42677021 31.82724041), LocationID - 48 and StratificationCategoryID1 - AGE.

Location related features are chosen in advance, so we can take them out and save the static values, which hold true for the entire dataset. Datasource will also be moved to metadata and removed from the dataset.

We will leave the stratification features in for now.

##Feature type conversion
"""

# convert numerical to categorical
alz_modified["YearStart"] = alz_modified["YearStart"].astype('category')
alz_modified["YearEnd"] = alz_modified["YearEnd"].astype('category')

"""Maybe we should combine YearStart and YearEnd into a single column

##Check for duplicates
"""

# check for duplicates
print(alz_modified.shape[0])
alz_modified.drop_duplicates(inplace=True)
print(alz_modified.shape[0])

"""I feel that no normalization/scaling is necessary for this dataset

Not sure how to yet how to handle spelling errors
"""

# spell = SpellChecker()
# misspelled = spell.unknown(air[air.columns[7]])
# print(len(misspelled))
# print(misspelled)

# # for word in misspelled:
# #     # Get the one `most likely` answer
# #     print(spell.correction(word))

# #     # Get a list of `likely` options
# #     print(spell.candidates(word))

profiling(alz_modified, False)

"""# Chronic diseases dataset"""

profiling(chr, True)

chr_modified = chr.drop("Unnamed: 0", axis=1)

"""##Missing values"""

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

"""##Feature type conversion"""

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

"""##Remove redundant features"""

# check if Data_Value and Data_Value_Alt are identical, and if so remove Data_Value_Alt
if (chr_modified["DataValue"] == chr_modified["DataValueAlt"]).all():
  chr_modified.drop("DataValue_Alt", axis=1, inplace=True)

for col in chr_modified.columns:
  has_null = chr_modified[col].isnull().any()
  if has_null:
    print(f"{col}: {chr_modified[col].isnull().sum()}")

chr_metadata = {}
for col in chr_modified.columns:
  unique_count = len(chr_modified[col].unique())
  if unique_count <= 1:
    print(f"{col}: {unique_count}")
    print(f"---> Value: {chr_modified[col][0]}")
    chr_metadata[col] = chr_modified[col][0]
# print(chr['RowId'].unique())
print(chr_metadata)

ts = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
chr_metadata_full_path = os.path.join(chr_metadata_path.split(".")[0] + ts + "." + chr_metadata_path.split(".")[1])
with open(chr_metadata_full_path, "w") as f:
    json.dump(chr_metadata, f, cls=NpEncoder)

chr_modified.drop(["LocationAbbr", "LocationDesc", "GeoLocation", "LocationID"], axis=1, inplace=True)

print(chr_modified.columns)

"""##Check for duplicates"""

# check for duplicates
print(chr_modified.shape[0])
chr_modified.drop_duplicates(inplace=True)
print(chr_modified.shape[0])

profiling(chr_modified, False)