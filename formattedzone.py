# -*- coding: utf-8 -*-
"""FormattedZone"""

import duckdb
import pandas as pd
import json
import os

base_dir = "/content/drive/MyDrive/ADSDB24"
temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
dataset_folder = "datasets_specific"

# 2017 version
# dataset1_fname = "alz_2017.csv"
# dataset2_fname = "chr_2017.csv"
# 2021 version
# dataset1_fname = "alz_2021.csv"
# dataset2_fname = "chr_2021.csv"

# dataset1 = os.path.join(base_dir, dataset_folder, dataset1_fname)
# dataset2 = os.path.join(base_dir, dataset_folder, dataset2_fname)
# dataset1_name = "alzheimer"
# dataset2_name = "chronic_disease_indicators"
# dataset1_abbrev = "alz"
# dataset2_abbrev = "chr"

# datasets = [
#     dataset1, dataset2
# ]

# dataset_names = {
#     dataset1_fname: dataset1_name,
#     dataset2_fname: dataset2_name
# }

# dataset_abbrev = {
#     dataset1_fname: dataset1_abbrev,
#     dataset2_fname: dataset2_abbrev
# }

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

"""Checking for new datasets in persistent landing zone and updating them to the database"""

def get_latest_datasets(folder):
    # get existing tables
    con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
    tables = con.execute("SHOW TABLES").fetchall()
    tables = [table[0] for table in tables]

    # get files from the folder in the landing zone
    files = [f for f in os.listdir(folder) if f.endswith('.csv')]

    # return new files that are not in the formatted database
    new_files = []
    for f in files:
       if f.split(".")[0] not in tables:
        new_files.append(f)
    else:
        return new_files

"""Function for adding new dataset if there is a new one"""

def add_new_dataset(folder):
  # load metadata regarding new files, not yet loaded into the trusted zone
  new_tables_list = []
  if os.path.exists(os.path.join(base_dir, metadata_folder, formatted_metadata)):
    with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "r") as f:
      fmetadata = json.load(f)
      new_tables_list = fmetadata["new_tables"]
    # print(fmetadata)

  con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
  # Check if there's a new dataset
  new_datasets = get_latest_datasets(folder)
  if new_datasets:
    # print(f"New datasets: {new_datasets}")
    for new_dataset in new_datasets:
        table_name = new_dataset.split(".")[0]
        dataset_path = os.path.join(folder, new_dataset)
        df = pd.read_csv(dataset_path)
        # Register the dataframe as a temporary table in the database
        con.register('df', df)
        # Create a new table in database
        con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        new_tables_list.append(table_name)
        print(f"Table '{table_name}' has been created in the database.")
  else:
    print("No new dataset detected.")
  con.close()

  # save updated metadata
  with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "w") as f:
    json.dump({"new_tables": new_tables_list}, f)

"""Main of the formatted zone"""

# create the databases folder if it doesn't exist
if not os.path.isdir(os.path.join(base_dir, duckdb_folder)):
  os.mkdir(os.path.join(base_dir, duckdb_folder))

# create the metadata folder if it doesn't exist
if not os.path.isdir(os.path.join(base_dir, metadata_folder)):
  os.mkdir(os.path.join(base_dir, metadata_folder))

# check each folder in the persistent landing for new datasets
persistent_dataset_dirs = os.listdir(persistent_landing)
persistent_dataset_dirs = [os.path.join(persistent_landing, d) for d in persistent_dataset_dirs]
print(persistent_dataset_dirs)
for ds_dir in persistent_dataset_dirs:
    add_new_dataset(ds_dir)