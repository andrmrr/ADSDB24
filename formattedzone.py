""" 
Formatted zone

"""

import duckdb
import pandas as pd
import time
import os
from util import *


original_datasets = ['alz_texas_2017.csv' , 'alz_texas_2021.csv', 'chr_texas_2017.csv', 'chr_texas_2021.csv']

"""Insert original datasets into database"""

def insert_datasets(con):

  #data_alz_2017 = pd.read_csv(data_alz_2017_path)
  #data_alz_2021 = pd.read_csv(data_alz_2021_path)
  #data_chr_2017 = pd.read_csv(data_chr_2017_path)
  #data_chr_2021 = pd.read_csv(data_chr_2021_path)

  # Create tables in DuckDB for each of the 4 datasets
  #con.sql(f"CREATE TABLE alzheimer_2017 AS SELECT * FROM data_alz_2017")
  #con.sql(f"CREATE TABLE alzheimer_2021 AS SELECT * FROM data_alz_2021")
  #con.sql(f"CREATE TABLE chronic_2017 AS SELECT * FROM data_chr_2017")
  #con.sql(f"CREATE TABLE chronic_2021 AS SELECT * FROM data_chr_2021")

  # Check which tables are in the database
  table_names = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
  print("Tables in the database:")
  for table in table_names:
      print(table[0])

"""Checking for new datasets in persistent landing zone and updating them to the database"""

# Function to get the latest dataset file in the folder
def get_latest_dataset(folder):
    files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    if files and files not in original_datasets:
      # Get the newest file by creation time
        latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(folder, f)))
        return latest_file
    else:
        return None

# Function for adding new dataset if there is a new one
def add_new_dataset(folder):
  if not os.path.exists(duckdb_formatted):
     # Create the database if it doesn't exist
     pass
  con = duckdb.connect(duckdb_formatted)

  # Check if there's a new dataset
  new_dataset = get_latest_dataset(folder)

  if new_dataset:
      curr_timestamp = time.strftime("%d%m%Y_%H")

      # Adapt the dataset name to existing dataset names -> folder name
      # we are adding it from
      dataset_name = os.path.basename(folder)

      # Create new table name by adding the timestamp to dataset name
      table_name = f"{dataset_name}_{curr_timestamp}"

      dataset_path = os.path.join(folder, new_dataset)
      df = pd.read_csv(dataset_path)

      # Register the dataframe as a temporary table in the database
      con.register('df', df)

      # Create a new table in database
      con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")
      print(f"Table '{table_name}' has been created in the database.")
  else:
      print("No new dataset detected.")
  
  con.close()

def load_to_formatted():
    if not os.path.isdir(os.path.join(base_dir, duckdb_folder)):
      os.mkdir(os.path.join(base_dir, duckdb_folder))

    persistent_dataset_dirs = os.listdir(persistent_landing)
    persistent_dataset_dirs = [os.path.join(persistent_landing, d) for d in persistent_dataset_dirs]
    for ds_dir in persistent_dataset_dirs:
       add_new_dataset(ds_dir)

# """Main for Formatted zone"""

# con = duckdb.connect(duckdb_formatted)

# insert_datasets(con)

# # Automatically checking for new datasets in persistent landing zone
# while True:
#   add_new_dataset(alzheimer_folder)
#   add_new_dataset(chronic_folder)
#   # Check once every day
#   time.sleep(86400)

# """Check tables that are currently in the database"""

# con = duckdb.connect(duckdb_formatted)

# tables = con.execute("SHOW TABLES").fetchall()

# # Print the tables
# print("Tables in the database:")
# for table in tables:
#     print(table[0])

# # Close the connection
# con.close()