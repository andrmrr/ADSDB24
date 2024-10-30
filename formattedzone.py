""" 
Formatted zone

"""

import duckdb
import pandas as pd
import json
import os
from util import *


"""Checking for new datasets in persistent landing zone and updating them to the database"""

def get_latest_datasets(folder):
    # get existing tables
    con = duckdb.connect(duckdb_formatted)
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

""" Function for adding new dataset if there is a new one """

def add_new_dataset(folder):
  # load metadata regarding new files, not yet loaded into the trusted zone
  new_tables_list = []
  if os.path.exists(os.path.join(base_dir, metadata_folder, formatted_metadata)):
    with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "r") as f:
      fmetadata = json.load(f)
      new_tables_list = fmetadata["new_tables"]
    # print(fmetadata)

  con = duckdb.connect(duckdb_formatted)
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


"""Main for Formatted zone"""

def load_to_formatted():
    # create the databases folder if it doesn't exist
    if not os.path.isdir(os.path.join(base_dir, duckdb_folder)):
      os.mkdir(os.path.join(base_dir, duckdb_folder))

    # create the metadata folder if it doesn't exist
    if not os.path.isdir(os.path.join(base_dir, metadata_folder)):
      os.mkdir(os.path.join(base_dir, metadata_folder))
    
    # check each folder in the persistent landing for new datasets
    persistent_dataset_dirs = os.listdir(persistent_landing)
    persistent_dataset_dirs = [os.path.join(persistent_landing, d) for d in persistent_dataset_dirs]
    for ds_dir in persistent_dataset_dirs:
       add_new_dataset(ds_dir)


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