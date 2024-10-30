"""
Trusted zone

"""

import duckdb
import pandas as pd
import time
import json
import os
from util import *


"""Keep one table for each data source in the database"""

def integrate_new_tables(con_for, ds):
  # get all tables corresponding to each dataset
  new_tables = []
  if os.path.exists(os.path.join(base_dir, metadata_folder, formatted_metadata)):
    with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "r") as f:
      fmetadata = json.load(f)
      all_new_tables = fmetadata["new_tables"]
      new_tables = [table for table in all_new_tables if table.startswith(dataset1_abbrev(os.path.basename(ds)))]
      all_new_tables = [table for table in all_new_tables if table not in new_tables]

  if len(new_tables) == 0:
     return None
  # no integration needed, just update the metadata
  elif len(new_tables) == 1:
     with open(os.path.join(base_dir, metadata_folder, formatted_metadata), "w") as f:
      json.dump({"new_tables": all_new_tables}, f)
     return con_for.execute("SELECT * FROM ?", [new_tables[0]]).fetchdf()
  
  # if actual integration is needed, i.e. there are multiple new tables
  df = con_for.execute("SELECT * FROM ?", [new_tables[0]]).fetchdf()
  main_schema = set(df.columns)
  for i in range(1, len(new_tables)):
    df2 = con_for.execute("SELECT * FROM ?", [new_tables[i]]).fetchdf()
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
  tru_name = dataset_names[os.path.basename(ds)]
  con_tru.sql(f"CREATE TABLE IF NOT EXISTS ?", [tru_name])
  con_tru.sql(f"""
            INSERT INTO ?
            SELECT * FROM df
        """, [tru_name])
  print(f"Data has been added to the {tru_name} table in the trusted zone.")

"""Main for Trusted Zone"""

def load_to_trusted():
  con_for = duckdb.connect(duckdb_formatted)
  con_tru = duckdb.connect(duckdb_trusted)
  for ds in datasets:
    df = integrate_new_tables(con_for, ds)
    # df = preprocess(df)
    df = add_new_data(con_tru, ds, df)
    df = df.drop_duplicates()
  con_for.close()
  con_tru.close()
