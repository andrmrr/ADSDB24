"""
Trusted zone

"""

import duckdb
import pandas as pd
import time
import os
from util import *

# base_dir = "/content/drive/MyDrive/ADSDB24"
# temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
# persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
# duckdb_trusted = os.path.join(base_dir, "duckdb_database", "formatted_zone.db")

# alzheimer_folder = os.path.join(base_dir, "landing_zone/persistent/alzheimer")
# chronic_folder = os.path.join(base_dir, "landing_zone/persistent/chronic_disease_indicators")

# datasource1 = os.path.join(base_dir, "datasets_original/Alzheimer_s_Disease_and_Healthy_Aging_Data.csv")
# datasource1_name = "alzheimer"
# datasource2 = os.path.join(base_dir, "datasets_original/U.S._Chronic_Disease_Indicators__CDI___2023_Release.csv")
# datasource2_name = "chronic_disease_indicators"


"""Checking which datasets are in the database"""

con = duckdb.connect(duckdb_trusted)

# Query to get all table names
table_names = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()

# Extract table names from the result and print them
print("Tables in the database:")
for table in table_names:
    print(table[0])

# Close the connection
con.close()

"""Create new database for Trusted zone"""

def create_db_tru_zone():
  # File path for new database
  duckdb_truzone = os.path.join(base_dir, "duckdb_database/trusted_zone.db")

  # Connect new database and create it
  con = duckdb.connect(duckdb_truzone)

  # Initialize the database by creating dummy table
  con.execute("CREATE TABLE sample_tru_table (id INTEGER, name VARCHAR)")

  con.close()

  return duckdb_truzone

"""Keep one table for each data source in the database"""

def integrate_tables(con_for, con_tru):
  # Getting tables from formatted zone
  alz_2017 = con_for.execute("SELECT * FROM alzheimer_2017").fetchdf()
  alz_2021 = con_for.execute("SELECT * FROM alzheimer_2021").fetchdf()
  chr_2017 = con_for.execute("SELECT * FROM chronic_2017").fetchdf()
  chr_2021 = con_for.execute("SELECT * FROM chronic_2021").fetchdf()

  # Add alz_2021 data to alz_2017
  con_tru.sql("""
    INSERT INTO alzheimer_2017
    SELECT * FROM alzheimer_2021
  """)
  print("Data from 'alzheimer_2021' has been added into 'alzheimer_2017'.")

  # Rename table to alzheimer
  con_tru.sql("ALTER TABLE alz_2017 RENAME TO alzheimer")

  # Add chr_2021 data to chr_2017
  con_tru.sql("""
    INSERT INTO chronic_2017
    SELECT * FROM chronic_2021
  """)
  print("Data from 'chronic_2021' has been added into 'chronic_2017'.")

  # Rename table to chronic_diseases
  con_tru.sql("ALTER TABLE chronic_2017 RENAME TO chronic_diseases")

  # Return main 2 tables
  return alz_2017, chr_2017

"""Drop 2021 tables from database (MISLIM DA OVO NE TREBA)"""

def drop_additional_tables(con):
  # Drop table alzheimer_2021
  con.sql("DROP TABLE IF EXISTS alzheimer_2021")

  # Drop table chronic_2021
  con.sql("DROP TABLE IF EXISTS chronic_2021")

"""Load new tables into base table in database"""

def load_tables(con):

  # Find new tables in the database
  tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
  tables = [t[0] for t in tables]
  new_tables = [table for table in tables if table not in ['alzheimer', 'chronic_diseases']]

  # Figure out which base table to add it to
  for table in new_tables:
    if 'alzheimer' in table.lower():
        # Insert data from the new table into alzheimer table
        con.sql(f"""
            INSERT INTO alzheimer
            SELECT * FROM {table}
        """)
        print(f"Data from '{table}' has been added to 'alzheimer'.")
    elif 'chronic' in table.lower():
        # Insert data from the new table into chronic_diseases table
        con.sql(f"""
            INSERT INTO chronic_diseases
            SELECT * FROM {table}
        """)
        print(f"Data from '{table}' has been added to 'chronic_diseases'.")
    else:
        raise ValueError(f"No base table detected")

    # Drop the table after appending
    con.sql(f"DROP TABLE {table}")
    print(f"Table '{table}' has been deleted from the database.")

"""Main for Trusted Zone"""

# File path for new database in trusted zone
duckdb_truzone = create_db_tru_zone()

con_for = duckdb.connect(duckdb_trusted)
con_tru = duckdb.connect(duckdb_truzone)

alzheimer, chronic_diseases = integrate_tables(con_for, con_tru)

# Automatically checking for new tables in the database to attach them to
# respective original table
while True:
  load_tables(con_tru)
  # Check once every day
  time.sleep(86400)

con_for.close()
con_tru.close()

"""Data quality processes"""

def data_quality_pipeline():
  pass

def profiling(df):
  print(df[:4])
  print(df.columns)

profiling(load_trusted("alzheimer_16102024_18"))

def remove_duplicates():
  pass

def handle_missing_values():
  pass

def normalization():
  pass

def handle_data_mistakes():
  pass

def format_consistency():
  pass