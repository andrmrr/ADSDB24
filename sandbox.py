# -*- coding: utf-8 -*-
""" Analytical Sandbox """

import pandas as pd
import numpy as np
import duckdb
import json
import os

from util import *

# Load tables from exploitation to sandbox without unnecessary columns
def exp_to_sandbox(con_exp, con_sb):
  tables = con_exp.execute("SHOW TABLES").fetchall()

  for table in tables:
    # print(table[0])

    if table[0].startswith(data_abbrev[0]): #alz
      command = f"""SELECT YearStart, Question, Data_Value AS DataValue,
        StratificationCategoryID2 AS StratificationCategory,
        Stratification2 AS Stratification, Latitude, Longitude
        FROM {table[0]}
        WHERE StratificationID1 = '65PLUS'"""
    elif table[0].startswith(data_abbrev[1]): #chr
      command = f"""SELECT YearStart, Question, DataValue,
        StratificationCategoryID1 AS StratificationCategory,
        Stratification1 AS Stratification, Latitude, Longitude
        FROM {table[0]}
        WHERE DataValueType NOT LIKE 'Age%'"""
    else:
      raise Exception("Table must have one of the following prefix: [alzheimer, chronic_disease_indicators]")

    # print(command)
    df = con_exp.execute(command).fetchdf()
    con_sb.execute(f"CREATE TABLE {table[0]} AS SELECT * FROM df")
    print(f"Created table {table[0]} in the sandbox.")

""" Main for analytical sandbox"""
def load_sandbox():
  con_exp = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))
  con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))

  # Empty the analytical sandbox in order to reload it
  tables = con_sb.execute("SHOW TABLES").fetchall()
  for table in tables:
    print(table[0])
    con_sb.execute(f"DROP TABLE IF EXISTS {table[0]}")

  # Load tables to sandbox and remove unnecessary columns
  exp_to_sandbox(con_exp, con_sb)

  con_sb.close()
  con_exp.close()
