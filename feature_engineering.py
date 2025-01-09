import pandas as pd
import numpy as np

import duckdb
import json
import os

from util import *

target_question = "Percentage of older adults who report having a disability \
(includes limitations related to sensory or mobility impairments or a physical, mental, or emotional condition)"
target_table_name = "alzheimer_disability_status_including_sensory_or_mobility_limitations"
test_table_name = "chronic_disease_indicators_diabetes"

# Fix float precision
def set_float_precision(df, cols, N=100000):
  for col in cols:
    df[col] = np.round(df[col]*N).astype(int)
    df[col] = df[col] / N

  return df

# Reconcile stratificators
def reconcile_stratificators(df):
  def mapping(s): # Hispanic, Female, Male are the same
        if s == 'Asian/Pacific Islander' or s == 'Asian or Pacific Islander' or s == 'Asian, non-Hispanic': # drop ili u other   alz
              # return 'Asian_or_Pacific_Islander'
              return "Other"
        elif s == 'Native Am/Alaskan Native' or s == 'American Indian or Alaska Native': # drop   alz
              # return 'American_Indian_or_Alaska_Native'
              return "Other"
        elif s == 'White, non-Hispanic':
              return 'White'
        elif s == 'Black, non-Hispanic':
              return 'Black'
        elif s == "Multiracial, non-Hispanic": # drop ili u other     chr
              # return "Multiracial"
              return "Other"
        elif  s == "Other, non-Hispanic": # drop ili u other   chr
              return "Other"
        elif s == "NONE":
              return "Overall"
        else:
              return s


  df["Stratification"] = df["Stratification"].astype(str).map(mapping)
  df = df[df["Stratification"] != "Other"] # removing other, because of multiple rows with same year,location,stratification
  return df

# Combine tables with the target question
def combine_with_target(target_df, question_df, question_cnt):
  question_label = f"Q{question_cnt}"

  # add question to the metadata
  questions = dict()
  with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), "r") as f:
    fmetadata = json.load(f)
    questions = fmetadata["questions"]
  questions[question_label] = question_df.iloc[0]["Question"]
  with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), "w") as f:
    json.dump({"questions": questions}, f)

  # add question data value to target_df
  question_df = question_df.rename(columns={"DataValue" : question_label})
  question_df = question_df.drop(["Question"], axis=1)
  target_df = pd.merge(target_df, question_df,
                       on=["YearStart", "Latitude", "Longitude", "StratificationCategory", "Stratification"], how="inner")

  return target_df

""" Main """
def feature_engineering_execute():
    con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))

    # Target table
    target_df = con_sb.execute(f"SELECT * FROM {target_table_name}").fetchdf()
    target_df = set_float_precision(target_df, ["Latitude", "Longitude"])
    target_df = reconcile_stratificators(target_df)
    target_df = target_df.rename(columns={"DataValue" : "QTarget"})
    with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), "w") as f:
        json.dump({"questions": {"QTarget" : target_df.iloc[0]["Question"]}}, f)
    target_df = target_df.drop(["Question"], axis=1)

    # Combine the tables with the target question
    tables = con_sb.execute("SHOW TABLES").fetchall()
    question_cnt = 1
    for table in tables:
        df = con_sb.execute(f"SELECT * FROM {table[0]}").fetchdf()
        df = set_float_precision(df, ["Latitude", "Longitude"])
        df = reconcile_stratificators(df)
        questions_df_dict = dict(tuple(df.groupby("Question")))
        for question_df in questions_df_dict.items():
            target_df = combine_with_target(target_df, question_df, question_cnt)
            question_cnt += 1
        # remove the table after it has been combined with the target
        con_sb.execute(f"DROP TABLE IF EXISTS {table[0]}")

    # Save final combined table
    final_table_name = "combined_analytical_table"
    con_sb.register("temp_table", target_df)
    con_sb.execute(f"CREATE TABLE {final_table_name} AS SELECT * FROM temp_table")

    con_sb.close()
    target_df.to_csv(os.path.join(base_dir, "all_questions.csv"))