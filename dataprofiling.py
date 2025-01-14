"""
DataProfiling

"""

import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

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