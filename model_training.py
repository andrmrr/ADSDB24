# -*- coding: utf-8 -*-
"""model_training"""

import pandas as pd
import numpy as np
import duckdb
import json
import os

# import seaborn as sns
# from matplotlib import pyplot as plt
from time import time
from datetime import timedelta
import pickle as pkl
from joblib import dump

# from sklearn.model_selection import train_test_split, KFold, cross_validate,
from sklearn.model_selection import GridSearchCV
# from sklearn.metrics import confusion_matrix, accuracy_score, classification_report, f1_score
# from sklearn.naive_bayes import GaussianNB
# from sklearn.linear_model import LogisticRegressionCV, LogisticRegression
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.metrics import make_scorer, mean_squared_error, r2_score
# from sklearn.neural_network import MLPClassifier
# from graphviz import Digraph

# from sklearn.metrics import confusion_matrix, \
                  # classification_report, accuracy_score,  precision_score, recall_score, f1_score

base_dir = "/content/drive/MyDrive/ADSDB24"
temporal_landing = os.path.join(base_dir,  "landing_zone/temporal")
persistent_landing = os.path.join(base_dir, "landing_zone/persistent")
dataset_folder = "datasets_specific"

"""DuckDB"""
duckdb_folder = "duckdb_database"
duckdb_formatted = os.path.join(duckdb_folder, "formatted_zone.db")
duckdb_trusted = os.path.join(duckdb_folder, "trusted_zone.db")
duckdb_exploitation = os.path.join(duckdb_folder, "exploitation_zone.db")
duckdb_sandbox = os.path.join(duckdb_folder, "sandbox_zone.db")

"""Metadata"""
metadata_folder = "metadata"
formatted_metadata = "formatted_metadata.json"
alz_metadata_fname = "alzheimer_metadata.json"
chr_metadata_fname = "chronic_disease_indicators_metadata.json"
analytical_sandbox_metadata = "analytical_sandbox_metadata.json"

data_abbrev = ["alz", "chr"]

"""### Load train and test data"""

con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
tables = con_sb.execute("SHOW TABLES").fetchall()
for table in tables:
  print(table[0])
con_sb.close()

con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
train = con_sb.execute("SELECT * FROM train_data").fetchdf()
test = con_sb.execute("SELECT * FROM test_data").fetchdf()
con_sb.close()

print(train.shape)
print(test.shape)

"""### Split data into predictors and target"""

# train
trainY = train["QTarget"]
trainX = train.drop(["QTarget"], axis=1)
print(trainX.shape)
print(trainY.shape)
# test
testY = test["QTarget"]
testX = test.drop(["QTarget"], axis=1)
print(testX.shape)
print(testY.shape)

print(trainX.isnull().sum())
print(trainY.isnull().sum())

print(trainY)

"""### Train extra-trees clasifier

Extra Trees is a faster version of Random Forest \\
Extra Trees hyperparameters:


1.   Criterion: *The function to measure the quality of a split. Supported criteria are “squared_error” for the mean squared error, which is equal to variance reduction as feature selection criterion and minimizes the L2 loss using the mean of each terminal node, “friedman_mse”, which uses mean squared error with Friedman’s improvement score for potential splits, “absolute_error” for the mean absolute error, which minimizes the L1 loss using the median of each terminal node, and “poisson” which uses reduction in Poisson deviance to find splits. Training using “absolute_error” is significantly slower than when using “squared_error”.
2.   n_estimators: *The number of trees in the forest.*
3.   max_depth: *The maximum depth of the tree. If None, then nodes are expanded until all leaves are pure or until all leaves contain less than min_samples_split samples.*
The deeper the tree, the more complex the model will become because there are more splits and it captures more information about the data and this is one of the root causes of overfitting. So, if the model is overfitting, reducing the number for max_depth is one way to combat overfitting. A very low depth will underfit sohow to find the best value.
4.   min_samples_split: *The minimum number of samples required to split an internal node*
5.   min_samples_leaf: *The minimum number of samples required to be at a leaf node. A split point at any depth will only be considered if it leaves at least min_samples_leaf training samples in each of the left and right branches. This may have the effect of smoothing the model, especially in regression.*
Let's say we have a min_samples_split and the resulting split results in a leaf with 1 sample and you have specified min_samples_leaf as 2, then the min_samples_split will not be allowed. In other words, min_samples_leaf is always guaranteed no matter the min_samples_split value.
6.   max_features: *The number of features to consider when looking for the best split*.   We have 14 different features
7.   bootstrap: *Whether bootstrap samples are used when building trees. If False, the whole dataset is used to build each tree.*
The default behaviour of ExtraTrees is to not boostrap and that is one of its main differences from RandomForest. In an ExtraTreesRegressor, we are drawing observations without replacement, so we will not have repetition of observations like in random forest. Setting it to True, makes its behavior more similar to RandomForest
"""

extress = ExtraTreesRegressor(random_state=17)

scoring_dict = {
    'mse': make_scorer(mean_squared_error, greater_is_better=False),
    'r2': 'r2',
}

init_time = time()
extress_cv = GridSearchCV(estimator=extress,
                   scoring=scoring_dict,
                   param_grid={
                       'n_estimators': [100, 500, 1000],
                       'criterion': ['squared_error'],
                       'max_depth': [2,4,6] + [None],
                       'min_samples_split': [2,4,6,8,10,12],
                       'min_samples_leaf': [1, 3, 5],
                      #  'max_features': ['sqrt', 'log2', None],
                   },
                   cv=5,
                   return_train_score=False,
                   refit='mse',
                   error_score='raise',
                   n_jobs=-1)
extress_5CV = extress_cv.fit(trainX, trainY)
print(timedelta(seconds=(time() - init_time)))

best_params = extress_5CV.best_params_
best_params

models_dir = "models"
with open(os.path.join(base_dir, models_dir, "ExtraTrees_bestparams.txt"), "w") as f:
  f.write(json.dumps(best_params))
dump(extress_5CV, os.path.join(base_dir, models_dir, "ExtraTrees_best.joblib"))

scoring_cols = [
    'param_n_estimators', 'param_criterion', 'param_max_depth', 'param_min_samples_split', 'param_min_samples_leaf', 'mean_test_mse', 'mean_test_r2'
]

"""### Top 5 parameter combinations ordered by mse and r2"""

pd.DataFrame(extress_5CV.cv_results_).sort_values(by='mean_test_mse', ascending=False)[scoring_cols].head()

results = extress_cv.cv_results_
print(pd.DataFrame(results))

"""# Estimation"""

y_pred = extress_5CV.predict(testX)
print(type(y_pred))
print(type(testY))

# results_df = compute_metrics(y_val, y_pred, labels, "ExtrTrees-best", results_df)
# results_df.sort_values(by='F1_Avg', ascending=False,inplace=True)
# results_df

mse_test = mean_squared_error(testY, y_pred)
r2_test = r2_score(testY, y_pred)

print(f"MSE on test set: {mse_test}")
print(f"R2 on test set: {r2_test}")

"""This is the standard deviation of the target variable"""

all_data = pd.concat([train, test])
all_data["QTarget"].std()