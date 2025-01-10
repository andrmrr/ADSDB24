""" Model Training """

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

# import seaborn as sns
# from matplotlib import pyplot as plt
from time import time
from datetime import timedelta
import pickle as pkl
from joblib import dump

from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.metrics import make_scorer, mean_squared_error, r2_score

from util import *

# Function to connect to the sandbox database and load data
def load_data():
    con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))
    train = con_sb.execute("SELECT * FROM train_data").fetchdf()
    test = con_sb.execute("SELECT * FROM test_data").fetchdf()
    con_sb.close()
    print(train.shape)
    print(test.shape)
    return train, test

# Split data into predictors and target
def split_data(train, test):
    # train
    trainY = train["QTarget"]
    trainX = train.drop(["QTarget"], axis=1)

    # test
    testY = test["QTarget"]
    testX = test.drop(["QTarget"], axis=1)

    return trainX, trainY, testX, testY

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
def train_extra_trees(trainX, trainY):
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

    return extress_5CV


# Function to save the best model and its parameters
def save_model_and_params(model, models_dir="models"):
    best_params = model.best_params_

    with open(os.path.join(base_dir, models_dir, "ExtraTrees_bestparams.txt"), "w") as f:
        f.write(json.dumps(best_params))
    dump(model, os.path.join(base_dir, models_dir, "ExtraTrees_best.joblib"))

    return model

# Top 5 parameter combinations ordered by mse and r2
def display_top_parameters(model, scoring_cols):
    pd.DataFrame(model.cv_results_).sort_values(by='mean_test_mse', ascending=False)[scoring_cols].head()
    results = model.cv_results_
    return results

# Evaluation of the model
def evaluate_model(model, testX, testY):
    y_pred = model.predict(testX)

    mse_test = mean_squared_error(testY, y_pred)
    r2_test = r2_score(testY, y_pred)

    print(f"MSE on test set: {mse_test}")
    print(f"R2 on test set: {r2_test}")
    for i, prediction in enumerate(y_pred):
        print(f"Sample {i + 1}: {prediction}")

    return mse_test, r2_test, y_pred

def model_training_execute():
    train, test = load_data()
    trainX, trainY, testX, testY = split_data(train, test)

    model = train_extra_trees(trainX, trainY)

    scoring_cols = [
        'param_n_estimators', 'param_criterion', 'param_max_depth', 'param_min_samples_split', 'param_min_samples_leaf', 'mean_test_mse', 'mean_test_r2'
    ]

    models_dir = "models"
    model, scoring_cols = save_model_and_params(model, models_dir)

    top_params_result = display_top_parameters(model, scoring_cols)

    mse_test, r2_test, predictions = evaluate_model(model, testX, testY)

    """This is the standard deviation of the target variable"""
    all_data = pd.concat([train, test])
    
    return mse_test, r2_test, predictions