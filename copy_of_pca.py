"""Copy of pca"""

import pandas as pd
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import sys

X = pd.read_csv('/content/drive/MyDrive/ADSDB/part2/FeatureGeneration/DataLabelling/df_ml_Xset.csv')
y = pd.read_csv('/content/drive/MyDrive/ADSDB/part2/FeatureGeneration/DataLabelling/df_ml_yset.csv')

sys.path.append('/content/drive/MyDrive/ADSDB/part2/FeatureGeneration/TrainingAndValidationDatasets')

from training_testing_sets import splitting

ts, rs, X_train, X_test, y_train, y_test = splitting()

pca = PCA(n_components = 2, random_state=42)

X_train_pca = pca.fit_transform(X_train)
X_test_pca = pca.transform(X_test)

explained_variance = pca.explained_variance_ratio_

explained_variance

print("original shape:   ", X_train.shape)
print("transformed shape:", X_train_pca.shape)

X_train_pca = pd.DataFrame(X_train_pca)
X_test_pca = pd.DataFrame(X_test_pca)
X_train_pca.to_csv('/content/drive/MyDrive/ADSDB/part2/AdvancedTopics/FeatureSelection/X_trainPCA.csv', index=False)
X_test_pca.to_csv('/content/drive/MyDrive/ADSDB/part2/AdvancedTopics/FeatureSelection/X_testPCA.csv', index=False)

y_train  = pd.DataFrame(y_train)
y_test = pd.DataFrame(y_test)
y_train.to_csv('/content/drive/MyDrive/ADSDB/part2/AdvancedTopics/FeatureSelection/y_train.csv', index=False)
y_test.to_csv('/content/drive/MyDrive/ADSDB/part2/AdvancedTopics/FeatureSelection/y_test.csv', index=False)

