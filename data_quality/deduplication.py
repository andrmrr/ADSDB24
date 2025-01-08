# -*- coding: utf-8 -*-
"""Deduplication"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import duckdb

import datetime
import os
import json

from util import *

con = duckdb.connect(os.path.join(base_dir, duckdb_formatted))
alz = con.execute(f"SELECT * FROM alz_texas_2017_2024_10_31_19_44_38").fetchdf()

print(alz.columns)
alz_modified = alz.copy()

print(alz_modified.shape[0])
alz_modified.drop_duplicates(inplace=True)
print(alz_modified.shape[0])