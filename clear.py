import os
import shutil
import duckdb
from util import *

if __name__ == "__main__":
    if os.path.isdir(temporal_landing):
        shutil.rmtree(temporal_landing)
    if os.path.isdir(persistent_landing):
        shutil.rmtree(persistent_landing)
    if os.path.isdir(duckdb_folder):
        shutil.rmtree(duckdb_folder)
    if os.path.exists(os.path.join(base_dir, metadata_folder)):
        shutil.rmtree(os.path.join(base_dir, metadata_folder))