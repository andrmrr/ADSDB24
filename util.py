import os

base_dir = os.path.join(".")
temporal_landing = os.path.join(base_dir,  "landing_zone", "temporal")
persistent_landing = os.path.join(base_dir, "landing_zone", "persistent")

# 2017 version
dataset_folder = "datasets2017"
dataset1_fname = "alz_texas_2017.csv"
dataset2_fname = "chr_texas_2017.csv"
# 2021 version
# dataset_folder = "datasets2021"
# dataset1_fname = "alz_texas_2021.csv"
# dataset2_fname = "chr_texas_2021.csv"

dataset1 = os.path.join(base_dir, dataset_folder, dataset1_fname)
dataset2 = os.path.join(base_dir, dataset_folder, dataset2_fname)
dataset1_name = "alzheimer"
dataset2_name = "chronic_disease_indicators"
dataset1_abbrev = "alz"
dataset2_abbrev = "chr"

datasets = [
    dataset1, dataset2
]

dataset_names = {
    dataset1_fname: dataset1_name,
    dataset2_fname: dataset2_name
}

dataset_abbrev = {
    dataset1_fname: dataset1_abbrev,
    dataset2_fname: dataset2_abbrev
}

"""DuckDB"""
duckdb_folder = "duckdb_database"
duckdb_formatted = os.path.join(duckdb_folder, "formatted_zone.db")
duckdb_trusted = os.path.join(duckdb_folder, "trusted_zone.db")

"""Metadata"""
metadata_folder = "metadata"
formatted_metadata = "formatted_metadata.json"
alz_metadata = "alzheimer_metadata.json"
chr_metadata = "chronic_disease_indicators_metadata.json"
