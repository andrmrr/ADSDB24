import os

base_dir = os.path.join(".")
temporal_landing = os.path.join(base_dir,  "landing_zone", "temporal")
persistent_landing = os.path.join(base_dir, "landing_zone", "persistent")

# 2017 version
datasource_folder = "datasets2017"
datasource1_fname = "alz_texas_2017.csv"
datasource2_fname = "chr_texas_2017.csv"
# 2021 version
# datasource_folder = "datasets2021"
# datasource1_fname = "alz_texas_2021.csv"
# datasource2_fname = "chr_texas_2021.csv"

datasource1 = os.path.join(base_dir, datasource_folder, datasource1_fname)
datasource2 = os.path.join(base_dir, datasource_folder, datasource2_fname)
datasource1_name = "alzheimer"
datasource2_name = "chronic_disease_indicators"

datasources = [
    datasource1, datasource2
]

datasource_names = {
    datasource1_fname: datasource1_name,
    datasource2_fname: datasource2_name
}

"""DuckDB"""
duckdb_folder = "duckdb_database"
duckdb_formatted = os.path.join(duckdb_folder, "formatted_zone.db")
duckdb_trusted = os.path.join(duckdb_folder, "trusted_zone.db")