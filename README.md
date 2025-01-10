# Data Management Backbone
**Authors**: Andrejic Andreja, Zatezalo Mateja

---

## Data Organization
- **datasets2017**: includes the 2017 and 2021 versions of the datasets
- **datasets2021**: includes the 2021 version of the datasets
- **landing_zone**: contains the temporal and persistent zones
- **duckdb_database**: contains databases for the formatted zone, trusted zone, and exploitation zone
- **metadata**: contains metadata JSON files

---

## Script Organization
- **util.py**: contains functions, paths, and variables used by different scripts
- **clear.py**: clears all files and databases
- **initial_sampling.py**: samples the original dataset to a managable size
- **ingestionlandingzone.py**: handles data ingestion and propagation from the temporal to the persistent landing zone
- **formattedzone.py**: saves the datasets from the landing zone in DuckDB tables
- **dataprofiling.py**: contains code for data profiling
- **alz_preprocessing.py**: contains functions for preparation of Alzheimer datasets
- **chr_preprocessing.py**: contains functions for preparation of Chronic Disease Indicators datasets
- **trustedzone.py**: handles data quality processes and integration of different versions of datasets from the formatted zone to the trusted zone. It uses `dataprofiling.py`, `alz_preprocessing.py`, and `chr_preprocessing.py` for data quality processes
- **exploitation.py**: does data reconciliation and combines the tables from the trusted zone into the exploitation zone based on semantic context 
- **orchestration.py**: connects all the zones. It can be executed for the full pipeline. It also provides functions that are called from the GUI
- **gui.py**: contains the code for the user interface

---

## Instructions on How to Run the Code
There are two ways to run the Data Management Backbone:
1. Each zone propagation can be called manually using the corresponding buttons in the GUI. Start the GUI by running the script `gui.py`.
2. The whole pipeline can be executed by running `orchestration.py`.

The choice of datasets to be ingested is done by changing the following variables in the `util.py` file:
   - `dataset_folder`
   - `dataset1_fname`
   - `dataset2_fname`

The file includes two ready configurations, namely the 2017 and 2021 versions of our datasets.

---

## Adding a New Data Source
To add another data source, follow these steps:
1. A specific data preparation function is necessary, similar to `chr_preprocessing` and `alz_preprocessing`. It has to be called from the main function of the trusted zone.
2. Data integration and reconciliation must be adjusted in the exploitation zone.
3. Required paths need to be added to `util.py`, following the example of the existing datasets.

---

In summary, we managed to automate the landing and the formatted zones. The trusted zone requires data quality processes that are specific to the nature of the data. The exploitation zone combinations and reconciliation are also specific to different data.
