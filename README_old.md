# Data Management Backbone
Authors: Andrejic Andreja, Zatezalo Mateja

## Data organization
- datasets2017 include the 2017 version of the datasets
- datasets2021 include the 2021 version of the datasets
- landing_zone contains the temporal and persistent zones
- duckdb_database contains databases for the: formatted zone, trusted zone and exploitation zone
- metadata contains metadata JSON files

## Script organization
- util.py contains functions, paths and variables used by different scripts
- clear.py clears all files and databases
- ingestionlandingzone.py handles data ingestion and propagation from the temporal to the persistent landing zone
- formattedzone.py saves the datasets from the landing zone in duckdb tables
- dataprofiling.py contains code for data profiling
- alz_preprocessing.py contains functions for preparation of Alzheimer datasets
- chr_preprocessing.py contains functions for preparation of Chronic Disease Indicators datasets
- trustedzone.py handles data quality processes and integration of different versions of datasets from the formatted zone to the trusted zone. It uses dataprofiling.py, alz_preprocessing.py and chr_preprocessing.py for data quality processes
- exploitation.py does data reconciliation and combines the tables from the trusted zone into the exploitation zone based on semantic context 
- orchestration.py connects all the zones. It can be executed for the execution of the full pipeline. It also provides the functions that are called from the GUI
- gui.py contains the code for the user interface

## Instructions on how to run the code
There are two ways to run the Data Management Backbone:
    1. Each zone propagation can be called manually using the corresponding buttons in the GUI. GUI is started by running the script gui.py
    2. The whole pipeline can be executed by running orchestration.py

The choice of datasets to be ingested is done by changing these variables in the util.py file: dataset_folder, dataset1_fname, dataset2_fname. In the file there are two ready configurations, namely 2017 and 2021 versions of our datasets.

## Adding a new datasource
In order to add another data source, the following steps are required:
    1. A specific data preparation function is necessary, similar to chr_preprocessing and alz_preprocessing. It has to be called from the main function of the trusted zone.
    2. Data integration and reconciliation have to be adjusted in the exploitation zone
    3. Required paths need to be added to the util.py, following the example of the existing datasets

All in all, we managed to automate the landing and the formatted zone. The trusted zone requires data quality processes that are specific to the nature of the data. The exploitation zone combinations and reconciliation is also specific to different data.
