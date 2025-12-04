# End-to-end US health pipeline and analysis
The goal of this project is creating a **production-ready automated pipeline that ingests and processes prepares US public healthcare dataset and trains an ML model on the prepared data**.
Furthermore, a **detailed analysis of the results** and the data in its entirety was conducted and the results are presented in the two documents: *ADSDB_Project_Part1.pdf* and *ADSDB_Project_Part2.pdf*.
The entire process is reproducible and the instructions for running the code, as well as the code structure are described in *Instructions.md* file. The entire project was written in **Python**.
The pipeline was separated into two parts: *Data Management Pipeline* and *Data Analysis Pipeline*. \\
Data Management Pipeline - the first part of the pipeline handling ingestion and generic data quality processes. It includes the following components
1. Landing zone - folder structure where the raw datasets are ingested, accompanied by the *persistant loader* that executes the ingestion
2. Formatted zone - **DuckDB** database where raw files are converted into proper tabular data defined by a schema, including relevant metadata and *formatted loaders* that insert the raw files into the database according to a schema
3. Trusted zone - an integrated table with the most recent versions of the datasets are included and go through a set of data quality processes, including the *trusted loader* that handles the integration and *preprocessing* scripts that conduct the data quality processes
4. Exploitation zone - a database including a set of logically separated tables, relevant to the defined KPIs and propossed analysis, along with an *exploitation loader* that correctly distributes the data into specified tables
Data Analysis Pipeline - the second part of the pipeline handling data processing specific to the analysis at hand, the model training and finally the analysis. It includes the following components:
1. Analytical Sandbox - a database with a relevant subset of data from the exploitation zone.
2. Feature Engineering Zone - a final dataframe of data prepared to be used for model training. It has two sections:
    1. Feature Generation Zone - final manipulation of the dataframe before model training, generating features that can be efficiently utilized by ML models.
    2. Data Preparation Zone - final data quality checks, including specific imputation of missing data, type conversions and necessary renaming (reconciliation).
3. Model Generation - the code for running and training the model, including mechanisms for model comparison and persistance of the final model.
4. Result Analysis - the complete analysis of the results: rating the prediction quality, residual analysis, PCA, and feature importance analysis.

The project also contains the following utility components:
- Profiling - showing statistics and visual representation of the data in each stage
- Orchestration - the code necessary to correctly execute the pipeline
- Performance Monitoring - monitoring and display of system performance metrics, namely CPU, memory and disk
- GUI - a simple graphical interface created using *tkinter* library to be used for monitoring and pipeline execution
