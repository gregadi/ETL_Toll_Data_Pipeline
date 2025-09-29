# ETL_Toll_Data_Pipeline
# Highway ETL Pipeline with Airflow

**Project Name:** `ETL_Toll_Data_Pipeline`  

## Description
This project implements an ETL (Extract, Transform, Load) pipeline using **Apache Airflow** to process and consolidate traffic data from multiple toll plazas across national highways. The pipeline handles data in **CSV, TSV, and fixed-width formats**, transforms it into a unified format, and loads it into a staging area for further analysis. The goal is to facilitate data-driven insights that can help decongest national highways.

## Features
- Extracts traffic data from multiple file formats: CSV, TSV, and fixed-width files.
- Transforms and cleans the data to ensure consistency.
- Consolidates data into a single unified file.
- Loads the cleaned data into a staging area for downstream analytics.
- Fully orchestrated using **Apache Airflow** with `BashOperator`.

## DAG Tasks
- Unzip Data – Extract compressed files.
- Extract CSV Data – Read and process CSV files.
- Extract TSV Data – Read and process TSV files.
- Extract Fixed-Width Data – Read and process fixed-width files.
- Consolidate Data – Merge all extracted data into a single file.
- Transform Data – Clean and standardize the consolidated data.
- Load Data – Save transformed data to the staging area.
