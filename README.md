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
