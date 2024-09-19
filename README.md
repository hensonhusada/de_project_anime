Overview
========

##### Simple ETL Project for Learning <br>
##### Anime data Project with Airflow as the ETL orchestrator

Dataset: https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020

Dags:
- anime_to_gcs: transfer csv files in folder include/datasets/anime to GCS bucket
- load_anime_to_bq: load GCS bucket to BigQuery Table

This project utilizes Astronomer Airflow for easy development<br>
More on Astronomer: https://www.astronomer.io/docs/learn/get-started-with-airflow

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Install Astro in your environment (check astronomer docs for more info on how)
2. Fill in airflow_settings.yaml
3. Start Airflow on your local machine by running 'astro dev start'.
4. Go to http://localhost:8080/ and login with admin:admin
5. You should see the dags

