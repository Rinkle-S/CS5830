This repository contains code for setting up a data engineering pipeline to acquire and process public domain climatological data from the National Centers for Environmental Information (NCEI).

Pipeline Overview
Task1
Pipeline 1: Data Fetch Pipeline
This pipeline is responsible for fetching the climatological data for a given year from the NCEI archive.

Task2
Pipeline 2: Analytics Pipeline
This pipeline processes the fetched data to generate data visualizations using geomaps.

Usage
Ensure all dependencies are installed and configured.
Run the Airflow scheduler and webserver.
Enable the DAGs in the Airflow web interface.
Monitor the progress and outputs of the pipelines in the Airflow UI.
Adjust the pipelines as needed for different datasets or processing requirements.
