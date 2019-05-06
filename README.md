# poc-amrs-offline-pyspark-scripts
Scripts to build and sync offline data using PySpark and Apache Airflow

Clone this repo and run `docker-compose up -d` to start the containers. Two containers will start:
1 Airflow Container - Based on an image that contains both Apache Airflow and Spark
2 Postgres Container - Responsible for saving workflows metadata for Apache Airflow

The spark folder contains the python scripts that build and stream data with Apache Spark, while the airflow/dags contain
the scheduling code for the scripts to run.
