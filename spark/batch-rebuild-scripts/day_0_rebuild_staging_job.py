
# coding: utf-8

# #### This notebook _rebuilds_ all OpenMRS Objects in the jobs array then stores them in Cassandra. To add a dataset to build:
# 1. Create a python job file that implements the job interface. The job should build the corresponding Openmrs objects.
# 2. Create a corresponding database to store the objects in Cassandra
# 3. Run the rebuild staging job
import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from patient_job import PatientJob
from vitals_job import VitalsJob
from lab_orders_job import LabOrdersJob
from labs_job import LabsJob
from hiv_summary_job import HivSummaryJob
from encounter_job import EncounterJob
from program_enrollment_job import ProgramEnrollmentJob

def save_to_cassandra(dataframe, table, keyspace="amrs"):
         dataframe.write.format("org.apache.spark.sql.cassandra")\
        .options(table=table, keyspace=keyspace)\
        .mode("append")\
        .save()
         print("Finished loading to cassandra table: " + table)

def run_and_save(job, table):
    dataframe = job.run()
    if('build_date' not in dataframe.columns):
        dataframe = dataframe.withColumn('build_date', f.current_timestamp())
    save_to_cassandra(dataframe, table)
    



def start_couchdb_jobs(cassandra_tables):
    pass




def rebuild(jobs):
    for job in jobs:
        cassandra_tables = []
        start = time.time()
        cassandra_tables.append(job["table"])
        run_and_save(job["job"], job["table"])
        end = time.time()
        print("Rebuilding" + job["table"] + "took %.2f seconds" % (end - start))
    
jobs = [{
                'table': 'vitals',
                'job': VitalsJob()
        }]
        
rebuild(jobs)
#start_couchdb_jobs(cassandra_tables)
