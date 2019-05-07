import os
import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Window

from helpers.couch_transformations import transform_for_couch
from config.config import getConfig

class AddPatientsToCouchRunner:
    
    def __init__(self, location_id, cassandra_tables, qualifying_patients, couchdb_name):
        self.location_id = location_id
        self.cassandra_tables = cassandra_tables
        self.qualifying_patients = qualifying_patients
        self.couchdb_name = couchdb_name
        
        
    def get_spark(self):
        config = getConfig()
        spark_submit_str = ('--driver-memory 40g --executor-memory 3g'
                            ' --packages'
                            ' org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,com.datastax.spark:spark-cassandra-connector_2.11:2.3.2'
                            ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                            ' --jars /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                            ' pyspark-shell')
        os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str
        spark = SparkSession\
        .builder\
        .config('spark.sql.repl.eagerEval.enabled', True)\
        .config('cloudant.host', config['couch']['host'])\
        .config('cloudant.username', config['couch']['username'])\
        .config('cloudant.password', config['couch']['password'])\
        .config('cloudant.protocol', config['couch']['protocol'])\
        .config("jsonstore.rdd.partitions", 15000)\
        .config('spark.driver.maxResultSize', "15000M")\
        .config('spark.sql.crossJoin.enabled', True)\
        .config('spark.sql.autoBroadcastJoinThreshold', 0)\
        .config("spark.cassandra.connection.host", config['cassandra']['host'])\
        .config("spark.cassandra.auth.username", config['cassandra']['username'])\
        .config("spark.cassandra.auth.password", config['cassandra']['password'])\
        .config("spark.cassandra.output.consistency.level", "ANY")\
        .getOrCreate()
    
        return spark
        
    def read_from_cassandra(self, table, keyspace="amrs"):
         return self.get_spark().read\
                .format("org.apache.spark.sql.cassandra")\
                .options(table=table, keyspace=keyspace)\
                .load()
        
    def save_to_couch(self, dataframe, database, createDBOnSave='false'):
        dataframe.write.save(database,"org.apache.bahir.cloudant",
                              bulkSize="500", createDBOnSave=createDBOnSave)
                

        
    def run(self):
                """
                Fetches data from Cassandra for the given patients and saves it in CouchDB
                """
            
                print('Building {0} dataset(s) to couchdb for location {1}'.format(str(len(self.cassandra_tables)), str(self.location_id)))

                start = time.time()

                #load data from cassandra
                for table in self.cassandra_tables:
                    cassandraData = self.read_from_cassandra(table).join(self.qualifying_patients, on="person_id").cache()
                    cassandraData = transform_for_couch(table, cassandraData)
                    
                    if(table == 'patient'):
                        try:
                                self.save_to_couch(cassandraData, self.couchdb_name)
                        except:
                                self.save_to_couch(cassandraData, self.couchdb_name, 'true')
                        cassandraData.unpersist()
                        
                    else:
                        patients_in_location = cassandraData.filter(f.col('location_id') == self.location_id).select('person_id').distinct()
                        all_data_for_patients_in_location = cassandraData.join(patients_in_location, 'person_id')
                        cassandraData.unpersist()
                        try:
                                self.save_to_couch(all_data_for_patients_in_location, self.couchdb_name, 'true')
                        except:
                                self.save_to_couch(all_data_for_patients_in_location, self.couchdb_name)

                end = time.time()
                print("Couch Job took %.2f seconds" % (end - start))    

            
        