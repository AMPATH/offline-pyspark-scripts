import os
import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Window

from helpers.openmrs_schemas import OpenmrsSchema
from helpers.add_new_patients_to_couch import AddPatientsToCouchRunner
from config.config import getConfig

class CouchJobRunner:
    
    def __init__(self, date, location_id, cassandra_tables, couchdb_name):
        self.location_id = location_id
        self.cassandra_tables = cassandra_tables
        self.date = date
        self.openmrs_schemas = OpenmrsSchema()
        self.couchdb_name = 'db-{0}'.format(couchdb_name)
        
    def get_spark(self):
        spark_submit_str = ('--driver-memory 40g --executor-memory 3g'
                            ' --packages'
                            ' org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,com.datastax.spark:spark-cassandra-connector_2.11:2.3.2'
                            ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                            ' --jars /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                            ' pyspark-shell')
        os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str
        config = getConfig()
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
        
    def read_from_mysql(self, query, db_name, config):
        global_config = getConfig()
        return self.get_spark().read.format("jdbc").\
             option("url", "jdbc:mysql://"+global_config['mysql']['host']+":" + global_config['mysql']['port']+ "/" + db_name + "?zeroDateTimeBehavior=convertToNull").\
             option("useUnicode", "true").\
             option("continueBatchOnError","true").\
             option("useSSL", "false").\
             option("user", global_config['mysql']['username']).\
            option("password", global_config['mysql']['password']).\
          option("dbtable",query).\
          option("partitionColumn", config['partitionColumn']).\
          option("fetchSize", config['fetchsize']).\
          option("lowerBound", config['lowerBound']).\
          option("upperBound", config['upperBound']).\
          option("numPartitions", config['numPartitions']).\
          load()


    def get_qualifying_patients(self, date, location_id):
        query = """(select distinct(person_id) from
                          etl.flat_hiv_summary_v15b
                          where rtc_date between date_sub('{0}', interval 28 day)
                          and date_add('{0}',interval 2 day) and location_id = {1}) foo""".format(date, location_id)
        
        qualifying_patients = self.read_from_mysql(query, 'etl', {
        'partitionColumn': 'person_id', 
        'fetchsize': 50,
        'lowerBound': 1500,
        'upperBound': 9000000,
        'numPartitions': 900})
        
        return qualifying_patients
    
        
    def run(self):
        qualifying_patients = self.get_qualifying_patients(self.date, self.location_id)
        add_patients = AddPatientsToCouchRunner(self.location_id, self.cassandra_tables, qualifying_patients, self.couchdb_name)
        add_patients.run()   
        


### RUN ###
           
CASSANDRA_TABLES = ['vitals']
LOCATION_ID = 13
LOCATION_UUID = '08fec056-1352-11df-a1f1-0026b9348838'
LOCATION_NAME = 'Kitale'
DATE = '2018-01-02'
job_runner = CouchJobRunner(DATE, LOCATION_ID, CASSANDRA_TABLES, LOCATION_UUID)
job_runner.run()