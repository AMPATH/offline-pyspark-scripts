
#### This pipeline ensures the databases in CouchDB have the correct patients. 
#### Simply adds new qualifying patients and removes unqualifying patients from CouchDB


# In[1]:


from pyspark.sql import SparkSession, Row, SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
from pyspark import SparkContext, StorageLevel
import datetime
import time
import pyspark.sql.functions as f
import datetime
import sys
import json
from config import getConfig
import requests
from couch_transformations import transform_for_couch


# In[2]:


import os
spark_submit_str = ('--driver-memory 45g --executor-memory 3g --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2'
                    ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                    ' --jars /home/jovyan/jars/spark-cassandra-connector.jar,/home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                    ' --conf spark.cassandra.connection.host="cassandra",spark.cloudant.host="10.50.80.115" pyspark-shell')

os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str



# In[3]:


class CouchBulkUpsert:
    
    def __init__(self, location_id, current_date, streaming_cassandra_data=None):
        self.cassandra_tables = ['hiv_summary']
        self.initialize_spark()
        self.location_id = location_id
        self.couchdb_name = 'location_13_sample_7'  #TODO: should be auto generated given location_id/uuid
        self.current_date = current_date
        self.qualifying_patients = self.fetch_qualifying_patients(self.current_date).alias('todays')
        self.streaming_cassandra_data = streaming_cassandra_data

        
    def initialize_spark(self):
        global_config = getConfig()
        self.spark = SparkSession.builder\
            .config('spark.sql.repl.eagerEval.enabled', True)\
                .config('cloudant.host', '10.50.80.115:5984')\
                    .config('cloudant.username', global_config['couch']['username'])\
                        .config('cloudant.password', global_config['couch']['password'])\
                            .config('cloudant.protocol', 'http')\
                                .config("jsonstore.rdd.partitions", 5)\
                                    .config("jsonstore.rdd.requestTimeout", 90000000)\
                                        .config('spark.rdd.compress', True)\
                                            .config('cloudant.useQuery', True)\
                                                .config('spark.sql.crossJoin.enabled', True)\
                                                    .config("schemaSampleSize", 1)\
                                                        .config("spark.sql.broadcastTimeout", 1200)\
                                                            .config("spark.sql.shuffle.partitions", 200)\
                                                                .config("spark.sql.autoBroadcastJoinThreshold", 1024*1024*300)\
                                                                    .getOrCreate()
        self.spark.sparkContext.setLogLevel('INFO')
        
    def getDataFromAmrsWithConfig(self, tableName, db_name, config):
        global_config = getConfig()
        print(global_config)
        print(global_config['mysql'])
        return self.spark.read.format("jdbc").\
                 option("url", "jdbc:mysql://"+global_config['mysql']['host']+":" + global_config['mysql']['port']+ "/" + db_name + "?zeroDateTimeBehavior=convertToNull").\
                                  option("useUnicode", "true").\
                                  option("continueBatchOnError","true").\
                                  option("useSSL", "false").\
                                  option("user", global_config['mysql']['username']).\
                                  option("password", global_config['mysql']['password']).\
                                    option("dbtable",tableName).\
                                        option("partitionColumn", config['partitionColumn']).\
                                            option("fetchSize", config['fetchsize']).\
                                                option("lowerBound", config['lowerBound']).\
                                                    option("upperBound", config['upperBound']).\
                                                        option("numPartitions", config['numPartitions']).\
                                                            load()

        
    def fetch_qualifying_patients(self, date):
        
        query = """(select distinct(person_id) from
                          etl.flat_hiv_summary_v15b
                          where rtc_date between date_sub('{0}', interval 28 day)
                          and date_add('{0}',interval 2 day) and location_id = {1}) foo""".format(date, self.location_id)
        
        qualifying_patients = self.getDataFromAmrsWithConfig(query, 'etl', {
        'partitionColumn': 'person_id', 
        'fetchsize':50,
        'lowerBound': 1500,
        'upperBound': 9000000,
        'numPartitions': 900})
        
        return qualifying_patients
    
    def save_to_couchdb(self, dataframe, database, createDBOnSave="false"):
        dataframe.write.save(database,"org.apache.bahir.cloudant",
                          bulkSize="800", createDBOnSave=createDBOnSave)
        
    def read_from_couchdb(self, dbName):
        return self.spark.read.format("org.apache.bahir.cloudant").load(dbName)
    
        
        
        
    def read_from_cassandra(self, table, keyspace="amrs"):
        return self.spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    
        
    
    def run(self):
        couchdb_data = self.read_from_couchdb(self.couchdb_name).select('_id', '_rev', 'build_date')\
        .withColumnRenamed('build_date', 'couch_build_date')\
        .alias('couchdb_data')\
        .cache()
        
        if(self.streaming_cassandra_data is None):
            for table in self.cassandra_tables:
                cassandra_data = self.read_from_cassandra(table).join(self.qualifying_patients, on='person_id').alias('cassandra_data').cache()
                # handle updates to existing patient data in couch
                existing_data_in_couch = cassandra_data.join(couchdb_data, on=f.col('couch_id') == f.col('_id'), how='inner')

                if(existing_data_in_couch.rdd.isEmpty() == False):
                    print('Data to be updated ---->', time.ctime())

                    updated_data = existing_data_in_couch\
                                        .filter(f.col("cassandra_data.build_date") != f.col("couch_build_date"))\
                                        .drop('_id', 'couch_build_date')

                    transformed_data = transform_for_couch(table, updated_data)
                    self.save_to_couchdb(transformed_data, self.couchdb_name)

                # add new data
                new_data = cassandra_data.join(couchdb_data, on=f.col('couch_id') == f.col('_id'), how='left').where(f.col('couchdb_data._id').isNull())\
                                         .drop('couchdb_data._rev', 'couchdb_data._id', 'couchdb_data.person_id', 'couchdb_data.build_date')
                
                transformed_new_data = transform_for_couch(table, new_data)
                self.save_to_couchdb(transformed_new_data, self.couchdb_name)
                cassandra_data.unpersist()
                print('Data updated to couchdb ------', time.ctime())
        else:
                print('Started streaming process for couchdb...')
                cassandra_data = self.streaming_cassandra_data.join(self.qualifying_patients, on='person_id').alias('cassandra_data').cache()
                # handle updates to existing patient data in couch
                existing_data_in_couch = cassandra_data.join(couchdb_data, on=f.col('couch_id') == f.col('_id'), how='inner')

                if(existing_data_in_couch.rdd.isEmpty() == False):
                    print('Data to be updated ---->', time.ctime())

                    updated_data = existing_data_in_couch\
                                        .filter(f.col("cassandra_data.build_date") != f.col("couch_build_date"))\
                                        .drop('_id', 'couch_build_date')

                    transformed_data = transform_for_couch('hiv_summary', updated_data)
                    self.save_to_couchdb(transformed_data, self.couchdb_name)

                # add new data
                new_data = cassandra_data.join(couchdb_data, on=f.col('couch_id') == f.col('_id'), how='left').where(f.col('couchdb_data._id').isNull())\
                                         .drop('couchdb_data._rev', 'couchdb_data._id', 'couchdb_data.person_id', 'couchdb_data.build_date')
                transformed_new_data = transform_for_couch('hiv_summary', new_data)
                self.save_to_couchdb(transformed_new_data, self.couchdb_name)
                cassandra_data.unpersist()
                print('Data added to couchdb ------', time.ctime())
                
        couchdb_data.unpersist()
