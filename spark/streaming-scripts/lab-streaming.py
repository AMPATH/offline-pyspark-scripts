
# coding: utf-8

# In[1]:


from config import getConfig
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext
import datetime
import time
import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import json
from pyspark.streaming.kafka import KafkaUtils
import requests
from openmrs_transformations import *
from nightly_bulk_upsert import CouchBulkUpsert


# In[2]:


spark_submit_str = ('--driver-memory 45g --executor-memory 3g'
                    ' --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0'
                    ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                    ' --jars /home/jovyan/jars/spark-cassandra-connector.jar,/home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                    ' pyspark-shell')

os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str


# In[3]:


def get_spark_instance(sparkConf=None):
    if("spark" not in globals()):
        globals()["spark"] = SparkSession             .builder             .config(conf=sparkConf)             .getOrCreate()
    return globals()["spark"]


# In[4]:


def save_to_cassandra(dataframe, table, keyspace="amrs"):
        dataframe.write.format("org.apache.spark.sql.cassandra")        .options(table=table, keyspace=keyspace)        .mode("append")        .save()
        print("Finished loading to cassandra table: " + table)


# In[ ]:


def read_from_mysql(db_name, table_name, spark_conf):  
    config = getConfig()
    spark = get_spark_instance(spark_conf)
    return spark.read.format("jdbc").      option("url", "jdbc:mysql://"+config['mysql']['host']+":" + config['mysql']['port']+ "/" + db_name + "?zeroDateTimeBehavior=convertToNull").      option("useUnicode", "true").      option("continueBatchOnError","true").      option("useSSL", "false").      option("user", config['mysql']['username']).      option("password", config['mysql']['password']).      option("dbtable",table_name).      load()


# In[ ]:


def get_labs_stream(kafka_stream):
    return kafka_stream         .map(lambda msg: msg['payload']['after'])


# In[ ]:


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],spark_config={}, ssc_config={}, kafka_config={}, callback=None):
    
    spark_conf = SparkConf().setAppName(app_name).setMaster(master)

    if(spark_config):
        for config in spark_config:
            spark_conf.setIfMissing(config,spark_config[config])

    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("WARN")
    spark = SparkSession(sparkContext=sc)
     
    ssc = StreamingContext(sc, ssc_config['batchDuration'])
    kafka_stream = KafkaUtils          .createDirectStream(ssc,topics=kafka_config['topics'],kafkaParams=kafka_config['config'])           .map(lambda msg: json.loads(msg[1]))
    if callback is not None:
        callback(kafka_stream=kafka_stream,sc=sc,spark_conf=spark_conf)
    ssc.start()
    ssc.awaitTermination()


# In[ ]:


def rebuild_microbatch(rdd, spark_conf):
    try:
        labs = rdd.collect()
        if len(labs) > 0:
            start_time = datetime.datetime.utcnow()  
            print("\n --- Micro-Batch --- \n")
            print("Building Lab Objects " + time.ctime())
           
            rows = []
            encounter_ids = set()
            
            for lab in labs:
                encounter_ids.add(lab['encounter_id'])
                
            filters = {
                'encounter': {
                    'column': 'encounter_id',
                    'values': encounter_ids
                }
            }
        
            query = """(select * from flat_labs_and_imaging where encounter_id in ({0})) foo""".format(",".join([str(i) for i in encounter_ids]))
            labs = read_from_mysql('etl', query, spark_conf)                         .alias('lab_orders')
            labs.show()
            transformed_labs = transform_labs(labs, streaming=True, filters=filters)
            transformed_labs.show()
            save_to_cassandra(transformed_labs, 'labs')
            trigger_couch_update_jobs(location_ids, enrollment_transformed_df)

        
    except:
        print("An unexpected error occured")
        raise


# In[ ]:


def trigger_couch_update_jobs(location_ids, transformed_encounter):
#     print(location_ids)
#     for _id in location_ids:
#         print(_id)
#         today = datetime.datetime.today().strftime('%Y-%m-%d')
        today = '2018-01-02'
        job = CouchBulkUpsert(list(location_ids)[0], today, transformed_encounter)
        job.run()
        print('Done with CouchDB --->', time.ctime())


# In[ ]:


def start_job(kafka_stream, sc, spark_conf):   
        print("-------------------- Started Pipelines --------------------")
        get_labs_stream(kafka_stream).foreachRDD(lambda rdd: rebuild_microbatch(rdd, spark_conf)) 


# In[ ]:


def start():
    config = getConfig()
    spark_config = config['spark']
    kafka_config = config['kafka-labs']
    start_spark(app_name=spark_config['app_name'],
                         master=spark_config['master'],
                         spark_config=spark_config['conf'],
                         ssc_config=spark_config['streaming'],
                         kafka_config=kafka_config,
                         callback=start_job)


# In[ ]:


start()

