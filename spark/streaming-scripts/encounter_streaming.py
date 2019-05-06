
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


def read_from_mysql(self, db_name, table_name, config):  
    global_config = getConfig()
    spark = get_spark_instance()
    return spark.read.format("jdbc")\
            .option("url", "jdbc:mysql://" + global_config['mysql']['host'] + ":" + global_config['mysql']['host'] + "/" + db_name + "?zeroDateTimeBehavior=convertToNull")\
            .option("useUnicode", "true")\
             .option("continueBatchOnError","true")\
             .option("useSSL", "false")\
             .option("user", global_config['mysql']['username'])\
             .option("password", global_config['mysql']['password'])\
             .option("dbtable",table_name)\
             .option("partitionColumn", config['partitionColumn'])\
             .option("fetchSize", config['fetchsize'])\
              .option("lowerBound", config['lowerBound'])\
              .option("upperBound", config['upperBound'])\
              .option("numPartitions", config['numPartitions'])\
              .load()


# In[ ]:


def fetch_qualifying_patients(date, location_id):
        print('Fetching qualifying patients for ' + date)
        query = """(select distinct(person_id) from
                          etl.flat_hiv_summary_v15b
                          where rtc_date between date_sub('{0}', interval 28 day)
                          and date_add('{0}',interval 2 day) and location_id = {1}) foo""".format(date, location_id)
        
        qualifying_patients = read_from_mysql(query, 'etl', {
        'partitionColumn': 'person_id', 
        'fetchsize':50,
        'lowerBound': 1500,
        'upperBound': 9000000,
        'numPartitions': 900})


# In[ ]:


def get_encounter_schema():
    return StructType([
                 StructField("encounter_id", IntegerType(), True),
                 StructField("encounter_type", IntegerType(), True),
                 StructField("patient_id", IntegerType(), True),
                 StructField("form_id", IntegerType(), True),
                 StructField("location_id", IntegerType(), True),
                 StructField("encounter_datetime", StringType(), True),
                 StructField("creator", IntegerType(), True),
                 StructField("date_created", StringType(), True),
                 StructField("voided", IntegerType(), True),
                 StructField("voided_by", IntegerType(), True),
                 StructField("date_voided", StringType(), True),
                 StructField("void_reason", StringType(), True),
                 StructField("uuid", StringType(), True),
                 StructField("changed_by", IntegerType(), True),
                 StructField("date_changed", StringType(), True),
                 StructField("visit_id", IntegerType(), True)
             ])


# In[ ]:


def save_to_cassandra(dataframe, table, keyspace="amrs"):
        dataframe.write.format("org.apache.spark.sql.cassandra")        .options(table=table, keyspace=keyspace)        .mode("append")        .save()
        print("Finished loading to cassandra table: " + table)


# In[ ]:


def get_encounters_stream(kafka_stream):
    return kafka_stream         .filter(lambda msg: msg['schema']['name'] == 'amrsmysql.amrs.encounter.Envelope')         .map(lambda msg: msg['payload']['after'])


# In[ ]:


def get_obs_stream(kafka_stream):
    return kafka_stream         .filter(lambda msg: msg['schema']['name'] == 'amrsmysql.amrs.obs.Envelope')         .map(lambda msg: msg['payload']['after'])


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
    kafka_stream = KafkaUtils              .createDirectStream(ssc,topics=kafka_config['topics'],kafkaParams=kafka_config['config'])               .map(lambda msg: json.loads(msg[1]))
    if callback:
            callback(kafka_stream=kafka_stream,sc=sc,spark_conf=spark_conf)
    ssc.start()
    ssc.awaitTermination()


# In[ ]:


def rebuild_microbatch(rdd, spark_conf):
    global_config = getConfig()
    try:
        encounters = rdd.collect()

        if len(encounters) > 0:
            start_time = datetime.datetime.utcnow()  
            print("\n --- Micro-Batch --- \n")
            print("Building encounter objects " + time.ctime())
           
            rows = []
            encounter_ids = set()
            location_ids = set()
            visit_ids = set()
            patient_ids = set()
            form_ids = set()
            
            for encounter in encounters:
                ## filters
                encounter_ids.add(encounter['encounter_id'])
                location_ids.add(encounter['location_id'])
                visit_ids.add(encounter['visit_id'])
                patient_ids.add(encounter['patient_id'])
                form_ids.add(encounter['form_id'])
                
                encounter_object = Row(**encounter)
                rows.append(encounter_object)

            spark = get_spark_instance(spark_conf)
            
            obs_query = '(select * from obs where encounter_id in ({0})) foo'.format((", ".join(["%d"] * len(encounter_ids))) % tuple(encounter_ids))
            
            obs = spark.read.format('jdbc').option('url', 'jdbc:mysql://mysql2:3306/' + 'amrs' + '?zeroDateTimeBehavior=convertToNull')\
                .option('useUnicode', 'true')\
                .option('continueBatchOnError', 'true').option('useSSL','false')\
                .option('user', global_config['mysql']['user'])\
                .option('password', global_config['mysql']['password'])\
                .option('dbtable', obs_query)\
                .load()   
                         
            encounter_df = spark.createDataFrame(rows, get_encounter_schema())                                .withColumnRenamed('encounter_datetime', 'encounter_unixtime')                                .withColumnRenamed('date_created', 'unixtime_created')                                .withColumnRenamed('date_voided', 'unixtime_voided')                                .withColumnRenamed('date_changed', 'unixtime_changed')                                .withColumnRenamed('voided', 'voided_int')
            
            encounter_df_fixed_schema = encounter_df.withColumn('encounter_datetime', f.to_timestamp(f.from_unixtime(f.col("encounter_unixtime") / 1000)))            .withColumn('date_created', f.to_timestamp(f.from_unixtime(f.col("unixtime_created") / 1000)))            .withColumn('date_voided', f.to_timestamp(f.from_unixtime(f.col("unixtime_voided") / 1000)))            .withColumn('date_changed', f.to_timestamp(f.from_unixtime(f.col("unixtime_changed") / 1000)))            .withColumn('voided', f.when(f.col('voided_int') == 0, False).otherwise(True))            .drop('encounter_unixtime', 'unixtime_created', 'unixtime_voided', 'unixtime_changed', 'voided_int')            .alias('encounter')
            
            filters = {
                       'encounter_ids': {'column': 'encounter_id', 'values': [0 if x is None else x for x in list(encounter_ids) ]},
                       'visit_ids':     {'column': 'visit_id', 'values': [0 if x is None else x  for x in list(visit_ids)]},
                       'form_ids':      {'column': 'form_id', 'values': [0 if x is None else x for x in list(form_ids)]},
                       'location_ids':  {'column': 'location_id', 'values': [0 if x is None else x for x in list(location_ids)]},
                       'patient_ids':   {'column': 'patient_id', 'values': [0 if x is None else x for x in list(patient_ids)]}
                      }
            transformed_obs = transform_obs(obs)
            transformed_encounter = transform_encounter(encounter_df_fixed_schema, transformed_obs, True, filters).cache()
            save_to_cassandra(transformed_encounter, 'encounter') 
            trigger_couch_update_jobs(location_ids, transformed_encounter)
            transformed_encounter.unpersist()
                

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
        get_encounters_stream(kafka_stream).foreachRDD(lambda rdd: rebuild_microbatch(rdd, spark_conf)) 


# In[ ]:


def start():
    config = getConfig()
    spark_config = config['spark']
    kafka_config = config['kafka-encounter-obs-orders']
    start_spark(app_name=spark_config['app_name'],
                         master=spark_config['master'],
                         spark_config=spark_config['conf'],
                         ssc_config=spark_config['streaming'],
                         kafka_config=kafka_config,
                         callback=start_job)


# In[ ]:


start()
