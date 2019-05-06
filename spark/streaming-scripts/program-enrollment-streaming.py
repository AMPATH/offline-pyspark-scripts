
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
from openmrs_schemas import OpenmrsSchema
from nightly_bulk_upsert import CouchBulkUpsert
from dateutil.parser import parse
import pytz


# In[2]:


spark_submit_str = ('--driver-memory 45g --executor-memory 3g'
                    ' --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0'
                    ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                    ' --jars /home/jovyan/jars/spark-cassandra-connector.jar,/home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                    ' pyspark-shell')

os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str


# In[3]:


schemas = OpenmrsSchema()


# In[4]:


def get_spark_instance(sparkConf=None):
    if("spark" not in globals()):
        globals()["spark"] = SparkSession            .builder            .config(conf=sparkConf)            .getOrCreate()
    return globals()["spark"]


# In[ ]:


def read_from_mysql(db_name, table_name):  
    config = getConfig()
    spark = get_spark_instance()
    return spark.read.format("jdbc").      option("url", "jdbc:mysql://"+config['mysql']['host']+":" + config['mysql']['port']+ "/" + db_name + "?zeroDateTimeBehavior=convertToNull").      option("useUnicode", "true").      option("continueBatchOnError","true").      option("useSSL", "false").      option("user", config['mysql']['username']).      option("password", config['mysql']['password']).      option("dbtable",table_name).      load()


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


def save_to_cassandra(dataframe, table, keyspace="amrs"):
        dataframe.write.format("org.apache.spark.sql.cassandra")        .options(table=table, keyspace=keyspace)        .mode("append")        .save()
        print("Finished loading to cassandra table: " + table)


# In[ ]:


def parse_dates(payload):
    for k,v in payload.items(): 
        if(type(v) is int and v > 1000000000 and v % 1000 == 0):
              payload[k] = int(v/1000)
            
        else:
            payload[k] = v
    print(payload, 'payload')
    return payload


# In[ ]:


def get_program_enrollments_stream(kafka_stream):
    return kafka_stream         .filter(lambda msg: 'patient_program' in msg['schema']['name'])         .map(lambda msg: msg['payload']['after'])


# In[ ]:


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],spark_config={}, ssc_config={}, kafka_config={}, callback=None):
    
    spark_conf = SparkConf().setAppName(app_name).setMaster(master)

    if(spark_config):
        for config in spark_config:
            spark_conf.setIfMissing(config,spark_config[config])

    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("WARN")
    spark = SparkSession(sparkContext=sc)
    
    def createSSC():
        ssc = StreamingContext(sc, ssc_config['batchDuration'])
        ssc.checkpoint('checkpoints/program_enrollment')
        kafka_stream = KafkaUtils          .createDirectStream(ssc,topics=kafka_config['topics'],kafkaParams=kafka_config['config'])           .map(lambda msg: json.loads(msg[1]))
        if callback is not None:
            callback(kafka_stream=kafka_stream,sc=sc,spark_conf=spark_conf)
        return ssc
    
    #ssc = StreamingContext.getOrCreate('checkpoints/program_enrollment', createSSC)  
    ssc = StreamingContext(sc, ssc_config['batchDuration'])
    kafka_stream = KafkaUtils      .createDirectStream(ssc,topics=kafka_config['topics'],kafkaParams=kafka_config['config'])       .map(lambda msg: json.loads(msg[1]))
    if callback is not None:
        callback(kafka_stream=kafka_stream,sc=sc,spark_conf=spark_conf)
    ssc.start()
    ssc.awaitTermination()


# In[ ]:


def rebuild_microbatch(rdd, spark_conf):
    try:
        enrollments = rdd.collect()
        
        if len(enrollments) > 0:
            start_time = datetime.datetime.utcnow()  
            print("\n --- Micro-Batch --- \n")
            print("Building program enrollment objects " + time.ctime())
           
            rows = []
            location_ids = set()
            patient_ids = set()
            patient_program_ids = set()
            
            for enrollment in enrollments:
                ## filters
                patient_program_ids.add(str(enrollment['patient_program_id']))      
                patient_ids.add(enrollment['patient_id'])
                location_ids.add(enrollment['location_id'])
                

            query = """(select * from program_enrollment_view where patient_program_id in ({0})) foo""".format(",".join(patient_program_ids))
            enrollment_df = read_from_mysql('amrs', query)
            
            enrollment_transformed_df = transform_program_enrollments(enrollment_df)
            enrollment_transformed_df.show()
#             save_to_cassandra(enrollment_transformed_df, 'vitals')
#             trigger_couch_update_jobs(location_ids, enrollment_transformed_df)

        
    except:
        print("An unexpected error occured")
        raise


# In[ ]:


def trigger_couch_update_jobs(location_ids, hiv_summary_transformed_df):
#     print(location_ids)
#     for _id in location_ids:
#         print(_id)
#         today = datetime.datetime.today().strftime('%Y-%m-%d')
        today = '2018-01-02'
        job = CouchBulkUpsert(list(location_ids)[0], today, hiv_summary_transformed_df)
        job.run()
        print('Done with CouchDB --->', time.ctime())


# In[ ]:


def start_job(kafka_stream, sc, spark_conf):   
        print("-------------------- Started Pipelines --------------------")
        spark = get_spark_instance(spark_conf)
        
        get_program_enrollments_stream(kafka_stream).foreachRDD(lambda rdd: rebuild_microbatch(rdd, spark_conf)) 


# In[ ]:


def start():
    config = getConfig()
    spark_config = config['spark']
    kafka_config = config['kafka-patient-program']
    start_spark(app_name=spark_config['app_name'],
                         master=spark_config['master'],
                         spark_config=spark_config['conf'],
                         ssc_config=spark_config['streaming'],
                         kafka_config=kafka_config,
                         callback=start_job)


# In[ ]:


start()

