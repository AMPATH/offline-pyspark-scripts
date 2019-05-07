
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


def get_hiv_summary_stream(kafka_stream):
    return kafka_stream         .filter(lambda msg: 'sample_hiv_summary' in msg['schema']['name'])         .map(lambda msg: msg['payload']['after'])         .map(lambda payload: parse_dates(payload))


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
    try:
        hiv_summary = rdd.collect()

        if len(hiv_summary) > 0:
            start_time = datetime.datetime.utcnow()  
            print("\n --- Micro-Batch --- \n")
            print("Building encounter objects " + time.ctime())
           
            rows = []
            encounter_ids = set()
            location_ids = set()
            visit_ids = set()
            patient_ids = set()
            form_ids = set()
            
            for summary in hiv_summary:
                ## filters
                encounter_ids.add(summary['encounter_id'])
                location_ids.add(summary['location_id'])
                
                row = Row(**summary)
                rows.append(row)

            spark = get_spark_instance(spark_conf)
            hiv_summary_df = spark.createDataFrame(rows, schemas.hiv_summary_schema())                .withColumn('date_created_parsed', 
                        f.to_timestamp(f.col('date_created')))\
                 .withColumn('encounter_datetime_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('encounter_datetime'))))\
                 .withColumn('enrollment_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('enrollment_date'))))\
                 .withColumn('hiv_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('hiv_start_date'))))\
                 .withColumn('death_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('death_date'))))\
                 .withColumn('transfer_in_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('transfer_in_date'))))\
                 .withColumn('transfer_out_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('transfer_out_date'))))\
                 .withColumn('prev_rtc_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_rtc_date'))))\
                 .withColumn('rtc_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('rtc_date'))))\
                 .withColumn('arv_first_regimen_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('arv_first_regimen_start_date'))))\
                 .withColumn('arv_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('arv_start_date'))))\
                 .withColumn('prev_arv_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_arv_start_date'))))\
                 .withColumn('prev_arv_end_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_arv_end_date'))))\
                 .withColumn('tb_screening_datetime_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('tb_screening_datetime'))))\
                 .withColumn('ipt_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('ipt_start_date'))))\
                 .withColumn('ipt_stop_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('ipt_stop_date'))))\
                 .withColumn('ipt_completion_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('ipt_completion_date'))))\
                 .withColumn('ipt_completion_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('ipt_completion_date'))))\
                 .withColumn('tb_tx_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('tb_tx_start_date'))))\
                 .withColumn('tb_tx_end_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('tb_tx_end_date'))))\
                 .withColumn('pcp_prophylaxis_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('pcp_prophylaxis_start_date'))))\
                 .withColumn('condoms_provided_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('condoms_provided_date'))))\
                 .withColumn('modern_contraceptive_method_start_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('modern_contraceptive_method_start_date'))))\
                 .withColumn('cd4_resulted_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_resulted_date'))))\
                 .withColumn('cd4_1_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_1_date'))))\
                 .withColumn('cd4_2_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_2_date'))))\
                 .withColumn('cd4_percent_1_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_percent_1_date'))))\
                 .withColumn('cd4_percent_2_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_percent_2_date'))))\
                 .withColumn('vl_resulted_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('vl_resulted_date'))))\
                 .withColumn('vl_1_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('vl_1_date'))))\
                 .withColumn('vl_2_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('vl_2_date'))))\
                 .withColumn('vl_order_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('vl_order_date'))))\
                 .withColumn('cd4_order_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('cd4_order_date'))))\
                 .withColumn('hiv_dna_pcr_order_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('hiv_dna_pcr_order_date'))))\
                 .withColumn('hiv_rapid_test_resulted_date_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('hiv_rapid_test_resulted_date'))))\
                 .withColumn('prev_encounter_datetime_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_encounter_datetime_hiv'))))\
                 .withColumn('next_encounter_datetime_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('next_encounter_datetime_hiv'))))\
                 .withColumn('prev_clinical_datetime_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_clinical_datetime_hiv'))))\
                 .withColumn('next_clinical_datetime_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('next_clinical_datetime_hiv'))))\
                 .withColumn('prev_clinical_rtc_date_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('prev_clinical_rtc_date_hiv'))))\
                 .withColumn('next_clinical_rtc_date_hiv_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('next_clinical_rtc_date_hiv'))))\
                 .withColumn('outreach_date_bncd_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('outreach_date_bncd'))))\
                 .withColumn('outreach_death_date_bncd_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('outreach_death_date_bncd'))))\
                 .withColumn('transfer_date_bncd_parsed', 
                        f.to_timestamp(f.from_unixtime(f.col('transfer_date_bncd'))))\
                .drop('encounter_datetime', 'enrollment_date', 'hiv_start_date', 'death_date', 'transfer_in_date',
                     'transfer_out_date', 'prev_rtc_date', 'rtc_date', 'arv_first_regimen_start_date', 'arv_start_date',
                     'prev_arv_start_date', 'prev_arv_end_date', 'tb_screening_datetime', 'ipt_start_date', 'ipt_stop_date',
                     'ipt_completion_date', 'tb_tx_start_date', 'tb_tx_end_date', 'pcp_prophylaxis_start_date', 'condoms_provided_date',
                     'modern_contraceptive_method_start_date', 'cd4_resulted_date', 'cd4_1_date', 'cd4_2_date', 'cd4_percent_1_date',
                     'cd4_percent_2_date', 'vl_resulted_date', 'vl_1_date', 'vl_2_date', 'vl_order_date', 'cd4_order_date', 'hiv_dna_pcr_order_date',
                     'hiv_dna_pcr_resulted_date', 'hiv_dna_pcr_1_date', 'hiv_dna_pcr_2_date', 'hiv_rapid_test_resulted_date', 'prev_encounter_datetime_hiv',
                     'next_encounter_datetime_hiv', 'prev_clinical_datetime_hiv', 'next_clinical_datetime_hiv', 'next_clinical_rtc_date_hiv', 
                     'prev_clinical_rtc_date_hiv', 'outreach_date_bncd', 'outreach_death_date_bncd', 'transfer_date_bncd', 'date_created')\
            
            new_column_name_list= [x.replace('_parsed', '') for x in hiv_summary_df.columns]
            
            print(new_column_name_list)
            
            hiv_summary_transformed_df = transform_hiv_summary(hiv_summary_df.toDF(*new_column_name_list))
                
            save_to_cassandra(hiv_summary_transformed_df, 'hiv_summary')
            trigger_couch_update_jobs(location_ids, hiv_summary_transformed_df)

        
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
        get_hiv_summary_stream(kafka_stream).foreachRDD(lambda rdd: rebuild_microbatch(rdd, spark_conf)) 


# In[ ]:


def start():
    config = getConfig()
    spark_config = config['spark']
    kafka_config = config['hiv-summary']
    start_spark(app_name=spark_config['app_name'],
                         master=spark_config['master'],
                         spark_config=spark_config['conf'],
                         ssc_config=spark_config['streaming'],
                         kafka_config=kafka_config,
                         callback=start_job)


# In[ ]:


start()

