import os
import time
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Window
from openmrs_schemas import OpenmrsSchema
from config import getConfig

def getSpark():
        global_config = getConfig()
        spark_submit_str = ('--driver-memory 40g --executor-memory 3g --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,com.datastax.spark:spark-cassandra-connector_2.11:2.4.0'
                            ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                            ' --jars /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                            ' pyspark-shell')
        os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str
        spark = SparkSession\
        .builder\
        .config('spark.sql.repl.eagerEval.enabled', True)\
        .config('cloudant.host', '10.50.80.115:5984')\
        .config('cloudant.username', global_config['couch']['username'])\
        .config('cloudant.password', global_config['couch']['password'])\
        .config('cloudant.protocol', 'http')\
        .config("cloudant.useQuery", True)\
        .config("jsonstore.rdd.partitions", 5)\
        .config('spark.driver.maxResultSize', "15000M")\
        .config('spark.sql.crossJoin.enabled', True)\
        .config('spark.sql.autoBroadcastJoinThreshold', 0)\
        .config("spark.cassandra.connection.host", global_config['cassandra']['host'])\
        .config("spark.cassandra.auth.username", global_config['cassandra']['username'])\
        .config("spark.cassandra.auth.password", global_config['cassandra']['password'])\
        .config("spark.cassandra.output.consistency.level", "ANY")\
        .config("spark.sql.shuffle.partitions", 200)\
        .getOrCreate()
        spark.sparkContext.setLogLevel("INFO") 
        return spark
    
def stringify_list(*args):
            return (", ".join(["%d"] * len(args))) % tuple(args)
                
def getDataFromMySQL(dbName, tableName, config=None):  
        global_config = getConfig()
        spark = getSpark()
        if(config is None):
            return spark.read.format("jdbc").\
              option("url", "jdbc:mysql://{0}:{1}/".format(global_config['mysql']['host'], global_config['mysql']['port']) + dbName + "?zeroDateTimeBehavior=convertToNull").\
              option("useUnicode", "true").\
              option("continueBatchOnError","true").\
              option("useSSL", "false").\
              option("user", global_config['mysql']['username']).\
              option("password", global_config['mysql']['password']).\
              option("dbtable",tableName).\
              load()
        else:
            return spark.read.format("jdbc").\
              option("url", "jdbc:mysql://"+global_config['mysql']['host']+":"+global_config['mysql']['port']+ "/"
               + dbName + "?zeroDateTimeBehavior=convertToNull").\
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




def get_provider(_filter=None):
    if(_filter is None):
        data = getDataFromMySQL('amrs', 'provider', {
            'partitionColumn': 'provider_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 45000000,
            'numPartitions': 20,
            }).select('uuid', 'identifier', 'provider_id', 'person_id'
                      ).withColumnRenamed('uuid', 'provider_uuid'
                    ).withColumnRenamed('identifier',
                    'provider_identifier').alias('provider')
    else:
        table = '(select uuid, identifier, provider_id, person_id from provider where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('amrs', table)

    person_name = getDataFromMySQL('amrs', 'person_name', {
            'partitionColumn': 'person_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 45000000,
            'numPartitions': 20,
            }).select('given_name', 'family_name', 'person_id')\
             .filter(f.col('preferred') == 1).alias('person_name')

    return data.join(person_name, 'person_id', how='left')\
                       .withColumn('provider_name',f.concat_ws(' ', f.col('given_name'),f.col('family_name')))\
                       .drop('given_name', 'family_name')

def get_encounter_providers(_filter = None, filter_columns=['encounter_id']):
    
    if(_filter is None):
        encounter_provider = getDataFromMySQL('amrs',
                                    'encounter_provider', {
                                    'partitionColumn': 'provider_id',
                                    'fetchsize': 4566,
                                    'lowerBound': 1,
                                    'upperBound': 50000000,
                                    'numPartitions': 500
                                    })
    else:
        query = '(select * from encounter_provider where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        encounter_provider = getDataFromMySQL('amrs', query)
        
    provider = get_provider()
        
    return encounter_provider.select('uuid', 'encounter_id', 'provider_id')\
                            .withColumnRenamed('uuid', 'encounter_provider_uuid')\
                            .join(provider, 'provider_id')\
                            .alias('enc_provider')


def get_encounter_types():
        encounter_type = getDataFromMySQL('amrs',
                'encounter_type', {
            'partitionColumn': 'encounter_type_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 100,
            'numPartitions': 1,
            }).select('uuid', 'name', 'encounter_type_id'
                      ).withColumnRenamed('uuid', 'encounter_type_uuid'
                    ).withColumnRenamed('name', 'encounter_type_name')

        return encounter_type

def get_forms(_filter = None, filter_columns=['form_id']):
    if(_filter is None):
        forms = getDataFromMySQL('amrs', 'form', {
            'partitionColumn': 'form_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 10000,
            'numPartitions': 10,
            })
    else:
        query = '(select * from form where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        forms = getDataFromMySQL('amrs', query)
        
    return forms.select('form_id', 'uuid', 'name')\
        .withColumnRenamed('uuid', 'form_uuid')\
        .withColumnRenamed('name', 'form_name')

def get_locations(_filter = None, filter_columns=['location_id']):
        if(_filter is None):
            location = getDataFromMySQL('amrs', 'location', {
                'partitionColumn': 'location_id',
                'fetchsize': 4566,
                'lowerBound': 1,
                'upperBound': 45000000,
                'numPartitions': 1,
                })
        else:
            query = '(select * from location where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
            location = getDataFromMySQL('amrs', query)

        return location.select('uuid', 'name', 'location_id'
                          ).withColumnRenamed('uuid', 'location_uuid'
                        ).withColumnRenamed('name', 'location_name')

def get_visits(locations, _filter = None, filter_columns=['visit_id']):

    if(filter is None):
        visit = getDataFromMySQL('amrs', 'visit', {
            'partitionColumn': 'visit_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 45000000,
            'numPartitions': 100,
            })
    else:
        query = '(select * from visit where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))

        visit = getDataFromMySQL('amrs', query)

    visit_type = getDataFromMySQL('amrs', 'visit_type', {
            'partitionColumn': 'visit_type_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 45000000,
            'numPartitions': 1,
            })\
    .select('uuid', 'name', 'visit_type_id')\
    .withColumnRenamed('uuid', 'visit_type_uuid')\
    .withColumnRenamed('name', 'visit_type_name')
    

    return visit.select(
            'uuid',
            'date_started',
            'date_stopped',
            'visit_type_id',
            'visit_id',
            'location_id',
            ).withColumnRenamed('uuid', 'visit_uuid').join(visit_type, on='visit_type_id')\
                .join(f.broadcast(locations), on='location_id')\
                .drop('visit_type_id', 'location_id').alias('visit')

def get_patients(_filter=None, filter_columns=['patient_id']):
    
        if(_filter is None):
            person = getDataFromMySQL('amrs', 'person', {
                'partitionColumn': 'person_id',
                'fetchsize': 4566,
                'lowerBound': 1,
                'upperBound': 1000000,
                'numPartitions': 100,
                }).select('uuid', 'person_id').withColumnRenamed('uuid',
                        'person_uuid')

            patient = getDataFromMySQL('amrs', 'patient', {
                'partitionColumn': 'patient_id',
                'fetchsize': 4566,
                'lowerBound': 1,
                'upperBound': 10000000,
                'numPartitions': 100,
                }).select('patient_id')

        else:
            patient_query = '(select * from patient where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
            person_query =  '(select * from person where person_id in ({0})) foo'.format(stringify_list(*_filter['values']))
            person = getDataFromMySQL('amrs', person_query).select('uuid', 'person_id').withColumnRenamed('uuid', 'person_uuid')
            patient = getDataFromMySQL('amrs', patient_query).select('patient_id')
            
        return person.join(patient, on=f.col('person_id') == f.col('patient_id')).drop('person_id')
            

    
def get_encounters(_filter = None):
    if(_filter is None):
            data = getDataFromMySQL('amrs', 'encounter', {
            'partitionColumn': 'encounter_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 10000000,
            'numPartitions': 100,
            }).filter(f.col('voided') == False).alias('encounter')
    else:
        table = '(select * from encounter where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('amrs', table)
        
    return data

def get_concepts():
        concepts = getDataFromMySQL('amrs', 'concept', {
            'partitionColumn': 'concept_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 25000,
            'numPartitions': 1,
            }).filter(f.col('retired') == False).select('uuid', 'concept_id').withColumnRenamed('uuid',
                    'concept_uuid')

        concept_names = getDataFromMySQL('amrs', 'concept_name'
                , {
            'partitionColumn': 'concept_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 50000,
            'numPartitions': 1,
            }).filter(f.col('locale_preferred') == 1).select('name',
                    'concept_id').withColumnRenamed('name',
                    'concept_name')

        return concepts.join(concept_names, on='concept_id')

def get_orders(_filter = None, filter_columns = []):
        if(_filter is None):
            table = 'orders'
            data = getDataFromMySQL('amrs', table , {
                'partitionColumn': 'encounter_id',
                'fetchsize': 4566,
                'lowerBound': 1,
                'upperBound': 10000000,
                'numPartitions': 200,
                })
        else:
            #for now only filtering by encounter id
            #TODO: extend to allow filtering by multiple columns
            table = '(select * from orders where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
            data = getDataFromMySQL('amrs', table)
        
        
        orders = data\
                 .filter(f.col('voided') == 0)\
                 .select(
                'uuid',
                'encounter_id',
                'concept_id',
                'orderer',
                'order_action',
                'date_activated',
                'date_created',
                'urgency',
                'order_type_id',
                'order_number',
                ).withColumnRenamed('uuid', 'order_uuid')

        order_type = getDataFromMySQL('amrs', 'order_type', {
            'partitionColumn': 'order_type_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 100,
            'numPartitions': 1,
            }).select('order_type_id', 'name').withColumnRenamed('name'
                    , 'order_type_name')

        concepts = get_concepts()

        orderer = get_provider()

        return orders.join(f.broadcast(order_type), on='order_type_id')\
                     .join(f.broadcast(concepts), on='concept_id')\
                     .join(f.broadcast(orderer), on=orders['orderer'] == orderer['provider_id'])\
                     .drop('concept_id', 'order_type_id')\
                     .alias('orders')


def get_obs(filter=None):
        if(filter is None):
                        return getSpark().read.format('org.apache.spark.sql.cassandra'
                ).options(table='obs', keyspace='amrs'
                          ).load().alias('obs')
        else:
            return getSpark().read.format('org.apache.spark.sql.cassandra'
                ).options(table='obs', keyspace='amrs'
                          ).load().filter(f.col(filter['column']).isin(filter['values'])).alias('obs')
        

        
def get_concepts():
        concepts = getDataFromMySQL('amrs', 'concept', {
            'partitionColumn': 'concept_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 20000,
            'numPartitions': 1,
            }).filter(f.col('retired') == False).select('uuid', 'concept_id').withColumnRenamed('uuid',
                    'concept_uuid')

        concept_names = getDataFromMySQL('amrs', 'concept_name'
                , {
            'partitionColumn': 'concept_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 50000,
            'numPartitions': 1,
            }).filter(f.col('locale_preferred') == 1).select('name',
                    'concept_id').withColumnRenamed('name',
                    'concept_name')

        return concepts.join(concept_names, on='concept_id')
    
def get_drug():
    return getDataFromMySQL('amrs', 'drug', {
                'partitionColumn': 'drug_id', 
                'fetchsize':15000,
                'lowerBound': 1,
                'upperBound': 15000,
                'numPartitions': 10})\
        .select('drug_id', 'name')

def get_obs_for_orders(_filter=None):
    if(_filter is None):
            table = 'obs_order_view'
            data = getDataFromMySQL('amrs', table , {
                'partitionColumn': 'encounter_id',
                'fetchsize': 4566,
                'lowerBound': 1,
                'upperBound': 10000000,
                'numPartitions': 200
                })
    else:
            table = '(select * from obs_order_view where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
            data = getDataFromMySQL('amrs', table)
            
    return data\
            .withColumnRenamed('uuid', 'obs_uuid')\
            .withColumnRenamed('obs_datetime', 'sample_collection_date')
        

def get_person_name(_filter=None):
    if(_filter is None):
        table = 'person_name'
        data = getDataFromMySQL('amrs', 'person_name', {
                                                'partitionColumn': 'person_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})\
                                                .select('given_name', 'middle_name', 'family_name', 'person_id', 'uuid')
    else:
        table = '(select given_name, middle_name, family_name, person_id, uuid from person_name where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('amrs', table)

    return data\
          .withColumnRenamed('uuid', 'patient_uuid')\
          .withColumn('person_name', f.concat_ws(' ', f.col('given_name'), f.col('middle_name'), f.col('family_name')))\
          .drop('given_name', 'middle_name', 'family_name')\
    
                                                

def get_patient_identifier(_filter):
    if(_filter is None):
        table = 'patient_identifier'
        data = getDataFromMySQL('amrs', 'patient_identifier', {
                                                'partitionColumn': 'patient_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 300})\
               .select('patient_id', 'identifier')
    else:
        table = '(select patient_id, identifier from patient_identifier where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('amrs', table)

    return data
    
    
def get_person(_filter):
    if(_filter is None):
        table = 'person'
        data = getDataFromMySQL('amrs', table, {
                                                'partitionColumn': 'person_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})\
                                                .select('person_id', 'uuid')
    else:
        table = '(select person_id, uuid from person where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('amrs', table)

    return data\
          .withColumnRenamed('uuid', 'person_uuid')
    
                                                

def get_hiv_summary(_filter = None):
    if(_filter is None):
            data = getDataFromMySQL('etl', 'flat_hiv_summary_v15b', {
            'partitionColumn': 'encounter_id',
            'fetchsize': 4566,
            'lowerBound': 1,
            'upperBound': 10000000,
            'numPartitions': 100,
            }).filter(f.col('voided') == False).alias('encounter')
    else:
        table = '(select * from flat_hiv_summary_v15b where {0} in ({1})) foo'.format(_filter['column'], stringify_list(*_filter['values']))
        data = getDataFromMySQL('etl', table)
        
    return data