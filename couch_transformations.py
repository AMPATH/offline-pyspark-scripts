import os
import time
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Window
from openmrs_schemas import OpenmrsSchema


def process_encounter(encounter_dataframe):
        openmrs_schemas = OpenmrsSchema()
        parsed_encounter_dataframe = encounter_dataframe\
        .withColumn('parsed_visit', f.from_json(f.col('visit'), openmrs_schemas.get_visit_schema()))\
        .withColumn('parsed_orders', f.from_json(f.col('orders'), openmrs_schemas.get_orders_schema()))\
        .withColumn('parsed_obs', f.from_json(f.col('obs'), openmrs_schemas.get_obs_schema()))\
        .withColumn('parsed_encounter_providers', f.from_json(f.col('encounterproviders'), openmrs_schemas.get_encounter_providers_schema()))\
        .withColumnRenamed('encounterdatetime', 'encounterDatetime')\
        .withColumnRenamed('encountertype', 'encounterType')\
        .drop('visit', 'obs', 'orders', 'encounterproviders')
        patient_window = Window.partitionBy(f.col('person_id')).orderBy(f.col('encounterDatetime').desc())
        return parsed_encounter_dataframe\
                .withColumnRenamed('parsed_encounter_providers', 'encounterProviders')\
                .withColumnRenamed('parsed_visit', 'visit')\
                .withColumnRenamed('parsed_orders', 'orders')\
                .withColumnRenamed('parsed_obs', 'obs')\
                .withColumn('rn', f.row_number().over(patient_window))\
                .where(f.col('rn') < 11).drop('rn')



def transform_for_couch(table, cassandraData):
                        openmrs_schemas = OpenmrsSchema()
                        transformedData = cassandraData.withColumn('_id', f.col('couch_id').cast('string')).drop('couch_id')
            
                        if(table == 'patient'):
                            return transformedData.withColumn('parsed_patient', f.from_json('patient', openmrs_schemas.get_patient_schema())).drop('patient')\
                            .withColumnRenamed('parsed_patient', 'patient')

                        elif(table == 'encounter'):
                            return process_encounter(transformedData)
                            
                        elif(table == 'program_enrollment'):
                            return transformedData\
                                            .withColumnRenamed('date_enrolled', 'dateEnrolled')\
                                            .withColumnRenamed('date_completed', 'dateCompleted')

                        elif(table == 'lab_orders'):
                            return transformedData\
                                         .withColumnRenamed('concepuuid', 'conceptUuid')\
                                         .withColumnRenamed('ordernumber', 'orderNumber')\
                                         .withColumnRenamed('locationuuid', 'locationUuid')\
                                         .drop('str_id')
                        else:
                            return transformedData
                        

