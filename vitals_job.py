from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import datetime
import time
import pyspark.sql.functions as f
from job import Job


class VitalsJob(Job):
    def __init__(self):
           pass

            
    def run(self):
        print('----> Started Vitals Job')
        
        vitals = super().getDataFromMySQL('etl', 'flat_vitals', {
            'partitionColumn': 'person_id', 
            'fetchsize':4566,
            'lowerBound': 1,
            'upperBound': 9000000,
            'numPartitions': 500})

        patient_window = Window.partitionBy(f.col('person_id')).orderBy(f.col('encounter_datetime').desc())

        last_10_recent_vitals = vitals.withColumn('rn', f.row_number().over(patient_window))\
                                                .where(f.col('rn') < 11).drop('rn')\
                                      .withColumn('type', f.lit('vitals'))\
                                      .withColumn('couch_id', f.concat_ws('_', f.lit('vitals'), f.col('person_id'), f.col('encounter_id')))\
                                      .withColumn('build_date', f.current_timestamp())
        
        return last_10_recent_vitals
           