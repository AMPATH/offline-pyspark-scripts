import datetime
import time

from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import pyspark.sql.functions as f

from job import Job


class ProgramEnrollmentJob(Job):
    def __init__(self):
           pass

            
    def run(self):
        print('----> Started Program Enrollments Job')
        
        program_enrollment = super().getDataFromMySQL('amrs', 'program_enrollment_view', {
            'partitionColumn': 'person_id', 
            'fetchsize':4566,
            'lowerBound': 1,
            'upperBound': 500000,
            'numPartitions': 900})\
        .filter(f.col('voided') == False)\
        .withColumn('couch_id', f.col('uuid'))\
        .withColumn('type', f.lit('program_enrollment'))\
        .withColumn('location', f.struct(
            f.col('location_name').alias('name'),
            f.col('location_uuid').alias('uuid')
        )).withColumn('program', f.struct(
            f.col('program_name').alias('name'),
            f.col('program_uuid').alias('uuid')
        )).drop('program_name',
                'location_uuid',
                'location_name')
        return program_enrollment
