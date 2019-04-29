from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import datetime
import time
import pyspark.sql.functions as f
from job import Job


class MetaDataJob(Job):
    def __init__(self):
           pass

            
    def run(self):
        print('----> Started Metadata Job')
        
        location = super().getDataFromMySQL('amrs', 'location', {
            'partitionColumn': 'location_id', 
            'fetchsize': 100,
            'lowerBound': 1,
            'upperBound': 500,
            'numPartitions': 1})
        
        program = super().getDataFromMySQL('etl', 'location', {
            'partitionColumn': 'program_id', 
            'fetchsize': 100,
            'lowerBound': 1,
            'upperBound': 500,
            'numPartitions': 1})
        
        return {
            "location": location,
            "program": program
        }

        
           