
# coding: utf-8

# This notebook is responsible for rebuilding and updating metadata objects for OpenMRS 
# such as locations, programs, etc and store them directly in CouchDB

import datetime
import time

from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
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
            'numPartitions': 1}).\
            select('name', 'uuid').\
            withColumn('_id', f.col('uuid')).\
            withColumn('build_date', f.current_timestamp()).\
            withColumn('type', f.lit('location'))
        
        program = super().getDataFromMySQL('amrs', 'program', {
            'partitionColumn': 'program_id', 
            'fetchsize': 100,
            'lowerBound': 1,
            'upperBound': 500,
            'numPartitions': 1}).\
            select('name', 'uuid', 'concept_id').\
            withColumn('_id', f.col('uuid')).\
            withColumn('build_date', f.current_timestamp()).\
            withColumn('type', f.lit('program'))
        
        concept = super().getDataFromMySQL('amrs', 'concept', {
            'partitionColumn': 'concept_id', 
            'fetchsize': 100,
            'lowerBound': 1,
            'upperBound': 500,
            'numPartitions': 1}).\
            select('concept_id', 'uuid').\
            withColumnRenamed('uuid', 'concept_uuid')
        
        program_object = program.join(concept, on="concept_id")\
                                .withColumn('concept', f.struct(f.col('concept_uuid').alias('uuid')))\
                                .drop('concept_uuid', 'concept_id')

        metadata_couch = Job.getSpark().read.format("org.apache.bahir.cloudant").load('openmrs-metadata').select('_id', '_rev')
        
        
        replace_program = program_object.join(metadata_couch, on='_id', how='left')
        replace_location = location.join(metadata_couch, on='_id', how='left')
        
        super().saveToCouchDB(replace_program, 'openmrs-metadata')
        super().saveToCouchDB(replace_location, 'openmrs-metadata')
        
    


### RUN ###

start = time.time()
job = MetaDataJob()
job.run()
end = time.time()
print("This took %.2f seconds" % (end - start))
