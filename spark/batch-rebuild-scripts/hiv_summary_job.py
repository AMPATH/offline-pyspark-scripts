import datetime
import time

from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import pyspark.sql.functions as f

from job import Job
import helpers.mappings as mappings

class HivSummaryJob(Job):
    
    def __init__(self, job_config=None):
           pass

            
    def run(self):
        print('----> Started Hiv Summary Job')
        # register udfs
        self.get_drug_names = f.udf(get_drug_names, StringType())
        # TODO
        # parameterize the number of hiv summary per patient
        # make method to fetch hiv_summary by date/by person_ids
        
        hiv_summary = super().getDataFromMySQL('etl', 'flat_hiv_summary_v15b', {
            'partitionColumn': 'encounter_id', 
            'fetchsize':4566,
            'lowerBound': 1,
            'upperBound': 9000000,
            'numPartitions': 900})

        patient_window = Window.partitionBy(f.col('person_id')).orderBy(f.col('encounter_datetime').desc())

        last_10_recent_hiv_summary = hiv_summary.withColumn('rn', f.row_number().over(patient_window))\
                                .where(f.col('rn') < 11).drop('rn')\
                                .withColumn('type', f.lit('hiv_summary'))\
            .withColumn('couch_id', f.concat_ws('_', f.lit('hivsummary'), f.col('person_id'), f.col('encounter_id')))\
            .withColumn('build_date', f.current_timestamp())\
            .withColumn('cur_arv_meds_names', self.get_drug_names(f.col('cur_arv_meds')))\
            .withColumn('arv_first_regimen_names', self.get_drug_names(f.col('arv_first_regimen')))\
            .withColumn('cur_arv_meds_strict_names', self.get_drug_names(f.col('cur_arv_meds_strict')))\
            .withColumn('prev_arv_meds_names', self.get_drug_names(f.col('prev_arv_meds')))\
            .drop('cur_arv_meds', 'arv_first_regimen', 'cur_arv_meds_strict', 'prev_arv_meds')\
            .withColumnRenamed('cur_arv_meds_names', 'cur_arv_meds')\
            .withColumnRenamed('cur_arv_meds_strict_names', 'cur_arv_meds_strict')\
            .withColumnRenamed('prev_arv_meds_names', 'prev_arv_meds')\
            .withColumnRenamed('arv_first_regimen_names', 'arv_first_regimen')

        return last_10_recent_hiv_summary
