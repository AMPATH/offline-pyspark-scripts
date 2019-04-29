from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import datetime
import time
import pyspark.sql.functions as f
from job import Job


class LabsJob(Job):
  
    def __init__(self):
             # register udfs
             self.get_drug_names = f.udf(LabsJob.getDrugNames, StringType())
        
            
    def run(self):
        print('----> Started Labs Job')
        flat_labs_and_imaging = super().getDataFromMySQL('etl', 'flat_labs_and_imaging', {
                                                'partitionColumn': 'encounter_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 900000000,
                                                'numPartitions': 500})
    
        hiv_summary = super().getDataFromMySQL('etl', 'flat_hiv_summary_v15b', {
                                            'partitionColumn': 'encounter_id', 
                                            'fetchsize':4566,
                                            'lowerBound': 1,
                                            'upperBound': 900000000,
                                            'numPartitions': 500})\
                     .where('is_clinical_encounter = 1')\
                     .groupBy(f.to_date('encounter_datetime').alias('encounter_date'))\
                    .agg(f.first('cur_arv_meds').alias('cur_arv_meds'), f.first('encounter_datetime').alias('encounter_datetime'))
        
        encounter = super().getDataFromMySQL('amrs', 'encounter', {
                                                'partitionColumn': 'encounter_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})\
                                        .select('encounter_id', 'location_id')
        
        
    
        labs_object = flat_labs_and_imaging\
                                    .join(encounter, on='encounter_id', how='left')\
                                    .join(hiv_summary, f.col('encounter_datetime') == f.col('test_datetime'), how='left')\
                                    .select('*', f.split('cur_arv_meds', ' ## ').alias('drug_ids_array'))\
                                    .select('*', self.get_drug_names(f.col('drug_ids_array')).alias('cur_arv_meds_names'))\
                                    .drop('cur_arv_meds')\
                                    .withColumnRenamed('cur_arv_meds_names', 'cur_arv_meds')\
                                    .withColumnRenamed('uuid', 'person_uuid')\
                                    .withColumn('couch_id', f.concat_ws('_', f.lit('labs'), f.col('person_id'), f.col('encounter_id')))\
                                    .withColumn('type', f.lit('labs'))\
                                    .drop('drug_ids_array')
        return labs_object
    
    
    
    @staticmethod
    def getDrugNames(drug_ids):
        drugMappings  = LabsJob.getDrugMappings()
        drugNames = ''
        if(drug_ids is not None):
            for drug_id in drug_ids:
                drugMapping = drugMappings[drug_id]
                if(drugMapping is not None):
                    drugName = drugMapping['name']
                    drugNames = drugNames + drugName + ','
            return drugNames
        else:
            return None
    
    @staticmethod
    def getDrugMappings():
        return {    
            "625": { "mapped_to_ids": "625", "name": "STAVUDINE" },
            "628": { "mapped_to_ids": "628", "name": "LAMIVUDINE" },
            "630" :{ "mapped_to_ids": "628;797", "name": "ZIDOVUDINE AND LAMIVUDINE" },
            "631": { "mapped_to_ids": "631", "name": "NEVIRAPINE" },
            "633": { "mapped_to_ids": "633", "name": "EFAVIRENZ" },
            "635": { "mapped_to_ids": "635", "name": "NELFINAVIR" },
            "749": { "mapped_to_ids": "749", "name": "INDINAVIR" },
            "791": { "mapped_to_ids": "791", "name": "EMTRICITABINE" },
            "792": { "mapped_to_ids": "625;628;631", "name": "STAVUDINE LAMIVUDINE AND NEVIRAPINE" },
            "794": { "mapped_to_ids": "794", "name": "LOPINAVIR AND RITONAVIR" },
            "795": { "mapped_to_ids": "795", "name": "RITONAVIR" },
            "796": { "mapped_to_ids": "796", "name": "DIDANOSINE" },
            "797": { "mapped_to_ids": "797", "name": "ZIDOVUDINE" },
            "802": { "mapped_to_ids": "802", "name": "TENOFOVIR" },
            "814": { "mapped_to_ids": "814", "name": "ABACAVIR" },
            "817": { "mapped_to_ids": "628;797;814", "name": "ABACAVIR LAMIVUDINE AND ZIDOVUDINE" },
            "1065" : { "mapped_to_ids": "1065", "name": "YES" },
            "1066": { "mapped_to_ids": "1066", "name": "NO" },
            "1107": { "mapped_to_ids": "1107", "name": "NONE" },
            "1400": { "mapped_to_ids": "628;802", "name": "LAMIVUDINE AND TENOFOVIR" },
            "5424": { "mapped_to_ids": "5424", "name": "OTHER ANTIRETROVIRAL DRUG" },
            "5811": { "mapped_to_ids": "5811", "name": "UNKNOWN ANTIRETROVIRAL DRUG" },
            "6156": { "mapped_to_ids": "6156", "name": "RALTEGRAVIR" },
            "6157": { "mapped_to_ids": "6157", "name": "DARUNAVIR" },
            "6158": { "mapped_to_ids": "6158", "name": "ETRAVIRINE" },
            "6159": { "mapped_to_ids": "6159", "name": "ATAZANAVIR" },
            "6160": { "mapped_to_ids": "795;6159", "name": "ATAZANAVIR AND RITONAVIR" },
            "6180": { "mapped_to_ids": "791;802", "name": "EMTRICITABINE AND TENOFOVIR" },
            "6467": { "mapped_to_ids": "628;631;633", "name": "NEVIRAPINE LAMIVUDINE AND ZIDOVUDINE" },
            "6679": { "mapped_to_ids": "628;814", "name": "ABACAVIR AND LAMIVUDINE" },
            "6964": { "mapped_to_ids": "628;633;802", "name": "TENOFOVIR AND LAMIVUDINE AND EFAVIRENZ" },
            "6965": { "mapped_to_ids": "625;628", "name": "LAMIVUDINE AND STAVUDINE" },
            "9435": { "mapped_to_ids": "9435", "name": "EVIPLERA" },
            "9759": { "mapped_to_ids": "9759", "name": "DOLUTEGRAVIR" },
            "9026": { "mapped_to_ids": "9026", "name": "LOPINAVIR" }
            }

           