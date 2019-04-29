#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext, Window, SparkSession
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StringType, StructField, \
    BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import datetime
import time
import pyspark.sql.functions as f
from pyspark import StorageLevel
from job import Job


class ObsJob(Job):
    def __init__(self):
           pass
        
    
    
    def build_obs(self):
        print('Computing Obs ...')
        concept = super().getDataFromMySQL('amrs', 'concept', {
                'partitionColumn': 'concept_id', 
                'fetchsize':1000,
                'lowerBound': 1,
                'upperBound': 10000,
                'numPartitions': 10})\
        .filter(f.col('retired') == 0)\
        .select('concept_id', 'uuid')\
        .withColumnRenamed('uuid', 'concept_uuid')
    
        concept_names = super().getDataFromMySQL('amrs', 'concept_name', {
                'partitionColumn': 'concept_id', 
                'fetchsize':15000,
                'lowerBound': 1,
                'upperBound': 15000,
                'numPartitions': 10})\
        .filter(f.col('locale_preferred') == 1)\
        .select('concept_id', 'name')
        
        drug = super().getDataFromMySQL('amrs', 'drug', {
                'partitionColumn': 'drug_id', 
                'fetchsize':15000,
                'lowerBound': 1,
                'upperBound': 15000,
                'numPartitions': 10})\
        .select('drug_id', 'name')
        
        concept_with_names = concept.join(concept_names, 'concept_id').cache()
        
        
        parent_obs = super().getDataFromMySQL('test', 'obs_view', {
                'partitionColumn': 'obs_id', 
                'fetchsize':35000,
                'lowerBound': 10000,
                'upperBound': 300000000,
                'numPartitions': 3000})\
        .select('obs_id', 
                'concept_id', 
                'uuid', 
                'value_coded', 
                'value_drug',
                'value_text', 
                'value_numeric', 
                'value_datetime',
                'obs_datetime',
                'encounter_id',
               'voided')\
        .alias('parent')\
        .join(f.broadcast(concept_with_names), 'concept_id', how='inner')\
                        .withColumnRenamed('name', 'parent_obs_concept_name')\
                        .withColumnRenamed('concept_uuid', 'parent_obs_concept_uuid')
        
        child_obs = super().getDataFromMySQL('test', 'obs_view', {
                'partitionColumn': 'obs_group_id', 
                'fetchsize':35000,
                'lowerBound': 1000000,
                'upperBound': 300000000,
                'numPartitions': 3000})\
        .select("obs_id", 
                "person_id", 
                "concept_id", 
                "encounter_id",
                "obs_datetime", 
                "obs_group_id",  
                "value_coded", 
                "value_drug", 
                "value_datetime", 
                "value_numeric", 
                "value_text",
                "uuid",
               "voided")\
        .alias('child')\
        .join(f.broadcast(concept_with_names).alias('child'), 'concept_id', how='inner')\
                        .withColumnRenamed('name', 'obs_concept_name')\
                        .withColumnRenamed('concept_uuid', 'obs_concept_uuid')
        
        
        obs_group = child_obs.join(parent_obs, parent_obs['parent.obs_id'] == child_obs['obs_group_id'], how='inner')
        
        processed_obs_group = process_obs_group(obs_group, concept_with_names, drug)
        

        obs_without_group = parent_obs\
                    .join(f.broadcast(concept_with_names), f.col('value_coded') == concept_with_names['concept_id'], how='left')\
                        .withColumnRenamed('name', 'value_coded_concept_name')\
                        .withColumnRenamed('concept_uuid', 'value_coded_concept_uuid')\
                    .join(f.broadcast(drug), f.col('value_drug') == f.col('drug_id'), how='left')\
                        .withColumnRenamed('name', 'drug_name')\
                    .withColumn('value_not_coded', f.when(f.col('value_numeric').isNotNull(), f.col('value_numeric').cast('string'))\
                                                                        .when(f.col('value_text').isNotNull(), f.col('value_text'))\
                                                                        .when(f.col('value_drug').isNotNull(), f.col('drug_name'))\
                                                                        .when(f.col('value_datetime').isNotNull(), f.col('value_datetime').cast('string'))
                                                   )\
                    .withColumn('value_type', f.when(f.col('value_numeric').isNotNull(), f.lit('numeric'))\
                                                                        .when(f.col('value_text').isNotNull(), f.lit('text'))\
                                                                        .when(f.col('value_drug').isNotNull(), f.lit('drug'))\
                                                                        .when(f.col('value_datetime').isNotNull(), f.lit('datetime'))\
                                                                        .when(f.col('value_coded').isNotNull(), f.lit('coded')))\
                   .select('obs_id', 'obs_datetime', 'value_not_coded', 'value_coded', 'parent_obs_concept_name', 
                           'value_type', 'encounter_id','value_coded_concept_name', 'value_coded_concept_uuid', 'parent_obs_concept_uuid')
        
        
        
        final_product = obs_without_group.join(processed_obs_group, f.col('obs_group_id') == f.col('obs_id'), how='left');
        return final_product
        
        


def process_obs_group(obs_group, concept_with_names, drug):
    
    obs_group_with_value_names =  obs_group\
                        .join(f.broadcast(concept_with_names), obs_group['child.value_coded'] == concept_with_names['concept_id'], how='left')\
                        .withColumnRenamed('name', 'value_coded_concept_name')\
                        .withColumnRenamed('concept_uuid', 'value_coded_concept_uuid')
    
    obs_group_with_drug_and_names = obs_group_with_value_names.join(f.broadcast(drug), obs_group['child.value_drug'] == drug['drug_id'], how='left')\
                        .withColumnRenamed('name', 'drug_name')
    
    
    
    obs_group_with_value_processed = obs_group_with_drug_and_names\
                                        .withColumn('value_not_coded', f.when(f.col('child.value_numeric').isNotNull(), f.col('child.value_numeric').cast('string'))\
                                                                        .when(f.col('child.value_text').isNotNull(), f.col('child.value_text'))\
                                                                        .when(f.col('child.value_drug').isNotNull(), f.col('drug_name'))\
                                                                        .when(f.col('child.value_datetime').isNotNull(), f.col('child.value_datetime').cast('string'))
                                                   )\
                                       .withColumn('value_type', f.when(f.col('child.value_numeric').isNotNull(), f.lit('numeric'))\
                                                                        .when(f.col('child.value_text').isNotNull(), f.lit('text'))\
                                                                        .when(f.col('child.value_drug').isNotNull(), f.lit('drug'))\
                                                                        .when(f.col('child.value_datetime').isNotNull(), f.lit('datetime'))\
                                                                        .when(f.col('child.value_coded').isNotNull(), f.lit('coded'))
                                                   ).repartition('obs_group_id')
    
    return obs_group_with_value_processed.groupBy('obs_group_id').agg(
        f.to_json(
        f.collect_set(
            f.struct(
                f.col('obs_concept_uuid').alias('uuid'),
                f.col('child.obs_datetime').alias('obsDatetime'),
                f.struct(
                    f.col('obs_concept_uuid').alias('uuid'),
                    f.col('obs_concept_name').alias('display'),
                ).alias('concept'),
                f.concat_ws(' ', f.col('parent_obs_concept_name'), f.lit(':'),
                                 f.col('obs_concept_name')).alias('display'),
                f.struct(
                    f.col('parent_obs_concept_name').alias('display'),
                    f.col('parent_obs_concept_uuid').alias('uuid')
                ).alias('obsGroup'),
                f.struct(
                f.when(f.col('value_not_coded').isNotNull(),
                      f.col('value_not_coded'))
                .otherwise(
                    f.to_json(
                    f.struct(
                        f.col('value_coded_concept_name').alias('display'),
                        f.col('value_coded_concept_uuid').alias('uuid')
                    )
                    )
                ).alias('value'),
                f.col('value_type').alias('type')).alias('value')
            )
        )).alias('groupmembers')
    )



			