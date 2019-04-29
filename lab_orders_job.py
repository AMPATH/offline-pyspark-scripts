from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import datetime
import time
import pyspark.sql.functions as f
from job import Job



class LabOrdersJob(Job):
        
    def __init__(self):
        pass
    
    def save_to_cassandra(self, dataframe, table, keyspace="amrs"):
         dataframe.write.format("org.apache.spark.sql.cassandra")\
            .options(table=table, keyspace=keyspace)\
            .mode("append").save()
         print("Finished loading to cassandra table: " + table)
            
    def run(self):
        print('----> Started Lab Orders Job')
        
        concept_name = super().getDataFromMySQL('amrs', 'concept_name', {
                                                'partitionColumn': 'concept_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 100000,
                                                'numPartitions': 1})\
                                                .filter(f.col('locale_preferred') == True)\
                                                .select('concept_id', 'name')\
                                                .withColumnRenamed('name', 'display')\
                                                .alias('concept_name')\
                                                .cache()
        

        
        concept = super().getDataFromMySQL('amrs', 'concept', {
                                                'partitionColumn': 'concept_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 100000,
                                                'numPartitions': 1})\
                                                .select('concept_id', 'uuid')\
                                                .withColumnRenamed('uuid', 'conceptuuid')\
                                                .alias('concept')
        
        obs = super().getDataFromMySQL('amrs', 'obs', {
                                                'partitionColumn': 'order_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 300000000,
                                                'numPartitions': 300})\
                                     .where(f.col('voided').isNull() | f.col('voided') == False)\
                                     .select('order_id', 'concept_id', 'value_coded', 'uuid', 'obs_datetime')\
                                     .withColumnRenamed('obs_datetime', 'sample_collection_date')\
                                     .withColumnRenamed('uuid', 'obs_uuid')\
                                     .join(f.broadcast(concept_name), on=f.col('concept_name.concept_id') == f.col('value_coded'), how='left')\
                                     .withColumnRenamed('concept_name.name', 'sample_drawn')\
                                     .alias('obs')
                                
        person_name = super().getDataFromMySQL('amrs', 'person_name', {
                                                'partitionColumn': 'person_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})\
                                                .select('given_name', 'middle_name', 'family_name', 'person_id', 'uuid')\
                                                .withColumnRenamed('uuid', 'patient_uuid')\
                                                .withColumn('person_name', f.concat_ws(' ', f.col('given_name'), f.col('middle_name'), f.col('family_name')))\
                                                .drop('given_name', 'middle_name', 'family_name')\
                                                .alias('person_name')
        
                
        person = super().getDataFromMySQL('amrs', 'person', {
                                        'partitionColumn': 'person_id',
                                        'fetchsize':4566,
                                        'lowerBound': 1,
                                        'upperBound': 9000000,
                                        'numPartitions': 300})\
                       .select('person_id')\
                       .alias('person')
        
        lab_orders = super().getDataFromMySQL('amrs', 'orders', {
                                                'partitionColumn': 'order_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})\
                            .where(f.col('voided').isNull() | f.col('voided') == False)\
                            .select("order_id", "order_number", "order_type_id", "uuid", "date_activated", "patient_id", "encounter_id", "orderer", "concept_id")\
                            .withColumnRenamed('order_number', 'ordernumber')\
                            .join(person, on=f.col('patient_id') == f.col('person.person_id'), how='left')\
                            .withColumnRenamed('person_id', 'personid')\
                            .join(person_name, on=f.col('patient_id') == f.col('person_name.person_id'), how='left')\
                            .join(f.broadcast(concept), on='concept_id', how='left')\
                            .join(f.broadcast(concept_name), on='concept_id', how='left')\
                            .withColumnRenamed('concept_name.name', 'display')\
                            .withColumnRenamed('concept_id', 'order_type')\
                            .drop('person_id')\
                            .withColumnRenamed('personid', 'person_id')\
                            .alias('lab_orders')
        
        encounter = super().getDataFromMySQL('amrs', 'encounter', {
                                                'partitionColumn': 'encounter_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 500})
        
                                                
        
        location = super().getDataFromMySQL('amrs', 'location', {
                                                'partitionColumn': 'location_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 1})\
                                                .select('location_id', 'name', 'uuid')\
                                                .withColumnRenamed('name', 'location_name')\
                                                .withColumnRenamed('uuid', 'locationuuid')\
                                                .alias('location')
        

        
        
        provider = super().getDataFromMySQL('amrs', 'provider', {
                                                'partitionColumn': 'provider_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 300})\
                                                 .select('provider_id', 'person_id', 'uuid')
        
        provider_with_name = provider.join(person_name, on=person_name['person_id'] == provider['person_id'], how='left')\
                                                 .withColumnRenamed('uuid', 'provider_uuid')\
                                                 .withColumnRenamed('person_name', 'provider_name')\
                                                 .alias('provider')\
                                                 .drop('person_id', 'patient_uuid')
        
        patient_identifier = super().getDataFromMySQL('amrs', 'patient_identifier', {
                                                'partitionColumn': 'patient_id',
                                                'fetchsize':4566,
                                                'lowerBound': 1,
                                                'upperBound': 9000000,
                                                'numPartitions': 300}).select('patient_id', 'identifier')

      
        transformed_orders = lab_orders.join(encounter, on='encounter_id', how='left')\
                  .join(f.broadcast(location), on='location_id', how='left')\
                  .join(f.broadcast(provider_with_name), on=lab_orders['orderer'] == provider_with_name['provider_id'], how='left')\
                  .join(patient_identifier, on=lab_orders['patient_id'] == patient_identifier['patient_id'], how='left')\
                  .join(obs, on=obs['order_id'] == lab_orders['order_id'], how='left')\
                  .select('lab_orders.*', 'provider.*', 'obs.sample_collection_date', 'identifier', 'obs.obs_uuid', 'location.*')
        
        final = transformed_orders.groupBy('lab_orders.order_id').agg(
                      *[f.first(col).alias(col) for col in transformed_orders.columns],
                      f.collect_set(f.col('identifier')).alias('identifiers')
        )\
    .withColumn('build_date', f.current_timestamp())\
    .withColumn('type', f.lit('lab_orders'))\
    .withColumn('couch_id', f.concat_ws('_', f.lit('lab_orders'), f.col('person_id'), f.col('encounter_id'), f.col('uuid')))\
    .withColumn('person_uuid', f.col('patient_uuid'))\
    .drop('order_id', 'orderer', 'provider_id', 'identifier', 'order_type_id')
        
        return lab_orders
    
                