
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, Row, SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
from pyspark import SparkContext
import datetime
import time
import pyspark.sql.functions as f
import datetime
from obs_job import ObsJob 
from job import Job
from config import getConfig


import os
spark_submit_str = ('--driver-memory 45g --executor-memory 3g --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2'
                    ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                    ' --jars /home/jovyan/jars/spark-cassandra-connector.jar,/home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                    ' pyspark-shell')

os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str


config = getConfig()
def save_to_cassandra(df, table):
             df.write.format("org.apache.spark.sql.cassandra")\
                    .options(table=table, keyspace="amrs")\
                    .mode("append")\
                    .save()
            
             print("Finished loading to cassandra" + time.ctime()) 


# In[4]:


spark = SparkSession.builder\
.config('spark.sql.repl.eagerEval.enabled', True)\
    .config('cloudant.host', '10.50.80.115:5984')\
        .config('cloudant.username', config['couch']['username'])\
            .config('cloudant.password', config['couch']['username'])\
                .config('cloudant.protocol', 'http')\
                    .config('spark.rdd.compress', True)\
                        .config('spark.sql.crossJoin.enabled', True)\
                            .config("jsonstore.rdd.maxInPartition", 500).\
                                config("jsonstore.rdd.minInPartition", 1000)\
                                    .config("cloudant.useQuery", "true")\
                                        .config("jsonstore.rdd.requestTimeout", 90000000)\
                                            .config("spark.sql.shuffle.partitions", 1000)\
                                                .config("schemaSampleSize",1)\
                                                    .getOrCreate()
spark.sparkContext.setLogLevel('INFO')


# In[5]:

class EncounterJob(Job):
        def saveToCouchDB(self, dataframe, database):
            dataframe.write.save(database,"org.apache.bahir.cloudant",
                                  bulkSize="800", createDBOnSave="false")


        
        # In[7]:


        def get_provider(self):
                provider = super().getDataFromMySQL('amrs', 'provider', {
                    'partitionColumn': 'provider_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 200})\
                .select('uuid', 'identifier', 'provider_id', 'person_id')\
                .withColumnRenamed('uuid', 'provider_uuid')\
                .withColumnRenamed('identifier', 'provider_identifier')\
                .alias('provider')

                person = super().getDataFromMySQL('amrs', 'person_name', {
                    'partitionColumn': 'person_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 200})\
                .select('given_name', 'family_name', 'middle_name', 'person_id')\
                .alias('person_name')

                return provider.join(person, on='person_id', how='left')\
                               .withColumn('provider_name', f.concat_ws(' ', f.col('given_name'),  f.col('middle_name'), f.col('family_name')))\
                               .drop('given_name', 'family_name', 'middle_name')


        # In[8]:


        def get_encounter_providers(self):
                encounter_provider = super().getDataFromMySQL('amrs', 'encounter_provider', {
                    'partitionColumn': 'provider_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 450})\
                .select('uuid', 'encounter_id', 'provider_id')\
                .withColumnRenamed('uuid', 'encounter_provider_uuid')\
                .alias('enc_provider')

                provider = self.get_provider()


                return encounter_provider.join(provider, 'provider_id')




        # In[9]:


        def get_encounter_types(self):
            encounter_type = super().getDataFromMySQL('amrs', 'encounter_type', {
                    'partitionColumn': 'encounter_type_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 10000,
                    'numPartitions': 1})\
            .select('uuid', 'name', 'encounter_type_id')\
            .withColumnRenamed('uuid', 'encounter_type_uuid')\
            .withColumnRenamed('name', 'encounter_type_name')

            return encounter_type


        # In[10]:


        def get_forms(self):
            forms = super().getDataFromMySQL('amrs', 'form', {
                    'partitionColumn': 'form_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 10000,
                    'numPartitions': 10})\
            .select('form_id', 'uuid', 'name')\
            .withColumnRenamed('uuid', 'form_uuid')\
            .withColumnRenamed('name', 'form_name')

            return forms


        # In[11]:


        def get_locations(self):
            location = super().getDataFromMySQL('amrs', 'location', {
                    'partitionColumn': 'location_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 1})\
            .select('uuid', 'name', 'location_id')\
            .withColumnRenamed('uuid', 'location_uuid')\
            .withColumnRenamed('name', 'location_name')

            return location


        # In[12]:


        def get_visits(self):
            visit = super().getDataFromMySQL('amrs', 'visit', {
                    'partitionColumn': 'visit_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 100})\
            .select('uuid', 'date_started', 'date_stopped', 'visit_type_id', 'visit_id', 'location_id')\
            .withColumnRenamed('uuid', 'visit_uuid')

            visit_type = super().getDataFromMySQL('amrs', 'visit_type', {
                    'partitionColumn': 'visit_type_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 45000000,
                    'numPartitions': 1})\
            .select('uuid', 'name', 'visit_type_id')\
            .withColumnRenamed('uuid', 'visit_type_uuid')\
            .withColumnRenamed('name', 'visit_type_name')

            locations = self.get_locations()

            return visit.join(visit_type, on='visit_type_id')\
                        .join(f.broadcast(locations), on='location_id')\
                        .drop('visit_type_id', 'location_id')\
                        .alias('visit')


        # In[13]:


        def get_patients(self):
            person = super().getDataFromMySQL('amrs', 'person', {
                    'partitionColumn': 'person_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 10000000,
                    'numPartitions': 200})\
            .select('uuid', 'person_id')\
            .withColumnRenamed('uuid', 'person_uuid')

            patient = super().getDataFromMySQL('amrs', 'patient', {
                    'partitionColumn': 'patient_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 1000000,
                    'numPartitions': 200})\
            .select('patient_id')


            return person.join(patient, on=f.col('person_id') == f.col('patient_id')).drop('person_id')


        # In[14]:


        def get_encounters(self):
            encounters = super().getDataFromMySQL('amrs', 'encounter', {
                    'partitionColumn': 'encounter_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 10000000,
                    'numPartitions': 100})\
                    .alias('encounter')

            return encounters


        # In[15]:


        def get_concepts(self):
            concepts = super().getDataFromMySQL('amrs', 'concept', {
                    'partitionColumn': 'concept_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 20000,
                    'numPartitions': 10})\
            .select('uuid', 'concept_id')\
            .withColumnRenamed('uuid', 'concept_uuid')

            concept_names = super().getDataFromMySQL('amrs', 'concept_name', {
                    'partitionColumn': 'concept_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 50000,
                    'numPartitions': 10})\
            .filter(f.col('locale_preferred') == 1)\
            .select('name', 'concept_id')\
            .withColumnRenamed('name', 'concept_name')

            return concepts.join(concept_names, on='concept_id')


        # In[16]:


        def get_orders(self):
            orders = super().getDataFromMySQL('amrs', 'orders', {
                    'partitionColumn': 'encounter_id', 
                    'fetchsize':4566,
                    'lowerBound': 1,
                    'upperBound': 10000000,
                    'numPartitions': 200})\
            .filter(f.col('voided') == 0)\
            .select('uuid', 'encounter_id', 'concept_id', 'orderer',
                    'order_action', 'date_activated', 'date_created',
                    'urgency', 'order_type_id', 'order_number')\
            .withColumnRenamed('uuid', 'order_uuid')

            order_type = super().getDataFromMySQL('amrs', 'order_type', {
                'partitionColumn': 'order_type_id', 
                'fetchsize':4566,
                'lowerBound': 1,
                'upperBound': 100,
                'numPartitions': 1})\
            .select('order_type_id', 'name')\
            .withColumnRenamed('name', 'order_type_name')

            concepts = self.get_concepts()

            orderer = self.get_provider()

            return orders.join(f.broadcast(order_type), on='order_type_id')\
                         .join(f.broadcast(concepts), on='concept_id')\
                         .join(f.broadcast(orderer), on=orders['orderer'] == orderer['provider_id'])\
                         .drop('concept_id', 'order_type_id')\
                         .alias('orders')


        # In[17]:


        def get_obs(self):
            return spark.read.format("org.apache.spark.sql.cassandra")\
                            .options(table="obs", keyspace="amrs")\
                            .load()\
                            .alias('obs')


        # In[18]:


        def transform_into_openmrs_object(self, encounter_dataframe):
            return encounter_dataframe.groupBy('encounter.encounter_id').agg(
                f.first('patient_id').alias('person_id'),
                f.lit('encounter').alias('type'),
                f.first('encounter.location_id').alias('location_id'),
                f.first('person_uuid').alias('person_uuid'),
                f.col('encounter.encounter_id').cast('string').alias('couch_id'),
                f.first('uuid').alias('uuid'),
                f.first('encounter_datetime').alias('encounterdatetime'),
                f.struct(
                    f.first('encounter_type_name').alias('display'),
                    f.first('encounter_type_uuid').alias('uuid')
                ).alias('encountertype'),
                f.struct(
                    f.first('form_name').alias('name'),
                    f.first('form_uuid').alias('uuid')
                ).alias('form'),
                f.struct(
                    f.first('location.location_name').alias('display'),
                    f.first('location.location_uuid').alias('uuid')                            
                ).alias('location'),
                f.to_json(f.collect_set(
                    f.when(f.col('encounter_provider_uuid').isNotNull(), f.struct(
                        f.col('encounter_provider_uuid').alias('uuid'),
                        f.col('encounter_provider.provider_name').alias('display'),
                        f.struct(
                            f.col('encounter_provider.provider_uuid').alias('uuid'),
                            f.concat_ws(' ', f.col('encounter_provider.provider_identifier'), f.lit('-'), f.col('encounter_provider.provider_name')).alias('display')
                        ).alias('provider')
                    ))
                )).alias('encounterproviders'),
                f.to_json(f.struct(
                    f.first('visit_uuid').alias('uuid'),
                    f.first('visit.date_started').alias('dateStarted'),
                    f.first('visit.date_stopped').alias('dateStopped'),
                    f.struct(
                        f.first('visit_type_name').alias('name'),
                        f.first('visit_type_uuid').alias('uuid')
                    ).alias('visitType'),
                    f.struct(
                        f.first('visit.location_name').alias('name'),
                        f.first('visit.location_uuid').alias('uuid')
                    ).alias('location'),
                    f.concat_ws(' ', f.first('visit_type_name'), f.lit('@'), f.first('visit.location_name'), f.lit('-'), f.first('visit.date_started'))
                    .alias('display')
                )).alias('visit'),
                f.to_json(f.collect_set(
                    f.when(f.col('order_uuid').isNotNull(),f.struct(
                        f.col('order_uuid').alias('uuid'),
                        f.col('order_number').alias('orderNumber'),
                        f.struct(
                            f.col('orders.concept_uuid').alias('uuid'),
                            f.col('orders.concept_name').alias('display')
                        ).alias('concept'),
                        f.struct(
                            f.col('orders.provider_uuid').alias('uuid'),
                            f.concat_ws(' ', 'orders.provider_identifier', 'orders.provider_name').alias('display')
                        ).alias('orderer'),
                        f.col('order_action').alias('action'),
                        f.col('orders.date_activated').alias('dateActivated'),
                        f.col('orders.date_created').alias('dateCreated'),
                        f.col('orders.urgency').alias('urgency'),
                        f.col('order_type_name').alias('type')
                    )
                ).otherwise(None))).alias('orders'),
                f.to_json(f.collect_list(
                   f.struct(
                         f.lit('obs_uuid_to_be_included').alias('uuid'),
                         f.col('obs_datetime').alias('obsDatetime'),
                         f.struct(
                             f.col('parent_obs_concept_uuid').alias('uuid'),
                             f.struct(
                             f.col('parent_obs_concept_name').alias('display'))
                             .alias('name')
                         ).alias('concept'),
                        f.when(f.col('value_coded').isNotNull(),
                            f.struct(
                                    f.col('value_type').alias('type'),
                                    f.to_json(
                                              f.struct(
                                                  f.col('value_coded_concept_uuid').alias('uuid'),
                                                  f.col('value_coded_concept_name').alias('display')
                                              )).alias('value')
                                    )
                        ).when(f.col('value_not_coded').isNotNull(),
                            f.struct(
                                    f.col('value_type').alias('type'),
                                    f.col('value_not_coded').alias('value')
                                    )
                        ).alias('value'),
                        f.when(f.col('groupmembers').isNotNull(), 
                               f.col('groupmembers')
                              ).alias('groupMembers')
                ))).alias('obs'),
            ).withColumn('build_date', f.current_timestamp())


        # In[19]:

        def run(self):
            ### build obs first
            obs = ObsJob().build_obs()

            ### start working on encounters
            encounters = self.get_encounters()
            forms = self.get_forms()
            locations = self.get_locations().alias('location')
            visits = self.get_visits()
            encounter_providers = self.get_encounter_providers().alias('encounter_provider')
            encounter_types = self.get_encounter_types()
            patients = self.get_patients()
            orders = self.get_orders()

            joined_encounters = encounters.join(f.broadcast(forms), on='form_id')\
            .join(f.broadcast(locations), on='location_id')\
            .join(f.broadcast(visits),on='visit_id')\
            .join(f.broadcast(encounter_types), on=encounters['encounter_type'] == encounter_types['encounter_type_id'])\
            .join(patients, on='patient_id').join(encounter_providers, on=encounter_providers['encounter_id'] == encounters['encounter_id'], how='left')\
            .join(orders, on=orders['encounter_id'] == encounters['encounter_id'], how='left')\
            .join(obs, on=obs['encounter_id'] == encounters['encounter_id'], how='left')\
            .drop('enc_provider.encounter_id', 'obs.encounter_id', 'orders.encounter_id')

            openmrs_encounter_object = self.transform_into_openmrs_object(joined_encounters)
            return openmrs_encounter_object

