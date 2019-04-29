from pyspark.sql import SparkSession, Row, SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
from pyspark import SparkContext
import datetime
import time
import pyspark.sql.functions as f
import datetime
import os
from openmrs_data import *
import mappings as m




def transform_obs(all_obs=None, parent_obs=None, child_obs=None):
        concept_with_names = get_concepts().cache()
        
        drug = get_drug()
        
        
        if(parent_obs==None and child_obs==None):
                parent_obs = all_obs\
        .filter(f.col('obs_group_id').isNull())\
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
                        .withColumnRenamed('concept_name', 'parent_obs_concept_name')\
                        .withColumnRenamed('concept_uuid', 'parent_obs_concept_uuid')
        
                child_obs = all_obs\
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
                        .withColumnRenamed('concept_name', 'obs_concept_name')\
                        .withColumnRenamed('concept_uuid', 'obs_concept_uuid')\
                        .withColumnRenamed('uuid', 'child_obs_uuid')
        
        
        obs_group = child_obs.join(parent_obs, parent_obs['parent.obs_id'] == child_obs['obs_group_id'], how='inner')
        
        processed_obs_group = process_obs_group(obs_group, concept_with_names, drug)
        

        obs_without_group = parent_obs\
                        .join(f.broadcast(concept_with_names), f.col('value_coded') == concept_with_names['concept_id'], how='left')\
                        .withColumnRenamed('concept_name', 'value_coded_concept_name')\
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
                       .select('obs_id', 'obs_datetime', 'value_not_coded', 'value_coded', 'parent_obs_concept_name', 'uuid',
                           'value_type', 'encounter_id','value_coded_concept_name', 'value_coded_concept_uuid', 'parent_obs_concept_uuid')
        
        
        
        final_product = obs_without_group.join(processed_obs_group, processed_obs_group['obs_group_id'] == obs_without_group['obs_id'], how='left');
        return final_product
        


def process_obs_group(obs_group, concept_with_names, drug):

    
    obs_group_with_value_names =  obs_group\
                        .join(f.broadcast(concept_with_names), obs_group['child.value_coded'] == concept_with_names['concept_id'], how='left')\
                        .withColumnRenamed('concept_name', 'value_coded_concept_name')\
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
                f.col('child_obs_uuid').alias('uuid'),
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
                    ))
                ).alias('value'),
                f.col('value_type').alias('type')).alias('value')
            )
        )).alias('groupmembers')
    )
     






def transform_encounter(encounter_dataframe, obs_dataframe, streaming=True, filters=None):
        
        
        if(streaming):
            orders = get_orders(filters['encounter_ids'])
            encounter_types = get_encounter_types()
            forms = get_forms(filters['form_ids'])
            encounter_providers = get_encounter_providers(filters['encounter_ids']).alias('encounter_provider')
            locations = get_locations(filters['location_ids']).alias('location')
            visits = get_visits(locations, filters['visit_ids'])
            patients = get_patients(filters['patient_ids'])

        else:
            forms = get_forms()
            locations = get_locations().alias('location')
            visits = get_visits(locations)
            encounter_types = get_encounter_types()
            patients = get_patients()
            orders=get_orders()
            encounter_providers = get_encounter_providers().alias('encounter_provider')
            
        obs = obs_dataframe.alias('obs')
        joined_encounters = encounter_dataframe.join(f.broadcast(forms),on='form_id')\
                                      .join(f.broadcast(locations), on='location_id')\
                                      .join(f.broadcast(visits), on='visit_id')\
                                      .join(f.broadcast(encounter_types), on=encounter_dataframe['encounter_type'] == encounter_types['encounter_type_id'])\
                                      .join(patients, on='patient_id')\
                                      .join(encounter_providers, on=encounter_providers['encounter_id'] == encounter_dataframe['encounter_id'], how='left')\
                                      .join(orders, on=orders['encounter_id'] == encounter_dataframe['encounter_id'], how='left')\
                                      .join(obs, on=obs['encounter_id'] == encounter_dataframe['encounter_id'], how='left')
        

        return joined_encounters\
        .groupBy('encounter.encounter_id').agg(
        f.first('patient_id').alias('person_id'),
        f.lit('encounter').alias('type'),
        f.first('encounter.location_id').alias('location_id'),
        f.first('person_uuid').alias('person_uuid'),
        f.col('encounter.encounter_id').cast('string').alias('couch_id'),
        f.first('encounter.uuid').alias('uuid'),
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
                 f.lit('obs.uuid').alias('uuid'),
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
    
    
    
def transform_hiv_summary(hiv_summary_dataframe):
          get_drug_names = f.udf(m.get_drug_names, StringType())
          return  hiv_summary_dataframe\
            .withColumn('type', f.lit('hiv_summary'))\
            .withColumn('couch_id', f.concat_ws('_', f.lit('hivsummary'), f.col('person_id'), f.col('encounter_id')))\
            .withColumn('build_date', f.current_timestamp())\
            .withColumn('cur_arv_meds_names', get_drug_names(f.col('cur_arv_meds')))\
            .withColumn('arv_first_regimen_names', get_drug_names(f.col('arv_first_regimen')))\
            .withColumn('cur_arv_meds_strict_names', get_drug_names(f.col('cur_arv_meds_strict')))\
            .withColumn('prev_arv_meds_names', get_drug_names(f.col('prev_arv_meds')))\
            .drop('cur_arv_meds', 'arv_first_regimen', 'cur_arv_meds_strict', 'prev_arv_meds')\
            .withColumnRenamed('cur_arv_meds_names', 'cur_arv_meds')\
            .withColumnRenamed('cur_arv_meds_strict_names', 'cur_arv_meds_strict')\
            .withColumnRenamed('prev_arv_meds_names', 'prev_arv_meds')\
            .withColumnRenamed('arv_first_regimen_names', 'arv_first_regimen')
        

def transform_vitals(vitals):
    return vitals.withColumn('type', f.lit('vitals'))\
          .withColumn('couch_id', f.concat_ws('_', f.lit('vitals'), f.col('person_id'), f.col('encounter_id')))\
         .withColumn('build_date', f.current_timestamp())


def transform_program_enrollments(program_enrollment):
    return program_enrollment\
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


def transform_lab_orders(lab_orders, streaming=True, filters=None):
         

        
        concept = get_concepts()\
                     .withColumnRenamed('concept_uuid', 'conceptuuid')\
                     .withColumnRenamed('concept_name', 'display')\
                     .alias('concept')
        
        obs = get_obs_for_orders(filters['obs'])\
            .join(f.broadcast(concept), 
                  on=f.col('concept.concept_id') == f.col('value_coded'), 
                  how='left')\
            .withColumnRenamed('concept.display', 'sample_drawn')\
            .alias('obs')
                                
        person_name = get_person_name(filters['person'])\
                            .alias('person_name')
        
        person = get_person(filters['person'])\
                 .alias('person')\
        
        
        encounter = get_encounters(filters['encounter']).select('encounter_id', 'location_id')
        
                                                
        
        location = get_locations()\
                        .withColumnRenamed('location_uuid', 'locationuuid')\
                        .alias('location')
        
        
        
        provider = get_provider(filters['provider'])\
                    .withColumnRenamed('uuid', 'provider_uuid')\
                    .withColumnRenamed('person_name', 'provider_name')\
                    .drop('person_id', 'patient_uuid', 'identifier')\
                    .alias('provider')\
        
        patient_identifier = get_patient_identifier(filters['patient'])

      
        transformed_orders = lab_orders\
                   .join(person_name, on=f.col('lab_orders.patient_id') == f.col('person_name.person_id'), how='left')\
                   .join(person, on=f.col('lab_orders.patient_id') == f.col('person.person_id'), how='left')\
                   .join(f.broadcast(concept), on='concept_id', how='left')\
                  .join(encounter, on='encounter_id', how='left')\
                  .join(f.broadcast(location), on='location_id', how='left')\
                  .join(f.broadcast(provider), on=lab_orders['orderer'] == provider['provider_id'], how='left')\
                  .join(patient_identifier, on=lab_orders['patient_id'] == patient_identifier['patient_id'], how='left')\
                  .join(obs, on=obs['order_id'] == lab_orders['order_id'], how='left')\
                  .select('lab_orders.*', 'provider.*', 'obs.sample_collection_date', 'identifier', 'obs.obs_uuid', 'location.*', 'person.person_id')\
                   .withColumnRenamed('concept_id', 'order_type')
        
        
        return   transformed_orders\
                  .groupBy('lab_orders.order_id')\
                  .agg(
                      *[f.first(col).alias(col) for col in transformed_orders.columns],
                      f.collect_set(f.col('identifier')).alias('identifiers')
                      )\
    .withColumn('build_date', f.current_timestamp())\
    .withColumn('type', f.lit('lab_orders'))\
    .withColumn('couch_id', f.concat_ws('_', f.lit('lab_orders'), f.col('patient_id'), f.col('encounter_id'), f.col('uuid')))\
    .drop('order_id', 'orderer', 'provider_id', 'identifier', 'order_type_id')
        
        
def transform_patient(patient):
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    return patient.na.fill("").groupBy('person_id').\
                   agg(
                      f.first('person_id').alias('couch_id'),
                      f.to_json(f.struct(
                                f.first('person_uuid').alias('uuid'),
                                f.concat_ws(' ', f.first('given_name'), f.first('middle_name'), f.first('family_name')).alias('display'),
                                f.struct(
                                    f.first('person_uuid').alias('uuid'),
                                    f.concat_ws(' ', f.first('given_name'), f.first('middle_name'), f.first('family_name')).alias('display'),
                                    f.first('gender').alias('gender'),
                                    f.first('birthdate').alias('birthdate'),
                                    f.first('dead').alias('dead'),
                                    (f.year(f.to_date(f.lit(now))) - f.year(f.first('birthdate'))).alias('age') ,
                                    f.first('death_date').alias('deathDate'),
                                    f.first('cause_of_death').alias('causeOfDeath'),
                                    f.first('birthdate_estimated').alias('birthdateEstimated'),
                                    f.collect_set(
                                        f.struct(
                                            f.concat_ws(' ', f.col('person_attribute_type_name'), f.lit('='), f.col('person_attribute_value')).alias('display'),
                                            f.col('person_attribute_value').alias('value'),
                                            f.col('person_attribute_uuid').alias('uuid'),
                                            f.col('person_attribute_voided').alias('voided'),
                                            f.struct(
                                                f.col('person_attribute_type_name').alias('display'),
                                                f.col('person_attribute_type_uuid').alias('uuid'),
                                            ).alias('attributeType')
                                        )
                                    ).alias('attributes')
                                ).alias('person'),
                                f.collect_set(
                                    f.struct(
                                         f.col('identifier'),
                                         f.col('identifier_preferred').alias('preferred'),
                                         f.struct(
                                             f.col('identifier_location_name').alias('name'),
                                             f.col('identifier_location_uuid').alias('uuid')
                                        ).alias('location'),
                                        f.struct(
                                            f.col('identifier_type_name').alias('name'),
                                            f.col('identifier_type_uuid').alias('uuid')
                                        ).alias('identifierType'),
                                        
                                  )).alias('identifiers'),
                                  f.struct(
                                       f.first('person_address_city_village').alias('cityVillage'),
                                       f.first('person_address_longitude').alias('longitude'),
                                       f.first('person_address_latitude').alias('latitude'),
                                       f.first('person_address_country').alias('country'),
                                       f.first('person_address_county_district').alias('countyDistrict'),
                                       f.first('person_address_1').alias('address1'),
                                       f.first('person_address_2').alias('address2'),
                                       f.first('person_address_3').alias('address3'),
                                       f.first('person_address_4').alias('address4'),
                                       f.first('person_address_5').alias('address5'),
                                       f.first('person_address_6').alias('address6'),
                                       f.first('person_address_preferred').alias('preferred')
                                  ).alias('preferredAddress')
                               )).alias('patient')).\
                      withColumn('type', f.lit('patient'))
                      
def transform_labs(flat_labs_and_imaging, streaming=True, filters=None):
        
        hiv_summary = get_hiv_summary(filters['encounter'])\
                     .where('is_clinical_encounter = 1')\
                     .groupBy(f.to_date('encounter_datetime').alias('encounter_date'))\
                    .agg(f.first('cur_arv_meds').alias('cur_arv_meds'), f.first('encounter_datetime').alias('encounter_datetime'))
        
        encounter = get_encounters(filters['encounter']).select('encounter_id', 'location_id')
        
        get_drug_names = f.udf(m.get_drug_names, StringType())
    
        labs_object = flat_labs_and_imaging\
                                    .join(encounter, on='encounter_id', how='left')\
                                    .join(hiv_summary, f.col('encounter_datetime') == f.col('test_datetime'), how='left')\
                                    .select('*', f.split('cur_arv_meds', ' ## ').alias('drug_ids_array'))\
                                    .select('*', get_drug_names(f.col('drug_ids_array')).alias('cur_arv_meds_names'))\
                                    .drop('cur_arv_meds')\
                                    .withColumnRenamed('cur_arv_meds_names', 'cur_arv_meds')\
                                    .withColumnRenamed('uuid', 'person_uuid')\
                                    .withColumn('couch_id', f.concat_ws('_', f.lit('labs'), f.col('person_id'), f.col('encounter_id')))\
                                    .withColumn('type', f.lit('labs'))\
                                    .drop('drug_ids_array')
        return labs_object