import datetime
import time

from pyspark.sql import SQLContext, Window
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, IntegerType, ArrayType, TimestampType, DoubleType
import pyspark.sql.functions as f

from job import Job


class PatientJob(Job):
    def __init__(self):
           super() 
            
    def run(self):
            print('----> Started Patient Job')
            now = datetime.datetime.now().strftime('%Y-%m-%d')
            patient = super().getDataFromMySQL('amrs', 'patient_view_2', {
                                        'partitionColumn': 'person_id', 
                                        'fetchsize':4566,
                                        'lowerBound': 1,
                                        'upperBound': 2283150,
                                        'numPartitions': 50
            })
            
            transformed_patient =  patient.na.fill("").groupBy('person_id').\
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
                      
            
            
            return transformed_patient

    

    
