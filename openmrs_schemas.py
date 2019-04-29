from pyspark.sql.types import *


class OpenmrsSchema:
    
    
    def __init__(self):
        pass
    

    def NameUuidType(self):
        return StructType([
                 StructField("name", StringType(), True),
                 StructField("uuid", StringType(), True)
             ])
    
    def DisplayUuidType(self):
        return StructType([
                 StructField("uuid", StringType(), True),
                 StructField("display", StringType(), True),
             ])
    
    def get_visit_schema(self):
        visit_schema = StructType([
             StructField("uuid", StringType(), True),
             StructField("dateStarted", TimestampType(), True),
             StructField("dateStopped", TimestampType(), True),
             StructField("visitType", self.NameUuidType(), True),
             StructField("location",self.NameUuidType(), True),
             StructField("display", StringType(), True)
        ])
        return visit_schema
    
    
    def get_encounter_providers_schema(self):
        encounter_providers_schema = ArrayType(
            StructType([
             StructField("uuid", StringType(), True),
             StructField("display", StringType(), True),
             StructField("provider", StructType([
                 StructField("uuid", StringType(), True),
                 StructField("display", StringType(), True)
             ]), True)
        ]))
        return encounter_providers_schema
    
    
    def get_orders_schema(self):
        orders_schema = ArrayType(
         StructType([
            StructField("uuid", StringType(), True),
            StructField("orderNumber", IntegerType(), True),
            StructField("concept", self.DisplayUuidType(), True),
            StructField("orderer", self.DisplayUuidType(), True),
            StructField("action", StringType(), True),
            StructField("dateActivated", TimestampType(), True),
            StructField("dateCreated", TimestampType(), True),
            StructField("urgency", StringType(), True),
            StructField("type", StringType(), True)
        ]), True)
        
        return orders_schema

    def get_obs_schema(self):
        obs_schema = ArrayType(
         StructType([
            StructField("uuid", StringType(), True),
            StructField("obsDatetime", TimestampType(), True),
            StructField("concept", StructType([
               StructField("uuid", StringType(), True),
               StructField("name", StructType([
                   StructField("display", StringType(), True)
               ]), True),
            ]), True),
            StructField("value", StructType([
                StructField("type", StringType(), True),
                StructField("value", StringType(), True),
            ]), True),
            StructField("groupMembers", StringType(), True)
        ]), True)
        
        return obs_schema
    
    def get_vitals_schema(self):
        return StructType([
            StructField("person_id", IntegerType(), True),
            StructField("uuid", StringType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("encounter_datetime", LongType(), True),
            StructField("location_id", IntegerType(), True),
            StructField("weight", StringType(), True),
            StructField("height", StringType(), True),
            StructField("temp", StringType(), True),
            StructField("oxygen_sat", StringType(), True),
            StructField("systolic_bp", StringType(), True),
            StructField("diastolic_bp", StringType(), True),
            StructField("pulse", StringType(), True)

        ])
        

                
    def get_patient_schema(self):
        
        address_schema = StructType([
            StructField("cityVillage", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("country", StringType(), True),
            StructField("countyDistrict", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("address3", StringType(), True),
            StructField("address4", StringType(), True),
            StructField("address5", StringType(), True),
            StructField("address6", StringType(), True),
            StructField("preferred", StringType(), True)

        ])


        identifier_schema = StructType([
            StructField("identifier", StringType(), True),
            StructField("preferred", BooleanType(), True),
            StructField("location", StructType([
                StructField("name", StringType(), True),
                StructField("uuid", StringType(), True),
            ]), True),
            StructField("identifierType", StructType([
                StructField("name", StringType(), True),
                StructField("uuid", StringType(), True),
            ]), True),
        ])



        attribute_schema = StructType([
            StructField("display", StringType(), True),
            StructField("value", StringType(), True),
            StructField("uuid", StringType(), True),
            StructField("voided", BooleanType(), True),
            StructField("attributeType", StructType([
                StructField("display", StringType(), True),
                StructField("uuid", StringType(), True),
            ]), True)
        ])



        person_schema = StructType([
            StructField("uuid", StringType(), True),
            StructField("display", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birthdate", TimestampType(), True),
            StructField("dead", BooleanType(), True),
            StructField("age", IntegerType(), True),
            StructField("deathDate", TimestampType(), True),
            StructField("causeOfDeath", StringType(), True),
            StructField("birthdateEstimated", BooleanType(), True),
            StructField("attributes", ArrayType(attribute_schema), True)
        ])


        patient_schema = StructType([
            StructField("uuid", StringType(), True),
            StructField("display", StringType(), True),
            StructField("person", person_schema, True),
            StructField("identifiers", ArrayType(identifier_schema), True),
            StructField("preferredAddress", address_schema, True)
        ])
        
        
        return patient_schema;
    
    def get_program_enrollments_schema(self):
        program_enrollments = StructType([
            StructField("patient_id", IntegerType(), True),
            StructField("program_id", IntegerType(), True),
            StructField("date_enrolled", LongType(), True),
            StructField("date_completed", LongType(), True),
            StructField("uuid", StringType(), True),
            StructField("location_id", IntegerType(), True)])
        return program_enrollments
    
    def hiv_summary_schema(self):
        hiv_summary = StructType([
            StructField("date_created", StringType(), True),
            StructField("person_id", IntegerType(), True),
            StructField("uuid", StringType(), True),
            StructField("visit_id", IntegerType(), True),
            StructField("encounter_id", IntegerType(), True),
            StructField("encounter_datetime", IntegerType(), True),
            StructField("encounter_type", IntegerType(), True),
            StructField("is_clinical_encounter", IntegerType(), True),
            StructField("location_id", IntegerType(), True),
            StructField("location_uuid", StringType(), True),
            StructField("visit_num", IntegerType(), True),
            
            StructField("enrollment_date", IntegerType(), True),
            StructField("enrollment_location_id", IntegerType(), True),
            StructField("hiv_start_date", IntegerType(), True),
            StructField("death_date", IntegerType(), True),
            StructField("scheduled_visit", IntegerType(), True),
            StructField("transfer_in", IntegerType(), True),
            StructField("transfer_in_location_id", IntegerType(), True),
            StructField("transfer_in_date", IntegerType(), True),
            StructField("transfer_out", IntegerType(), True),
            StructField("transfer_out_location_id", IntegerType(), True),
            StructField("transfer_out_date", IntegerType(), True),
            StructField("patient_care_status", StringType(), True),
            
            
            StructField("out_of_care", IntegerType(), True),
            StructField("prev_rtc_date", IntegerType(), True),
            StructField("rtc_date", IntegerType(), True),
            StructField("arv_first_regimen", StringType(), True),
            StructField("arv_first_regimen_location_id", IntegerType(), True),
            StructField("arv_first_regimen_start_date", IntegerType(), True),
            StructField("prev_arv_meds", StringType(), True),
            StructField("cur_arv_meds", StringType(), True),
            StructField("cur_arv_meds_strict", StringType(), True),
            StructField("arv_start_date", IntegerType(), True),
            StructField("arv_start_location_id", IntegerType(), True),
            StructField("prev_arv_start_date", IntegerType(), True),
            
            StructField("prev_arv_end_date", IntegerType(), True),
            StructField("prev_arv_line", IntegerType(), True),
            StructField("cur_arv_line", IntegerType(), True),
            StructField("cur_arv_line_strict", IntegerType(), True),
            StructField("cur_arv_line_reported", IntegerType(), True),
            StructField("prev_arv_adherence", StringType(), True),
            StructField("cur_arv_adherence", StringType(), True),
            StructField("hiv_status_disclosed", IntegerType(), True),
            StructField("edd", IntegerType(), True),
            StructField("tb_screen", IntegerType(), True),
            StructField("tb_screening_result", StringType(), True),
            StructField("tb_screening_datetime", IntegerType(), True),
            
            
            StructField("on_ipt", IntegerType(), True),
            StructField("ipt_start_date", IntegerType(), True),
            StructField("ipt_stop_date", IntegerType(), True),
            StructField("ipt_completion_date", IntegerType(), True),
            StructField("on_tb_tx", IntegerType(), True),
            StructField("tb_tx_start_date", IntegerType(), True),
            StructField("tb_tx_end_date", IntegerType(), True),
            StructField("pcp_prophylaxis_start_date", IntegerType(), True),
            StructField("condoms_provided_date", IntegerType(), True),
            StructField("modern_contraceptive_method_start_date", IntegerType(), True),
            StructField("contraceptive_method", StringType(), True),
            StructField("cur_who_stage", IntegerType(), True),
            
            
            StructField("discordant_status", StringType(), True),
            StructField("cd4_resulted", IntegerType(), True),
            StructField("cd4_resulted_date", IntegerType(), True),
            StructField("cd4_1", DoubleType(), True),
            StructField("cd4_1_date", IntegerType(), True),
            StructField("cd4_2", DoubleType(), True),
            StructField("cd4_2_date", IntegerType(), True),
            StructField("cd4_percent_1", DoubleType(), True),
            StructField("cd4_percent_1_date", IntegerType(), True),
            StructField("cd4_percent_2", DoubleType(), True),
            StructField("cd4_percent_2_date", IntegerType(), True),
            StructField("vl_resulted", IntegerType(), True),
            
            
            StructField("vl_resulted_date", IntegerType(), True),
            StructField("vl_1", DoubleType(), True),
            StructField("vl_1_date", IntegerType(), True),
            StructField("vl_2", DoubleType(), True),
            StructField("vl_2_date", IntegerType(), True),
            StructField("vl_order_date", IntegerType(), True),
            StructField("cd4_order_date", IntegerType(), True),
            StructField("hiv_dna_pcr_order_date", IntegerType(), True),
            StructField("hiv_dna_pcr_resulted", IntegerType(), True),
            StructField("hiv_rapid_test_resulted_date", IntegerType(), True),
            StructField("prev_encounter_datetime_hiv", IntegerType(), True),
            StructField("next_encounter_datetime_hiv", IntegerType(), True),
            
            
            StructField("prev_encounter_type_hiv", IntegerType(), True),
            StructField("next_encounter_type_hiv", IntegerType(), True),
            StructField("prev_clinical_datetime_hiv", IntegerType(), True),
            StructField("next_clinical_datetime_hiv", IntegerType(), True),
            StructField("prev_clinical_location_id", IntegerType(), True),
            StructField("next_clinical_location_id", IntegerType(), True),
            StructField("prev_clinical_rtc_date_hiv", IntegerType(), True),
            StructField("next_clinical_rtc_date_hiv", IntegerType(), True),
            StructField("outreach_date_bncd", IntegerType(), True),
            StructField("outreach_death_date_bncd", IntegerType(), True),
            StructField("outreach_patient_care_status_bncd", StringType(), True),
            StructField("transfer_date_bncd", IntegerType(), True),
            StructField("transfer_transfer_out_bncd", StringType(), True)
        ])
        return hiv_summary