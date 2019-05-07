from config import getConfig
class BulkDelete:
    
    def __init__(self, location_id, couchdb_name, current_date, previous_date):
        self.location_id = location_id
        self.couchdb_name = couchdb_name
        self.current_date = current_date
        self.previous_date = previous_date
        self.cassandra_tables = ['vitals', 'hiv_summary', 'program_enrollments', 'patient']
        self.initialize_spark()
        
    def initialize_spark(self):
        global_config = getConfig()
        self.spark = SparkSession\
                .builder\
                .config('spark.sql.repl.eagerEval.enabled', True)\
                .config('cloudant.host', global_config['couch']['host'])\
                .config('cloudant.username', global_config['couch']['username'])\
                .config('cloudant.password', global_config['couch']['password'])\
                .config('cloudant.protocol', global_config['couch']['protocol'])\
                .config("jsonstore.rdd.partitions", 200)\
                .config("jsonstore.rdd.requestTimeout", 90000000)\
                .config('spark.rdd.compress', True)\
                .config('cloudant.useQuery', 'false')\
                .config('spark.sql.crossJoin.enabled', True)\
                .config("schemaSampleSize", 1)\
                .config("spark.sql.broadcastTimeout", 1200)\
                .config("spark.sql.shuffle.partitions", 800)\
                .config("spark.sql.autoBroadcastJoinThreshold", 1024*1024*300)\
                .getOrCreate()
        self.spark.sparkContext.setLogLevel('INFO')
        
    def getDataFromAmrsWithConfig(self, tableName, dbName, config):
        global_config = getConfig
        return self.spark.read.format("jdbc").\
          option("url", "jdbc:mysql://" + global_config['mysql']['host'] + ":"+global_config['mysql']['port']+"/"+dbName+"?zeroDateTimeBehavior=convertToNull").\
          option("useUnicode", "true").\
          option("continueBatchOnError","true").\
          option("useSSL", "false").\
          option("user", global_config['mysql']['username']).\
          option("password", global_config['mysql']['password']).\
          option("dbtable",tableName).\
          option("partitionColumn", config['partitionColumn']).\
          option("fetchSize", config['fetchsize']).\
          option("lowerBound", config['lowerBound']).\
          option("upperBound", config['upperBound']).\
          option("numPartitions", config['numPartitions']).\
          load()

        
    def fetch_qualifying_patients(self, date):
        
        query = """(select distinct(person_id) from
                          etl.flat_hiv_summary_v15b
                          where rtc_date between date_sub('{0}', interval 28 day)
                          and date_add('{0}',interval 2 day) and location_id = {1}) foo""".format(date, self.location_id)
        
        qualifying_patients = self.getDataFromAmrsWithConfig(query, 'etl', {
        'partitionColumn': 'person_id', 
        'fetchsize':50,
        'lowerBound': 1500,
        'upperBound': 9000000,
        'numPartitions': 900}).alias('encounter')
        
        return qualifying_patients
    
    def save_to_couchdb(self, dataframe, database, createDBOnSave="false"):
        dataframe.write.save(database,"org.apache.bahir.cloudant",
                          bulkSize="800", createDBOnSave=createDBOnSave)
        
    def read_from_couchdb(self, dbName):
        return self.spark.read.format("org.apache.bahir.cloudant").load(dbName)
    
        
    def read_from_cassandra(self, table, keyspace="amrs"):
        return self.spark.read\
            .format("org.apache.spark.sql.cassandra")\
            .options(table=table, keyspace=keyspace)\
            .load()
    
    def delete_stale_patients_from_couchdb(self, patient_ids_to_be_deleted, couchdb_data, couchdb_name):
        couchdb_data_to_delete =  couchdb_data.join(patient_ids_to_be_deleted, 'person_id')\
        .withColumn('_deleted', f.lit(True))
        self.save_to_couchdb(couchdb_data_to_delete, couchdb_name)
        print('Successfully deleted stale patients from couch!')
        
#     def save_newly_qualifying_patients_to_couchdb(self, cassandra_tables, patients_not_in_couch, couchdb_name):
#         add_patients = AddPatientsToCouchRunner(self.location_id, cassandra_tables, patients_not_in_couch, couchdb_name)
#         add_patients.run()
            
    
    def run(self):
        couchdb_data = self.read_from_couchdb(self.couchdb_name).select('_id', '_rev', 'person_id').alias('couchdb_data').cache()

        today_qualifying_patients = self.fetch_qualifying_patients(self.current_date).alias('todays')
        yesterday_qualifying_patients = self.fetch_qualifying_patients(self.previous_date).alias('yesterdays')

        patients_to_be_deleted = yesterday_qualifying_patients.join(today_qualifying_patients, on='person_id', how='left')\
                                                                .where(f.col('todays.person_id').isNull())

        patients_not_in_couch = today_qualifying_patients.join(couchdb_data, on='person_id', how='left')\
                                .where(f.col('couchdb_data.person_id').isNull())
        
        job.delete_stale_patients_from_couchdb(patients_to_be_deleted, couchdb_data)