from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
from config import getConfig

class Job: 
    
    @staticmethod
    def getSpark():
        global_config = getConfig()
        spark_submit_str = ('--driver-memory 40g --executor-memory 3g --packages org.apache.spark:spark-sql_2.11:2.4.0,org.apache.bahir:spark-sql-cloudant_2.11:2.3.2,com.datastax.spark:spark-cassandra-connector_2.11:2.4.0'
                            ' --driver-class-path /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar' 
                            ' --jars /home/jovyan/jars/mysql-connector-java-5.1.42-bin.jar'
                            ' pyspark-shell')
        os.environ['PYSPARK_SUBMIT_ARGS'] = spark_submit_str
        spark = SparkSession\
        .builder\
        .config('spark.sql.repl.eagerEval.enabled', True)\
        .config('cloudant.host', global_config['couch']['host'])\
        .config('cloudant.username', global_config['couch']['username'])\
        .config('cloudant.password', global_config['couch']['password'])\
        .config('cloudant.protocol', 'http')\
        .config("jsonstore.rdd.partitions", 15000)\
        .config('spark.driver.maxResultSize', "15000M")\
        .config('spark.sql.crossJoin.enabled', True)\
        .config('spark.sql.autoBroadcastJoinThreshold', 0)\
        .config("spark.cassandra.connection.host", global_config['cassandra']['host'])\
        .config("spark.cassandra.auth.username", global_config['cassandra']['username'])\
        .config("spark.cassandra.auth.password", global_config['cassandra']['password'])\
        .config("spark.cassandra.output.consistency.level", "ANY")\
        .config("spark.sql.shuffle.partitions", 1000)\
        .getOrCreate()
        spark.sparkContext.setLogLevel("INFO") 
        return spark
                
    def getDataFromMySQL(self, dbName, tableName, config):  
       
        spark = Job.getSpark()
        global_config = getConfig()
        return spark.read.format("jdbc").\
          option("url", "jdbc:mysql://"+global_config['mysql']['host']+ ":" +global_config['mysql']['port']+ "/" + dbName + "?zeroDateTimeBehavior=convertToNull").\
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
    
    
    def saveToCouchDB(self, dataframe, database, createDBOnSave='false'):
        dataframe.write.save(database,"org.apache.bahir.cloudant",
                              bulkSize="800", createDBOnSave=createDBOnSave)

