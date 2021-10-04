from pyspark.sql.session import SparkSession


#if we are not using cluster mode we can utilize this
def dynamic_suffle_partion(spark : SparkSession):

    numExecutors = int(spark.conf.get("spark.executor.instances"))
    numExecutorsCores = int(spark.conf.get("spark.executor.cores"))
    numShufflePartitions = (numExecutors * numExecutorsCores) * 3
    spark.conf.set("spark.sql.shuffle.partitions", numShufflePartitions)
    

#configuring spark from provided configuration in file
def configure_spark(config):
    # creating spark session
    spark = SparkSession.builder \
        .master(config['application_config']['master_url']) \
        .getOrCreate()

    #Can be used in cluster mode    
    #dynamic_suffle_partion(spark)
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
    
    # spark.conf.set("spark.jars", "./azure-storage-8.6.6.jar,./hadoop-azure-3.3.1.jar")

    for conf in config['spark_config'].keys():
        #print(conf +' : ' + config['spark_config'][conf])
        spark.conf.set(conf, config['spark_config'][conf])
    return spark
