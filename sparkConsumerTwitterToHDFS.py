
import os
import configuration as c
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, BooleanType
import logHandler
#
# fs = pa.hdfs.connect(
#     host='Cnt7-naya-cdh63',
#     port=8020,
#     user='hdfs',
#     kerb_ticket=None,
#     extra_conf=None)
#
# # First we use mkdir() to create a staging area in HDFS
# isExists = pa.HadoopFileSystem.exists(fs, path= c.folderPath)
# if not isExists:
#     fs.mkdir(c.folderPath, create_parents=True)
#     fs.chmod(c.folderPath,777)

# read data from kafka with spark

def saveDataToHDFS():
    # conection between  spark and kafka
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    bootstrapServers = c.brokers
    topics = c.topicHdfs


    spark = SparkSession\
            .builder\
            .config("spark.hadoop.fs.defaultFS", "hdfs://Cnt7-naya-cdh63:8020")\
            .appName("ReadTweetsToHdfs")\
            .getOrCreate()


    # Construct a streaming DataFrame that reads from topic
    df_kafka = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', bootstrapServers) \
                .option('subscribe', topics) \
                .option('startingOffsets', 'latest') \
                .load()

    schema = StructType() \
        .add("tweet_created_at", StringType()) \
        .add("tweet_id", StringType()) \
        .add("text", StringType()) \
        .add("source", StringType()) \
        .add("user_account_created_at", StringType()) \
        .add("user_id", StringType()) \
        .add("name", StringType()) \
        .add("location", StringType()) \
        .add("tweetUrl", StringType()) \
        .add("description", StringType()) \
        .add("followers_count", IntegerType()) \
        .add("friends_count", IntegerType())  \
        .add("listed_count", IntegerType()) \
        .add("favourites_count", IntegerType()) \
        .add("statuses_count", IntegerType())


    df_allTweets = df_kafka.select(col("value").cast("string"))\
        .select(from_json(col("value"), schema).alias("value"))\
        .select("value.*")


    df_allTweets.printSchema()

    try:
        insertToHDFS = df_allTweets\
            .writeStream\
            .format("parquet")\
            .option("path", c.folderPath)\
            .option("checkpointLocation", "/tmp/checkpoint")\
            .outputMode("append") \
            .start()

        insertToHDFS.awaitTermination()
    except Exception as e:
        logger = logHandler.myLogger("HDFS", "save tweets")
        logger.logError('error:sparkConsumerTwitterToHDFS.insertToHDFS ' + str(e))


saveDataToHDFS()