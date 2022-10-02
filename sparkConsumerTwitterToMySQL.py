import os

import os
import configuration as c
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, BooleanType
import connectToMySQL as mySQLDB
from pyspark.sql.functions import lit
import logHandler

def saveToMySQL():
    # conection between  spark and kafka
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    bootstrapServers = c.brokers
    topics = c.topic


    spark = SparkSession\
            .builder\
            .config("spark.jars", "/home/naya/mySQL-connectors/mysql-connector-java-8.0.30.jar")\
            .appName("ReadTweets")\
            .getOrCreate()


    # Construct a streaming DataFrame that reads from topic
    df_kafka = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', bootstrapServers) \
                .option('subscribe', topics) \
                .option('startingOffsets', 'latest') \
                .load()


    # .add("geo", MapType()) \
    # .add("coordinates", IntegerType()) \

    #Create schema for create df from json
    schema = StructType() \
        .add("tweet_created_at", StringType()) \
        .add("tweet_id", StringType()) \
        .add("text", StringType()) \
        .add("user_account_created_at", StringType()) \
        .add("user_id", StringType()) \
        .add("name", StringType()) \
        .add("location", StringType()) \
        .add("tweetUrls", StringType()) \
        .add("hashtags", StringType())

    df_allTweets = df_kafka.select(col("value").cast("string"))\
        .select(from_json(col("value"), schema).alias("value"))\
        .select("value.*")

    df_tweets = df_allTweets.select(col("tweet_id").alias('tweetID'), col("location").alias('countryPublished'),
                                    col("tweetUrls").alias('ContentLink'), col("text").alias('Brief'),
                                    col("hashtags").alias("keywordsOrHashtags"))


    df_tweets = df_tweets.withColumn("reportingDate", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss")) \
                         .withColumn("platform", lit("Twitter")) \
                         .withColumn("categoryType", lit(None).cast(StringType())) \
                         .withColumn("language", lit("English")) \
                         .withColumn("language2", lit(None).cast(StringType())) \
                         .withColumn("logiReported", lit("N").cast(StringType())) \
                         .withColumn("logiRemoved", lit("N").cast(StringType()))\
                         .withColumn("screenshotLink",  lit(None).cast(StringType())) \
                         .withColumn("isThreat", lit(None).cast(StringType())) \
                         .withColumn("updatedVolName", lit(None).cast(StringType())) \
                         .withColumn("Vol_Email", lit(None).cast(StringType())) \
                         .withColumn("updatedVolDate", lit(None).cast(StringType()))

    connectMySqlDB = mySQLDB.mySqlHandle()
    # connectMySqlDB.createTables()

    try:
        insertToMYSQL = df_tweets\
            .writeStream\
            .foreachBatch(connectMySqlDB.insertDataToTweetReportsTblSpark)\
            .outputMode("append") \
            .start()

        insertToMYSQL.awaitTermination()
    except Exception as e:
        logger = logHandler.myLogger("mySQL", "save tweets")
        logger.logError('error:sparkConsumerTwitterToMySQL.insertToMYSQL ' + str(e))



saveToMySQL()