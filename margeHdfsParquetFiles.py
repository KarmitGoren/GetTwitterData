import configuration as c
import pyarrow as pa
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import date

#connect To hdfs
fs = pa.hdfs.connect(
    host=c.host,
    port=c.port,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

dateToday = date.today().strftime("%b-%d-%Y")
isExists = pa.HadoopFileSystem.exists(fs, path=c.folderPath)
folderSize = pa.hdfs.HadoopFileSystem.disk_usage(fs, c.folderPath) / pow(1024, 2)

spark = SparkSession.builder\
    .appName("margeParquetFiles") \
    .getOrCreate()


def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def hdfs_delete(path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    fs.delete(spark._jvm.org.apache.hadoop.fs.Path(path), True)
    return True

# block_size = spark._jsc.hadoopConfiguration().get("dfs.blocksize")
# if not block_size:
#     block_size = 134217728 #128MB default in hadoop
# fileSize = math.ceil(folderSize/block_size)
#
#
# fPath = c.folderPathMargeFiles+'/date_'+dateToday
# df = spark.read.parquet(c.folderPath)
# df.coalesce(fileSize).write.parquet(path=c.folderPathMargeFiles)
# # delete_path(spark, c.folderPath)
#


# d = [{'text': 'ProudBoys', 'indices': [92, 102]}, {'text': 'OathKeepers', 'indices': [109, 121]}, {'text': 'DomesticTerrorists', 'indices': [122, 141]}]
# x = [item["text"] for item in d]
# print(x)
# var_string = ', '.join(x)
# print(var_string)

import connectToMySQL as mysql

m=mysql.mySqlHandle()
print(m.selectDataInTweetReportsTbl('tweetid is not null'))

