import configuration as c
from pyhive import hive
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import matplotlib.pyplot as plt
from google.cloud import storage
import io
import urllib, base64
import datetime as dt

#Create Hive connection
hive_cnx = hive.Connection(
    host=c.host,
    port=c.hive_port,
    username=c.hive_username,
    password=c.hive_password,
    auth=c.hive_mode
)

def createDbAndTables(hive_cnx):
    #create db
    creat_db_stmt = """CREATE DATABASE IF NOT EXISTS {}""".format(c.hive_db_name)
    cursor = hive_cnx.cursor()
    cursor.execute(creat_db_stmt)
    cursor.close()

    # create table
    creat_table_stmt = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} ( tweet_created_at string, tweet_id string, text string,
                                                  source string, user_account_created_at string, 
                                                  user_id string, name string,
                                                  location string, tweetUrl string, description string, 
                                                  followers_count int, friends_count int,
                                                  listed_count int, favourites_count int,statuses_count int
                                                  )
    row format delimited fields terminated by ',' 
    STORED AS PARQUET
    LOCATION '{}'
    """.format(c.hive_db_name, c.hive_table_name, c.hive_location)

    cursor = hive_cnx.cursor()
    cursor.execute(creat_table_stmt)
    cursor.close()


def savePlotToGCPStorage(blob_name):
    # connect to GCP and upload the plot
    storage_client = storage.Client.from_service_account_json(c.gsheets_keys)
    # print(list(storage_client.list_buckets()))

    # This defines the path where the file will be stored in the bucket
    bucket = storage_client.get_bucket(c.bucket_name)
    now = dt.datetime.now().strftime("%Y-%m-%d")
    blob = bucket.blob(blob_name + now + ".png")
    blob.upload_from_filename(c.local_path + '\\'+blob_name+'.png', content_type='image/png')


def createMassegeForSendMail(hive_cnx):
    query1 = "select user_id,name,count(tweet_id) from {}.{} group by user_id,name having count(tweet_id) >1".format(c.hive_db_name, c.hive_table_name)

    query2 = "select location,count(user_id) from {}.{} group by location having count(user_id) >1".format(c.hive_db_name, c.hive_table_name)

    query3 = """select name,max(followers_count) x from {}.{} group by name 
                 order by x desc  limit 5""".format(c.hive_db_name, c.hive_table_name)

    cursor = hive_cnx.cursor()
    cursor.execute(query1)
    data_by_user= cursor.fetchall()

    cursor.execute(query2)
    data_by_location = cursor.fetchall()
    cursor.close()

    cursor.execute(query3)
    data_by_followers = cursor.fetchall()
    cursor.close()
    print(data_by_user)
    print(data_by_location)

    chart_name = 'data_by_user'
    x_val = [x[1] for x in data_by_user]
    y_val = [x[2] for x in data_by_user]
    plt.pie(y_val, labels=x_val, shadow=True)
    plt.savefig(c.local_path+'\\' + chart_name +'.png')
    savePlotToGCPStorage(chart_name)
    plt.show()

    chart_name='location'
    x_val2 = [x[0] for x in data_by_location]
    y_val2 = [x[1] for x in data_by_location]
    plt.pie(y_val2, labels=x_val2,  shadow=True)
    plt.savefig(c.local_path + '\\'+chart_name+'.png')
    savePlotToGCPStorage(chart_name)


    chart_name = 'followers'
    x_val2 = [x[0] for x in data_by_followers]
    y_val2 = [x[1] for x in data_by_followers]
    plt.bar(x_val2, y_val2, color='maroon', width=0.4)
    plt.title('users with high followers count')
    plt.xlabel('user name')
    plt.ylabel('followers count')
    plt.show()
    plt.savefig(c.local_path + '\\' + chart_name + '.png')
    savePlotToGCPStorage(chart_name)

createDbAndTables(hive_cnx)
#createMassegeForSendMail(hive_cnx)
