###########################################
###                MySQL                ###
###########################################


mysql_host = 'localhost'
mysql_port = 3306
mysql_database_name = 'AntisemitismDB'
mysql_username = 'naya'
mysql_password = '******'
mysql_table_name = 'twitterReportsTbl'
mysql_log_table = 'logTbl'

###########################################
###           Twitter                   ###
###########################################

consumer_key = '******************'
consumer_secret = '*************************'
access_token = '***************************'
access_secret = '**********************************'

###########################################
###            kafka connect            ###
###########################################
topic = 'TweeterData'
topicHdfs = 'From_Kafka_To_Hdfs_Archive_Json'
brokers = 'cnt7-naya-cdh63:9092'

###########################################
###           HDFS                      ###
###########################################
host = 'Cnt7-naya-cdh63'
port = 8020
folderPath = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/finalProject'
folderPathMargeFiles = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/finalProjectMargeFiles'

###########################################
###           HIVE                      ###
###########################################
# HIVE details
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_mode = 'CUSTOM'
hive_db_name = 'AntisemitismDB'
hive_table_name = 'twitter_data_tbl'
hive_location = '/user/naya/finalProject'

###########################################
###            Google Sheets  + gcp     ###
###########################################
# Goggle Sheets Keys
local_path = "/home/naya/finalProject"
gsheets_keys = "/home/naya/finalProject/googleSheetkeys.json"

# Google Sheets Files - All files are in Google Drive
gsheets_path = "https://docs.google.com/spreadsheets"

keywords_file_name = "Keywords"
reports_file_name = "Reports"

ws_slang = "Slang"
ws_en_keywords = "Keywords English"
ws_en_hashtags = "Hashtags English"
ws_en_combinations = "Word Combinations English"

ws_posts = "Posts"


bucket_name = 'final-project-bucket-foa'
###############################################
### Google Sheets - Reports sheet Structure ###
###############################################
gs_tweet_id             = (1,  "A",  0)
gs_category             = (2,  "B",  1)
gs_removed              = (3,  "C",  2)
gs_removed1             = (4,  "D",  3)
gs_date                 = (5,  "E",  4)
gs_first_name           = (6,  "F",  5)
gs_last_name            = (7,  "G",  6)
gs_platform             = (8,  "H",  7)
gs_langauge             = (9,  "I",  8)
gs_content_link         = (10, "J",  9)
gs_other_langauge       = (11, "K", 10)
gs_reported             = (12, "L", 11)
gs_screenshot_link      = (13, "M", 12)
gs_brief                = (14, "N", 13)
gs_email_address        = (15, "O", 14)
gs_country_published    = (16, "P", 15)
gs_any_treat            = (17, "Q", 16)
gs_keywords_or_hashtags = (18, "R", 17)



###########################################
###                 Slack               ###
###########################################
slack_sch_msg_url = "https://hooks.slack.com/services/***********/***********/************************"
slack_ah_msg_url = "https://hooks.slack.com/services/***********/***********/************************"
