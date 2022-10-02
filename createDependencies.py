import configuration as c
import mysql.connector as mc
import pyarrow as pa

mysql_conn = ''
# =======================# Creating DB==============================#
def createDB():
    global mysql_conn
    mysql_conn = mc.connect(
                    user=c.mysql_username,
                    password=c.mysql_password,
                    host=c.mysql_host,
                    port=c.mysql_port,
                    autocommit=True,
                    )

    mysql_cursor = mysql_conn.cursor()
    #create database if needed
    mysql_cursor.execute("create database if not exists {};".format(c.mysql_database_name))
    mysql_cursor.close()

# =======================# Creating tables==============================#
def createTables():
    global mysql_conn
    mysql_cursor_tbl = mysql_conn.cursor()
    mysql_create_tbl_tweet_reports = '''create table if not exists {}.twitterReportsTbl (
                                    _id integer NOT NULL AUTO_INCREMENT,                          
                                    reportingDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    categoryType varchar (4),
                                    platform varchar (100),
                                    language varchar (50),
                                    language2 varchar (50),
                                    tweetID bigint unique,
                                    ContentLink varchar (100),
                                    logiReported char(1),
                                    logiRemoved char(1),
                                    screenshotLink varchar (100),
                                    Brief varchar(1000) COLLATE utf8mb4_general_ci DEFAULT NULL, 
                                    countryPublished varchar (100) COLLATE utf8mb4_general_ci DEFAULT NULL,
                                    isThreat varchar (10),
                                    keywordsOrHashtags varchar (300),
                                    updatedVolName varchar (100) null,
                                    Vol_Email varchar (100) null,
                                    updatedVolDate  datetime null default null,
                                    CONSTRAINT contacts_pk PRIMARY KEY (_id)
                                     );'''.format(c.mysql_database_name)

    mysql_create_tbl_reports = '''create table if not exists {}.userReportsTbl (
                                    _id integer NOT NULL AUTO_INCREMENT,
                                    vol_FirstName varchar (100),
                                    vol_LastName varchar (100),
                                    Vol_Email varchar (100),
                                    reportingDate datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    categoryType varchar (4),
                                    platform varchar (100),
                                    language varchar (50),
                                    language2 varchar (50),
                                    ContentLink varchar (100),
                                    logiReported char(1),
                                    logiRemoved char(1),
                                    screenshotLink varchar (100),
                                    Brief varchar (500),
                                    countryPublished varchar (100),
                                    isThreat varchar (10),
                                    keywordsOrHashtags varchar (300),
                                    updatedVolFName varchar (30),
                                    updatedVolLName varchar (30),
                                    updatedVolDate datetime null default null,
                                    CONSTRAINT contacts_pk PRIMARY KEY (_id)
                    );'''.format(c.mysql_database_name)

    mysql_create_tbl_parameters = '''create table if not exists {}.parametersTBL (
                                        emailReports varchar (100),
                                        emailOperationType varchar (30)
    );'''.format(c.mysql_database_name)

    mysql_create_tbl_logs = """create table if not exists {}.{}
    (
    _id integer NOT NULL AUTO_INCREMENT,
    logDate datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    logModule varchar (50),
    logModule2 varchar(50),    
    logLevel varchar(100) not null,
    logMessage varchar(4000)  not null,
    CONSTRAINT log_pk PRIMARY  KEY(_id) );""".format(c.mysql_database_name,c.mysql_log_table)
    mysql_cursor_tbl.execute(mysql_create_tbl_tweet_reports)
    mysql_cursor_tbl.execute(mysql_create_tbl_reports)
    mysql_cursor_tbl.execute(mysql_create_tbl_parameters)
    mysql_cursor_tbl.execute(mysql_create_tbl_logs)

    mysql_cursor_tbl.close()


# =======================# Creating hdfs folder==============================#
def createHdfsFolders():
    fs = pa.hdfs.connect(
        host=c.host,
        port=c.port,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None)

    # First we use mkdir() to create a staging area in HDFS
    isExists = pa.HadoopFileSystem.exists(fs, path= c.folderPath)
    if not isExists:
        fs.mkdir(c.folderPath, create_parents=True)
        fs.chmod(c.folderPath, 775)

    # First we use mkdir() to create a staging area in HDFS
    isExists = pa.HadoopFileSystem.exists(fs, path= c.folderPathMargeFiles)
    if not isExists:
        fs.mkdir(c.folderPathMargeFiles, create_parents=True)
        fs.chmod(c.folderPathMargeFiles, 775)

createDB()
createTables()
createHdfsFolders()