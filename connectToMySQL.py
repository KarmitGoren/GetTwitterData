import configuration as c
import mysql.connector as mc
from datetime import datetime, timezone
import logHandler
import gsheetsHandler as gs
# import module sys to get the type of exception
import sys

#===========================connector to mysql====================================#
class mySqlHandle():
    def __init__(self):
        self.mysql_conn = mc.connect(
                user=c.mysql_username,
                password=c.mysql_password,
                host=c.mysql_host,
                port=c.mysql_port,
                autocommit=True,
                database=c.mysql_database_name
                )

        self.mysql_conn.connect()

    def insertDataToTweetReportsTblSpark(self, data, args):
        try:
            if(data.count() == 0):
                return
            db_target_properties = {"user": c.mysql_username, "password": c.mysql_password, "driver":'com.mysql.cj.jdbc.Driver' }
            path = 'jdbc:mysql://' + c.mysql_host + ':' + str(c.mysql_port) + '/' + c.mysql_database_name
            data.show(truncate=False)
            data.write.jdbc(url=path,
                            table="twitterReportsTbl",
                            properties=db_target_properties,
                            mode='append')
            pass
            wks = gs.connectByName(c.reports_file_name, c.ws_posts)
            for row in data.collect():
                gs.copyTweetMySQLToSheets(wks, row["tweetID"])

            self.closeConnection()
        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "save tweets")
            logger.logError('error:connectToMySQL.insertDataToTweetReportsTblSpark ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def insertDataToTweetReportsTbl(self, data):
        # mysql_conn = getConnection()

        insertStatement = """
            INSERT INTO twitterReportsTbl(
                reportingDate ,
                categoryType ,  platform ,
                language , language2 ,
                tweetID , ContentLink ,
                logiReported , logiRemoved ,
                screenshotLink ,  Brief , countryPublished ,
                isThreat ,  keywordsOrHashtags ,
                updatedVolName ,  Vol_Email, updatedVolDate)
            VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
                        '{}', '{}', '{}', '{}', '{}','{}', {}); """
        try:
            mysql_cursor = self.mysql_conn.cursor()
            sql = insertStatement.format(
                data["reportingDate"],
                str(data["categoryType"]), data["platform"],
                data["language"], data["language2"],
                data["tweetID"], data["ContentLink"],
                data["logiReported"], data["logiRemoved"],
                data["screenshotLink"], data["Brief"], data["countryPublished"],
                data["isThreat"], data["keywordsOrHashtags"],
                data["updatedVolName"], data["Vol_Email"], data["updatedVolDate"]
            )
            # print(sql)
            self.mysql_conn.commit()
            mysql_cursor.execute(sql)
            mysql_cursor.close()
            # self.closeConnection()
            pass

        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "insert tweets")
            logger.logError('error:connectToMySQL.insertDataToTweetReportsTbl: ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def deleteDataInTweetReportsTbl(self, dateFrom):
        deleteStatement = """
            delete from twitterReportsTbl
            where  reportingDate <= {}  );"""
        try:
            mysql_cursor = self.mysql_conn.cursor()
            sql = deleteStatement.format(dateFrom)
            # print(sql)
            self.mysql_conn.commit()
            mysql_cursor.execute(sql)
            mysql_cursor.close()
            # self.closeConnection()
            pass
        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "delete tweets")
            logger.logError('error:connectToMySQL.deleteDataInTweetReportsTbl: ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def selectDataInTweetReportsTbl(self, whereClause):
        selectStatement = """select * from twitterReportsTbl"""
        if whereClause != '':
            selectStatement += """ where {};"""
        else:
            selectStatement += ";"
        try:
            mysql_cursor = self.mysql_conn.cursor(buffered=True)
            sql = selectStatement.format(whereClause)
            # print(sql)
            self.mysql_conn.commit()
            mysql_cursor.execute(sql)
            records = mysql_cursor.fetchall()  # new
            mysql_cursor.close()
            # self.closeConnection()
            pass
            return records  # new
        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "select tweets")
            logger.logError('error:connectToMySQL.selectDataInTweetReportsTbl: ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def updateDataInTweetReportsTbl(self, data):
        updateStatement = """
            update twitterReportsTbl
            set  reportingDate = {},
                 categoryType = '{}' ,
                 platform = '{}' ,
                 language = '{}' , language2 = '{}' ,
                 ContentLink = '{}' ,
                 logiReported = '{}' , logiRemoved = '{}' ,
                 screenshotLink = '{}' ,  Brief = '{}' , countryPublished = '{}' ,
                 isThreat = '{}' ,  keywordsOrHashtags = '{}' ,
                 updatedVolName = '{}' ,
                 Vol_Email = '{}' ,updatedVolDate = current_timestamp()
             where _id = {} ; """
        try:
            mysql_cursor = self.mysql_conn.cursor()
            sql = updateStatement.format(
                data["reportingDate"],
                data["categoryType"], data["platform"],
                data["language"], data["language2"],
                data["ContentLink"],
                data["logiReported"], data["logiRemoved"],
                data["screenshotLink"], data["Brief"],
                data["countryPublished"],
                data["isThreat"], data["keywordsOrHashtags"],
                data["updatedVolFName"], data["updatedVolLName"],
                data["Vol_Email"],
                data["_id"]
            )
            # print(sql)
            mysql_cursor.execute(sql)
            self.mysql_conn.commit()
            mysql_cursor.close()
            # self.closeConnection()
            pass
        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "update tweets")
            logger.logError('error:connectToMySQL.updateDataInTweetReportsTbl: ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def insertDataToLogTbl(self, data):
        insertStatement = """
            INSERT INTO logTbl(
                logDate,
                logModule, 
                logModule2, 
                logLevel,
                logMessage)
            VALUES (current_timestamp(), "{}", "{}", "{}", "{}");"""
        try:
            mysql_cursor = self.mysql_conn.cursor()
            sql = insertStatement.format(
                data["logModule"],
                data["logModule2"],
                data["logLevel"],
                data["logMessage"]
            )
            # print(sql)
            self.mysql_conn.commit()
            mysql_cursor.execute(sql)
            mysql_cursor.close()
            # self.closeConnection()
        except Exception as e:
            err = str(e)
            logger = logHandler.myLogger("mySQL", "insert log")
            logger.logError('error:connectToMySQL.insertDataToLogTbl: ' + err[0:1000].replace("\"", '\''))
            self.closeConnection()

    def deleteDataInLogTbl(self, condition):
        logger = log.myLogger("logHandler", "deleteDataInLogTbl")

        deleteStatement = """
            delete from logTbl
            where  {};"""

        mysql_cursor = self.mysql_conn.cursor()
        sql = deleteStatement.format(condition)

        logger.logDebug(sql)

        self.mysql_conn.commit()
        mysql_cursor.execute(sql)
        mysql_cursor.close()
        # self.closeConnection()

    def closeConnection(self):
        self.mysql_conn.close()


# # check
# mySql = mySqlHandle()
# r = mySql.selectDataInTweetReportsTbl('')
# print(r)