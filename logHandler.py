import connectToMySQL as mySQLDB


############################################
###     Class to handle log messages     ###
############################################

###
## Sample:
## logger = log.MyLogger("Twitter", "Remove Tweets")
## logger.LogInfo("Tweet id {} was deleted".format(tweetID))
## logger.LogError("Tweet id {} not found".format(tweetID)
##


class myLogger:
    ##
    ## Main function
    ##
    ## Parameters:
    ##      module - The name of the module that related to the message
    ##      module2 - The name of the sub-module that related to the message
    ##
    ##
    def __init__(self, module, module2):
        self.module = module
        self.module2 = module2



    ##
    ## Function Name: logInfo
    ##
    ## Information level messages
    ##
    ## Parameters:
    ##      message - Informative log message
    ##
    ##
    def logInfo(self, message):
        self.__addLog(message, "Info")




    ##
    ## Function Name: logWarn
    ##
    ## Warning level messages
    ##
    ## Parameters:
    ##      message - Warning log message
    ##
    ##
    def logWarn(self, message):
        self.__addLog(message, "Warning")




    ##
    ## Function Name: logError
    ##
    ## Error level messages
    ##
    ## Parameters:
    ##      message - Error log message
    ##
    ##
    def logError(self, message):
        self.__addLog(message, "Error")




    ##
    ## Function Name: logDebug
    ##
    ## Debug level messages
    ##
    ## Parameters:
    ##      message - Debug message
    ##
    ##
    def logDebug(self, message):
        self.__addLog(message, "Debug")



    def __addLog(self, message, level):

        # newLog = Event(appLog=self.appName,
        #                logLevel = type,
        #                Message=message
        #               )
        #
        # self.session.add(newLog)
        # self.session.commit()
        # self.session.close()

        if(len(message) == 0):
            return

        dict = {
                "logModule": self.module,
                "logModule2": self.module2,
                "logLevel": level,
                "logMessage": message
                }

        db = mySQLDB.mySqlHandle()
        db.insertDataToLogTbl(dict)
        db.closeConnection()






    ##
    ## Function Name: cleanLogTable
    ##
    ## Debug level messages
    ##
    ## Parameters:
    ##      days - Number of days to keep.
    ##             If days = 0, all records will be deleted
    ##
    ##
    def cleanLogTable(self, days):

        if days < 0:
            return

        db = mySQLDB.mySqlHandle()
        db.deleteDataInLogTbl("date(logDate) <= date_sub(curdate(), interval {} day)".format(days))
        db.closeConnection()
