import pygsheets as gc
import mysql.connector as mc

import configuration as c
import connectToMySQL as mySQLDB
import logHandler as log


#########################################################
###     to handle pygsheets API related functions     ###
#########################################################

##
## Function Name: connectByName
##
## Connect to Google sheets.
## File should be in the GCP user's Google drive
##
## Input parameter:
##      spredsheet - Name of Google Sheets document
##      workbook - Name of workbook
##
def connectByName(spredsheet, workbook):
    gs = gc.authorize(service_account_file=c.gsheets_keys)
    sh = gs.open(spredsheet)
    wks = sh.worksheet_by_title(workbook)
    return wks


##
## Function Name: getSingleRow
##
## Fetch one row from the worksheet
##
##
## Input parameter:
##      workbook
##      n - row number
##
def getSingleRow(wks, n):
    return wks.get_row(n, include_tailing_empty=True)


##
## Function Name: getSingleColumn
##
## Fetch one column from the worksheet.
## Column number is in numbers and not letters.
## i.e. the first column is 1 and not A
##
## Input parameter:
##      n - Column number
##
def getSingleColumn(wks, n):
    return wks.get_col(n, include_tailing_empty=False)


##
## Function Name: getFirstColumn
##
## Get the data of the first column (Column A)
##
## Input parameter:
##      header - Boolean. True - Includes the first row
##                        False - Start on the second row (First row is a title)
##
def getFirstColumn(wks, header):
    data = getSingleColumn(wks, 1)

    if header:
        return data
    else:
        return data[1:]


##
## Function Name: gsheetsDataFrame
##
## Fetch all data from wokrsheet.
## Return dataFrame
##
## Input parameter:
##     None
##
def gsheetsDataFrame(wks):
    return wks.get_as_df(include_tailing_empty_rows=False)


##
## Function Name: gsheetsToJson
##
## Fetch all data from worksheet.
## Return data in Json format
##
## Input parameter:
##     None
##
def gsheetsToJson(wks):
    return wks.get_as_df().to_json()


##
## Function Name: getLastRowNo
##
## Get last row num with data
##
## Input parameter:
##     None
##
def getLastRowNo(wks):
    cells = wks.get_all_values(include_tailing_empty_rows=False, include_tailing_empty=False, returnas='matrix')
    return len(cells)


##
## Function Name: getTweetRowNo
##
##  Tweet ID is a unique number and it is written in the first column (A).
##  Fetch the line number of a certain tweet id
##  If tweet ID was not found, return zero (0)
##
## Input parameter:
##      tweetID - Tweet ID to find
##
def getTweetRowNo(wks, tweetID):
    # Bring the exact string (tweet id)
    tweetID_col = c.gs_tweet_id[0]
    cells = wks.find(pattern=str(tweetID), matchEntireCell=True, cols=[tweetID_col, tweetID_col])

    if (len(cells) == 0):
        return 0
    else:
        return cells[0].row


##
## Function Name: getKeywords
##
##  Fetch all keywords from a certain workbook.
##
## Input parameter:
##      workbook - Name of workbook
##      header - Boolean. True - Includes the first row
##                        False - Start on the second row (First row is a title)
##
def getKeywords(workbook, header):
    wks = connectByName(c.keywords_file_name, workbook)

    words = getFirstColumn(wks, header)

    clean_list = [word for word in words if word != '']

    return (clean_list)


##
## Function Name: getAllKeywords
##
##  Fetch all keywords from keywords file.
##
## Input parameter:
##      None
##
def getAllKeywords():
    slang = getKeywords(c.ws_slang, True)
    keywords = getKeywords(c.ws_en_keywords, True)
    hashtags = getKeywords(c.ws_en_hashtags, False)
    combination = getKeywords(c.ws_en_combinations, False)

    words = slang + keywords + hashtags + combination
    words_set = set(words)
    words_unique = list(words_set)

    return (words_unique)


##
## Function Name: markRemovedTweets
##
##  Change remove column in Google Sheets to "Removed"
##
## Input parameter:
##      tweetID - Tweet ID to update
##
def markRemovedTweets(wks, tweetID):
    row_no = getTweetRowNo(wks, tweetID)

    if row_no > 0:
        cell_no = c.gs_removed[1] + str(row_no)
        wks.update_value(cell_no, "Removed")


##
## Function Name: categoryUpdate
##
##  Update category column in MySQL according to the value in Google Sheets
##
## Input parameter:
##      tweetID - Tweet ID to update
##
def categoryUpdate(wks, update_date):
    db = mySQLDB.mySqlHandle()

    date_str = update_date.strftime("%d-%m-%Y")

    cells = wks.find(pattern=date_str, matchEntireCell=True, cols=[c.gs_date[0], c.gs_date[0]])

    if (len(cells) > 0):

        for cell in cells:
            tweetID = wks.get_value(c.gs_tweet_id[1] + str(cell.row))
            category = wks.get_value(c.gs_category[1] + str(cell.row))

            results = db.selectDataInTweetReportsTbl("tweetID={}".format(tweetID))

            if (results is None or len(results) == 0) :
                continue;
            data = list(results[0])

            if (data[2] != category):

                if (data[8] == 'Y'):
                    isReported = 'Y'
                else:
                    isReported = 'N'

                dict = {"_id": data[0],
                        "reportingDate": "date_format('{}','%Y-%m-%d %T')".format(data[1]),
                        "categoryType": category,
                        "platform": data[3],
                        "language": data[4],
                        "language2": data[5],
                        "ContentLink": data[7],
                        "logiReported": isReported,
                        "logiRemoved": data[9],
                        "screenshotLink": data[10],
                        "Brief": data[11],
                        "countryPublished": data[12],
                        "isThreat": data[13],
                        "keywordsOrHashtags": data[14],
                        "updatedVolName": data[15],
                        "Vol_Email": data[16],
                        }

                db.updateDataInTweetReportsTbl(dict)
                print('{} rows affected '.format(len(dict)))

    db.closeConnection()


##
## Function Name: copyTweetMySQLToSheets
##
##  Copy tweet from MySQL to Google Sheets
##
##
## Input parameter:
##      tweetID - Tweet ID to copy
##
def copyTweetMySQLToSheets(wks, tweetID):
    logger = log.myLogger("gsheetsHandler", "copyTweetMySQLToSheets")

    print("Type: ", type(tweetID))
    try:
        db = mySQLDB.mySqlHandle()

        results = db.selectDataInTweetReportsTbl("tweetID = {}".format(tweetID))

        db.closeConnection()

        data = list(results[0])

    except mc.Error as e:
        logger.logError("Error reading data from MySQL table. Error: {}".format(e))
        return

    sheet_row = []
    sheet_row.append(str(data[6]))  # Tweet_ID
    sheet_row.append(data[2])  # Category

    if (data[9] == "Y"):  # Removed
        sheet_row.append("Removed")
    else:
        sheet_row.append("Not Removed")

    sheet_row.append(None)  # Removed 1
    sheet_row.append(data[1].strftime("%d-%m-%Y"))  # Date
    sheet_row.append(data[15]) # First name
    sheet_row.append(None) # Last name
    sheet_row.append(data[3])  # Platform
    sheet_row.append(data[4])  # Langauge
    sheet_row.append(data[7])  # Content Link
    sheet_row.append(None)  # Other language?  Please specify below.
    sheet_row.append(data[8])  # Reported?
    sheet_row.append(data[10])  # screenshot of content
    sheet_row.append(data[11])  # brief
    sheet_row.append(data[16])  # Email address
    sheet_row.append(data[12])  # Country published
    sheet_row.append(data[13])  # any threat?
    sheet_row.append(data[14])  # keywords or hashtags\

    row_no = getTweetRowNo(wks, data[6])

    if (row_no == 0):
        last_row = getLastRowNo(wks)
        wks.insert_rows(row=last_row, values=sheet_row)
    else:
        wks.update_row(row_no, sheet_row)

##
## Function Name: copyTweetSheetsToMySQL
##
##   Copy tweet from Google shhets to MySQL
##
## Input parameter:
##      tweetID - Tweet to copy
##
def copyTweetSheetsToMySQL(wks, tweetID):
    logger = log.myLogger("gsheetsHandler", "copyTweetSheetsToMySQL")

    row_no = getTweetRowNo(wks, tweetID)

    if (row_no > 0):
        row = getSingleRow(wks, row_no)

        try:
            db = mySQLDB.mySqlHandle()

            results = db.selectDataInTweetReportsTbl("tweetID={}".format(tweetID))

            if (len(results) == 0):

                if (row[c.gs_removed[0]] == "Removed"):
                    removed = "Y"
                else:
                    removed = "N"

            full_name = "{} {}".format(row[c.gs_first_name[2]], row[c.gs_last_name[2]])

            dict = {"reportingDate": "str_to_date('{}','%Y-%m-%d %T')".format(row[c.gs_date[2]]),
                    "categoryType": row[c.gs_category[2]],
                    "platform": row[c.gs_platform[2]],
                    "language": row[c.gs_langauge[2]],
                    "language2": row[c.gs_other_langauge[2]],
                    "tweetID": int(row[c.gs_tweet_id[2]]),
                    "ContentLink": row[c.gs_content_link[2]],
                    "logiReported": row[c.gs_reported[2]],
                    "logiRemoved": removed,
                    "screenshotLink": row[c.gs_screenshot_link[2]],
                    "Brief": row[c.gs_brief[2]],
                    "countryPublished": row[c.gs_country_published[2]],
                    "isThreat": row[c.gs_any_treat[2]],
                    "keywordsOrHashtags": row[c.gs_keywords_or_hashtags[2]],
                    "updatedVolName": full_name,
                    "Vol_Email": row[c.gs_email_address[2]],
                    "updatedVolDate": "str_to_date('{}','%Y-%m-%d %T')".format(row[c.gs_date[2]])
                    }

            db.insertDataToTweetReportsTbl(dict)
            db.closeConnection()

        except mc.Error as e:
            logger.logError("Error reading data from MySQL table", e)
            print("Error reading data from MySQL table", e)

    else:
        logger.logDebug("Tweet {} does not exists in sheets".format(tweetID))

