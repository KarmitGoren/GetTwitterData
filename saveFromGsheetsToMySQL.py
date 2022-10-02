import gsheetsHandler as gs
import configuration as c
from datetime import datetime, timedelta
import connectToMySQL as m

yesterday = datetime.now() - timedelta(days=3)  # timedelta(10)
db = m.mySqlHandle()
wks = gs.connectByName(c.reports_file_name, c.ws_posts)


def updateCat():
    global yesterday
    global wks
    print('yesterday = {}'.format(yesterday))

    gs.categoryUpdate(wks, yesterday)


def checkExistingRows(date1, date2):
    global db
    print('count rows in db from {},{} = {}'.format(date1, date2, len(db.selectDataInTweetReportsTbl("reportingDate >= '{}' and reportingDate <='{}'".format(date1, date2)))))


def insertFromGSToMySQL():
    global yesterday
    global wks
    global db
    date_str = yesterday.strftime("%d-%m-%Y")
    cells = wks.find(pattern=date_str, matchEntireCell=True, cols=[c.gs_date[0], c.gs_date[0]])
    print('count of rows {}'.format(len(cells)))
    if (len(cells) > 0):
        for cell in cells:
            tweetID = wks.get_value(c.gs_tweet_id[1] + str(cell.row))
            rec = db.selectDataInTweetReportsTbl("tweetID='{}'".format(tweetID))
            if(len(rec)==0):
                gs.copyTweetSheetsToMySQL(wks, tweetID)
                print('tweetID {} inserted to the DB'.format(tweetID))


updateCat()
checkExistingRows(yesterday.strftime("%d-%m-%Y"), (yesterday + timedelta(days=1)).strftime("%d-%m-%Y"))
insertFromGSToMySQL()

