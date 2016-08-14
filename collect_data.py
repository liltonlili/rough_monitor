#coding:utf8
import common
import pymongo
from pandas import DataFrame
import ModeStudy


mongourl = "localhost"
start = "20160101"
mongodb = pymongo.MongoClient(mongourl)
results = mongodb.stock.ZDT_by_date.find({"date":{"$gte":start}})
CloseFrame = DataFrame()
NoFrame = DataFrame()






for result in results:
    date = result['date']
    freshList = result['freshStocks'].split("_")
    dframe = common.get_mysqlData(freshList,[date])
    dframe1 = dframe[(dframe.LOWEST_PRICE < dframe.HIGHEST_PRICE) & (dframe.CLOSE_PRICE == dframe.HIGHEST_PRICE)]    #开板收住
    dframe2 = dframe[(dframe.LOWEST_PRICE < dframe.HIGHEST_PRICE) & (dframe.CLOSE_PRICE < dframe.HIGHEST_PRICE)] #开板没收住
    closeList = dframe1.TICKER_SYMBOL.values
    nocloseList = dframe2.TICKER_SYMBOL.values
    plot_dir1 = "D:\data\close"
    plot_dir2 = "D:\data\noclose"
    for stcid in closeList:
        print "close",date,stcid
        try:
            ModeStudy.study_plot(stcid,date,plot_dir1)
        except Exception,err:
            print err
            print "error in close for %s,%s"%(stcid,date)

    for stcid in nocloseList:
        print "noclose",date,stcid
        try:
            ModeStudy.study_plot(stcid,date,plot_dir2)
        except:
            print "error in no-close for %s,%s"%(stcid,date)

