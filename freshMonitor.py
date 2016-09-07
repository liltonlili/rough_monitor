#coding:utf8
import pymongo
import common
import datetime
import multiprocessing
import numpy as np
import pandas as pd
from Tkinter import *
from tkMessageBox import *
import time

# multiprocessing.freeze_support()
mongoUrl = "localhost"
today = datetime.date.today().strftime("%Y%m%d")
yesToday = common.get_last_date(today)
strYestoday = common.format_date(yesToday,"%Y%m%d")
mongodb = pymongo.MongoClient(mongoUrl)
freshCols = mongodb.stock.ZDT_by_date.find_one({"date":strYestoday})
freshList =freshCols['freshStocks'].split("_")
datelist = []
datelist.append(strYestoday)
lastFrame = common.get_mysqlData(freshList,datelist)
lastFrame['ZT_PRICE']=lastFrame['CLOSE_PRICE'].astype(np.float64)*1.1
lastFrame['ZT_PRICE']=lastFrame['ZT_PRICE'].round(decimals=2)
lastFrame.set_index('TICKER_SYMBOL',inplace=True)
lastFrame = lastFrame['ZT_PRICE']
excludeList = []

while(True):

    time.sleep(5)
    ttime=time.localtime()
    thour=ttime.tm_hour
    tmin=ttime.tm_min
    if (thour > 15) or (thour == 15 and tmin > 1):
        break

    timestamp=time.strftime("%X",time.localtime())
    print timestamp

    curframe = common.get_sina_data(list(set(freshList)))
    curframe.set_index("stcid",inplace=True)
    judframe = pd.concat([lastFrame,curframe],axis=1)
    buFrame = judframe[(judframe.close < judframe.ZT_PRICE) | (judframe.low < judframe.ZT_PRICE)]

    if len(buFrame) == 0:
        continue

    tellList = buFrame.index.values
    tellName = buFrame.name.values
    tellStr = "_".join(tellList)
    tellNameStr = "_".join(tellName)
    common.showinfos(u"开班啦\n%s\n%s"%(tellStr,tellNameStr))
    excludeList.extend(tellList)
    freshList = [x for x in freshList if x not in excludeList]

    if len(freshList) < 1:
        break

print "Today's monitor is done"