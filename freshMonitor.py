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
import copy

# multiprocessing.freeze_support()
mongoUrl = "localhost"
today = datetime.date.today().strftime("%Y%m%d")
yesToday = common.get_last_date(today)
strYestoday = common.format_date(yesToday,"%Y%m%d")
mongodb = pymongo.MongoClient(mongoUrl)
freshCols = mongodb.stock.ZDT_by_date.find_one({"date":strYestoday})
freshList =freshCols['freshStocks'].split("_")
# freshList.append("")
datelist = []
datelist.append(strYestoday)
lastFrame = common.get_mysqlData(freshList,datelist)
lastFrame['ZT_PRICE']=lastFrame['CLOSE_PRICE'].astype(np.float64)*1.1
lastFrame['ZT_PRICE']=lastFrame['ZT_PRICE'].round(decimals=2)
lastFrame.set_index('TICKER_SYMBOL',inplace=True)
lastFrame = lastFrame['ZT_PRICE']
excludeList = []
initial_fresh_list = copy.deepcopy(freshList)

while(True):

    time.sleep(5)
    ttime=time.localtime()
    thour=ttime.tm_hour
    tmin=ttime.tm_min
    if (thour > 15) or (thour == 15 and tmin > 1):
        break
    result = mongodb.stock.ZDT_by_date.find_one({"date":today})
    timestamp=time.strftime("%X",time.localtime())
    print "fresh monitor",timestamp
    try:
        # 监控所有的新股股票
        curframe = common.get_sina_data(list(set(initial_fresh_list)))
        curframe.set_index("stcid",inplace=True)
        judframe = pd.concat([lastFrame,curframe],axis=1)
        buFrame = judframe[(judframe.close < judframe.ZT_PRICE) | (judframe.low < judframe.ZT_PRICE)]
        buFrame = buFrame[buFrame.close > 1]
        if len(buFrame) == 0:
            continue

        # 监控已经开板后的股票
        alert = ""
        tmp_dict = {}
        # 监控开板之后的股票继续涨停
        if "today_open_telled" not in result.keys():
            for idx in buFrame.index.values:
                tmp_dict[idx] = buFrame.loc[idx, 'rate']
        else:
            for stock in excludeList:
                tmp_dict[stock] = curframe.loc[stock, 'rate']
                if stock in result["today_open_telled"].keys():
                    previous_ratio = result['today_open_telled'][stock]
                    this_ratio = curframe.loc[stock, 'rate']
                    if this_ratio >= 9 and previous_ratio <= 9:
                        if len(alert) == 0:
                            alert = stock
                        else:
                            alert += "_%s"%stock
        if len(alert) > 2:
            common.showinfos(u"快要回封了\n%s\n%s"%(alert,""))
        mongodb.stock.ZDT_by_date.update({"date":today}, {"$set":{"today_open_telled":tmp_dict}}, True, True)

        # 将buFrame filter为仅仅包含不在excludeList中的股票，用来监控开板
        buFrame['tindex'] = buFrame.index.values
        buFrame['position'] = buFrame['tindex'].apply(lambda x: x in excludeList)
        buFrame = buFrame[buFrame.position == False]
        if len(buFrame) == 0:
            continue
        tellList = buFrame.index.values
        tellName = buFrame.name.values
        tellStr = "_".join(tellList)
        tellNameStr = "_".join(tellName)
        common.showinfos(u"开班啦\n%s\n%s"%(tellStr,tellNameStr))
        excludeList.extend(tellList)
        freshList = [x for x in freshList if x not in excludeList]
    except Exception,err:
        common.showinfos("Error for freshMonitor")
        print err

print "Today's monitor is done for fresh Monitor"