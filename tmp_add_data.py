#coding:utf8
__author__ = 'li.li'
import pandas as pd
import tushare as ts
import os
import pandas as pd
from pandas import DataFrame
import sys
sys.path.append("D:\Money")
from common import *
import os
import requests
import time
import numpy as np
import multiprocessing
import datetime
import numpy as np
import pymongo
import trace_yesterday




# #



# def get_statistics(stock_list,timestamp):
#     stock_lists=stock_list
#     if len(stock_list)>200:
#         tframe_list=[]
#         for i in range(len(stock_list)/200+1):
#             if 200*i > len(stock_list):
#                 break
#             tframe=get_value(stock_lists[200*i:200*(i+1)],timestamp)
#             tframe_list.append(tframe)
#         tframe=pd.concat(tframe_list,axis=0)
#     else:
#         tframe=get_value(stock_lists,timestamp)
#     up_10=0
#     dn_10=0
#     pos=len(tframe[tframe.change_rate > 0])
#     pos_rate=round(float(pos)/len(tframe),2)
#     mean=tframe['change_rate'].mean()
#     pos_rate=round(pos_rate,2)
#     mean=round(mean,2)
#     return (up_10,dn_10,pos_rate,mean,tframe)
#
#
#
# def get_meat(cdate,predate,mongodb):
#     Sframe=pd.DataFrame()
#     count = 0
#     predate = datetime.datetime.strptime(predate,"%Y-%m-%d").strftime("%Y%m%d")
#     cdate = datetime.datetime.strptime(cdate,"%Y-%m-%d").strftime("%Y%m%d")
#     cdateinfo = mongodb.stock.ZDT_by_date.find_one({"date":cdate})
#     predateinfo = mongodb.stock.ZDT_by_date.find_one({"date":predate})
#     pre_ztlist = predateinfo['ZT_stocks'].split("_")
#     pre_dtlist = predateinfo['DT_stocks'].split("_")
#     timestamp = time.strftime("%X",time.localtime())
#
#     if pre_ztlist[0] != u'':
#         # print "handling zt list... of %s"%pre_ztlist
#         (up_10,dn_10,pos_rate,mean,ztframe)=get_statistics(pre_ztlist,cdate)
#         Sframe.loc[count,'time']=timestamp
#         Sframe.loc[count,'zt']=up_10
#         Sframe.loc[count,'dt']=dn_10
#         Sframe.loc[count,'pos_rate']=pos_rate
#         Sframe.loc[count,'mean']=mean
#
#     if pre_dtlist[0] != u'':
#         # print "handling dt list... of %s"%pre_dtlist
#         (up_10,dn_10,pos_rate,mean,dtframe)=get_statistics(pre_dtlist,cdate)
#         Sframe.loc[count,'ddt']=dn_10
#         Sframe.loc[count,'dposr']=pos_rate
#         Sframe.loc[count,'dmean']=mean
#
#     if not ztframe.empty:
#         meatFrame = ztframe[ztframe.change_rate > 3]
#         holeFrame = ztframe[ztframe.change_rate < -2]
#         meatList = "_".join(list(meatFrame['stcid'].values))
#         holeList = "_".join(list(holeFrame['stcid'].values))
#         mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"meatList":meatList,
#                                                                             "holeList":holeList}},True,True)
#
#     if 'ddt' not in Sframe.columns:         ##无跌停
#         Sframe.loc[count,'dmean'] = 1000
#         Sframe.loc[count,'dposr'] = 1000
#     if 'zt' not in Sframe.columns:
#         Sframe.loc[count,'pos_rate'] = 1000
#         Sframe.loc[count,'mean'] = 1000
#
#     mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"pre_dtm":Sframe.loc[count,'dmean'],
#                                                                        "pre_dtpos":Sframe.loc[count,'dposr'],
#                                                                        "pre_ztm":Sframe.loc[count,'mean'],
#                                                                        "pre_ztpos":Sframe.loc[count,'pos_rate']}},True,True)
#
# # ## 从指定日期开始，计算meat,hole, 以及pre的情况
# dateStart = "20160425"
# mongoUrl = "localhost"
# aclass = trace_yesterday.top_statistic()
#
# mongodb = pymongo.MongoClient(mongoUrl)
# # dateDicts=mongodb.stock.ZDT_by_date.find({"date":{"$gt":dateStart}})
# dateDicts=mongodb.stock.ZDT_by_date.find({"date":dateStart})
# for dateDict in dateDicts:
#     cdate=dateDict['date']
#     print cdate
#     cdate=datetime.datetime.strptime(cdate,"%Y%m%d").strftime("%Y-%m-%d")
#     predate=get_last_date(cdate)
#     get_meat(cdate,predate,mongodb)



## 在指定日期区间内，找到一字涨停的新股，更新到mongo中的freshStocks
def update_freshStocks(dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
    # step 2
    # 给定日期区间内，计算连续涨跌停个数，并去重zt_stocks, dt_stocks, 重新计算个数
    # 需要依赖数据库中已经有的涨跌停股票代码以及连续涨跌停数（如果没有连续值，则从0开始）
    mongoUrl = "localhost"
    mongodb = pymongo.MongoClient(mongoUrl)
    if dateEq != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":dateEq})
    elif dateEnd != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":{"$gt":dateStart},"date":{"$lt":dateEnd}})
    elif dateStart != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":{"$gt":dateStart}})
    else:
        print "Type error, please reinput"
        return 1

    for dateDict in dateDicts:
        cdate=dateDict['date']
        predate=get_last_date(cdate)
        oneDate_freshStocks(cdate,predate,mongodb)

def oneDate_freshStocks(cdate,predate,mongodb):

    predate = format_date(predate,"%Y%m%d")
    cdate = format_date(cdate,"%Y%m%d")
    yesResult = mongodb.stock.ZDT_by_date.find_one({"date":predate})
    yesFreshStocks = []

    ## 今日的次新一字板股，应该是昨天的freshStock(连续过来的新股）以及昨天Add_newStocks中的股票
    if yesResult is not None and yesResult.has_key("freshStocks"):
        yesFreshStocks.extend(yesResult['freshStocks'].split("_"))
    if yesResult is not None and yesResult.has_key("Add_newStocks"):
        yesFreshStocks.extend(yesResult['Add_newStocks'].keys())


    ##再把今天上市的新股加进来
    todayNewadds = []
    todayResult = mongodb.stock.ZDT_by_date.find_one({"date":cdate})
    if todayResult.has_key("Add_newStocks"):
        yesFreshStocks.extend(todayResult['Add_newStocks'].keys())
        todayNewadds.extend(todayResult['Add_newStocks'].keys())

    ##去重
    yesFreshStocks=list(set(yesFreshStocks))

    ##和涨停股票进行比较
    todayZTs = todayResult['ZT_stocks'].split("_")      ##今日涨停股票
    freshStocks = [x for x in yesFreshStocks if x in todayZTs]      ##今日的次新一字涨停股
    freshStocks.extend(todayNewadds)
    openedFreshStocks = [x for x in yesFreshStocks if x not in todayZTs and x not in todayNewadds]    ##次新股中今天的开板股票
    actulZtStocks = [x for x in todayZTs if x not in yesFreshStocks]    ##去除次新股的今日自然涨停股票

    freshStocks = "_".join(freshStocks)
    openedFreshStocks = "_".join(openedFreshStocks)
    actulZtStocks = "_".join(actulZtStocks)
    print "################ %s ########################"%cdate
    print u"次新：%s"%freshStocks
    print u"涨停: %s"%actulZtStocks
    print u"开板：%s"%openedFreshStocks
    dicts = {
        "freshStocks":freshStocks,
        "actulZtStocks":actulZtStocks
    }
    mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":dicts},True)

if __name__ == "__main__":
    update_freshStocks(dateStart = '20160404')