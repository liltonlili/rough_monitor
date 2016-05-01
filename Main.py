__author__ = 'li.li'
import history_10_list
import fetch_data
# import Show_Result

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
import multiprocessing
import datetime
import trace_yesterday
import statistic
import report_env
import os
import pymongo
import numpy as np
import pandas as pd
from pandas import  DataFrame


class MainProc:


    def __init__(self,syear,smonth,eyear,emonth):
        ## get ZDT data from the database, generate /ZDT/year_month.csv
        his_proc=history_10_list.Gdata(syear,smonth,eyear,emonth)
        # ## get minute level data from database, generate ZDT/minute/yearmonthday.csv
        # minute_proc = fetch_data.fetch_min_data(syear,smonth,eyear,emonth)
        ## get the statistic results, generate /ZDT/statis_resus
        stat_proc = statistic.statistic(syear,smonth,eyear,emonth)
        #
        # ## plot the temperature
        # temperature_proc= temperature.get_temperature(syear,smonth,eyear,emonth)



    def update_ZDT_stocksNum(self,stock_list,date_list):
        # step 1
        # 更新指定日的涨停股票，跌停股票，涨停数，跌停数
        # stock_list = []
        # date_list = ['20160428']
        fullFrame = get_mysqlData(stock_list,date_list)
        #      TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE    HIGHEST_PRICE   LOWEST_PRICE  CLOSE_PRICE
        # 0           000033          *ST新都  2016-04-27            10.38        0.00           0.00          0.00        10.38
        # 1           600710          *ST常林  2016-04-27             9.36        0.00         10.61         10.52        10.57

        fullFrame['ztprice']=fullFrame['PRE_CLOSE_PRICE'] * 1.1
        fullFrame['dtprice']=fullFrame['PRE_CLOSE_PRICE'] * 0.9
        fullFrame['ztprice'] = fullFrame['ztprice'].round(decimals=2)
        fullFrame['dtprice'] = fullFrame['dtprice'].round(decimals=2)

        mongo_url="localhost"
        mongodb=pymongo.MongoClient(mongo_url)
        for date in np.unique(fullFrame['TRADE_DATE']):
            str_date = datetime.datetime.strptime(str(date),"%Y-%m-%d").strftime("%Y%m%d")
            # if str_date < "20160326" or str_date >= "20160407":
            #     continue
            print str_date
            dayFrame = fullFrame[fullFrame.TRADE_DATE==date]
            ztList=dayFrame[dayFrame.CLOSE_PRICE==dayFrame.ztprice]['TICKER_SYMBOL'].values
            ztList=["0"*(6-len(str(x)))+str(x) for x in ztList]
            ztnum = len(ztList)
            ztList="_".join(ztList)

            dtList=dayFrame[dayFrame.CLOSE_PRICE==dayFrame.dtprice]['TICKER_SYMBOL'].values
            dtList=["0"*(6-len(str(x)))+str(x) for x in dtList]
            dtnum = len(dtList)
            dtList="_".join(dtList)

            hiList=dayFrame[(dayFrame.HIGHEST_PRICE==dayFrame.ztprice) & (dayFrame.CLOSE_PRICE!=dayFrame.ztprice)]['TICKER_SYMBOL'].values
            hiList=["0"*(6-len(str(x)))+str(x) for x in hiList]
            hiList="_".join(hiList)

            lowList=dayFrame[(dayFrame.LOWEST_PRICE==dayFrame.dtprice) & (dayFrame.CLOSE_PRICE!=dayFrame.dtprice)]['TICKER_SYMBOL'].values
            lowList=["0"*(6-len(str(x)))+str(x) for x in lowList]
            lowList="_".join(lowList)

            dicts={
                "ZT_stocks":ztList,
                "DT_stocks":dtList,
                "HD_stocks":hiList,
                "LD_stocks":lowList,
                "ZT_num":ztnum,
                "DT_num":dtnum,
                "date":str_date
            }
            print dicts
            mongodb.stock.ZDT_by_date.update({"date":dicts['date']},{"$set":dicts},True)

    def update_ZDT_contsNum(self, dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
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
            print cdate
            cdate=datetime.datetime.strptime(cdate,"%Y%m%d").strftime("%Y-%m-%d")
            predate=get_last_date(cdate)
            update_numZD(cdate,predate,mongodb)


    def update_ZDT_yesterday(self,dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
        # step 2
        ## 从指定日期开始，计算meat,hole, 以及pre的情况
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
            print cdate
            cdate=datetime.datetime.strptime(cdate,"%Y%m%d").strftime("%Y-%m-%d")
            predate=get_last_date(cdate)
            self.get_meat(cdate,predate,mongodb)

    def get_statistics(self,stock_list,timestamp):
        stock_lists=stock_list
        if len(stock_list)>200:
            tframe_list=[]
            for i in range(len(stock_list)/200+1):
                if 200*i > len(stock_list):
                    break
                tframe=get_value(stock_lists[200*i:200*(i+1)],timestamp)
                tframe_list.append(tframe)
            tframe=pd.concat(tframe_list,axis=0)
        else:
            tframe=get_value(stock_lists,timestamp)
        up_10=0
        dn_10=0
        pos=len(tframe[tframe.change_rate > 0])
        pos_rate=round(float(pos)/len(tframe),2)
        mean=tframe['change_rate'].mean()
        pos_rate=round(pos_rate,2)
        mean=round(mean,2)
        return (up_10,dn_10,pos_rate,mean,tframe)



    def get_meat(self,cdate,predate,mongodb):
        Sframe=pd.DataFrame()
        count = 0
        predate = datetime.datetime.strptime(predate,"%Y-%m-%d").strftime("%Y%m%d")
        cdate = datetime.datetime.strptime(cdate,"%Y-%m-%d").strftime("%Y%m%d")
        cdateinfo = mongodb.stock.ZDT_by_date.find_one({"date":cdate})
        predateinfo = mongodb.stock.ZDT_by_date.find_one({"date":predate})
        pre_ztlist = predateinfo['ZT_stocks'].split("_")
        pre_dtlist = predateinfo['DT_stocks'].split("_")
        timestamp = time.strftime("%X",time.localtime())

        if pre_ztlist[0] != u'':
            # print "handling zt list... of %s"%pre_ztlist
            (up_10,dn_10,pos_rate,mean,ztframe)=self.get_statistics(pre_ztlist,cdate)
            Sframe.loc[count,'time']=timestamp
            Sframe.loc[count,'zt']=up_10
            Sframe.loc[count,'dt']=dn_10
            Sframe.loc[count,'pos_rate']=pos_rate
            Sframe.loc[count,'mean']=mean

        if pre_dtlist[0] != u'':
            # print "handling dt list... of %s"%pre_dtlist
            (up_10,dn_10,pos_rate,mean,dtframe)=self.get_statistics(pre_dtlist,cdate)
            Sframe.loc[count,'ddt']=dn_10
            Sframe.loc[count,'dposr']=pos_rate
            Sframe.loc[count,'dmean']=mean

        if not ztframe.empty:
            meatFrame = ztframe[ztframe.change_rate > 3]
            holeFrame = ztframe[ztframe.change_rate < -2]
            meatList = "_".join(list(meatFrame['stcid'].values))
            holeList = "_".join(list(holeFrame['stcid'].values))
            mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"meatList":meatList,
                                                                                "holeList":holeList}},True,True)

        if 'ddt' not in Sframe.columns:         ##无跌停
            Sframe.loc[count,'dmean'] = 1000
            Sframe.loc[count,'dposr'] = 1000
        if 'zt' not in Sframe.columns:
            Sframe.loc[count,'pos_rate'] = 1000
            Sframe.loc[count,'mean'] = 1000

        mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"pre_dtm":Sframe.loc[count,'dmean'],
                                                                           "pre_dtpos":Sframe.loc[count,'dposr'],
                                                                           "pre_ztm":Sframe.loc[count,'mean'],
                                                                           "pre_ztpos":Sframe.loc[count,'pos_rate']}},True,True)

if __name__ == "__main__":
    MP=MainProc(2016,3,2016,3)
