#coding:utf8
__author__ = 'li.li'
import Collect_NewStocks
import pandas as pd
from pandas import DataFrame
import common
import os
import requests
import time
import numpy as np
import multiprocessing
import datetime
import redis

import pymongo

frame_list=[]
class rtMonitor:
    def __init__(self, rediser):
        self.redis = rediser
        self.stock_list = []
        self.get_stock_list()
        self.stframe = DataFrame()
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        self.count=0
        self.dir = u'D:/Money/tenPercent'

    def get_stock_list(self):
        stock_list = self.redis.keys()
        self.stock_list = [x for x in stock_list if len(str(x)) == 6]
        self.stock_list = list(set(self.stock_list))

    def runBatch(self):
        ## 将今日涨停股票入库
        self.get_summarize_day()

    '''
    time stockid close preclose high low rate
    '''
    def get_price_frame(self):
        ttframe = common.get_price_from_redis(self.stock_list, self.redis)
        return ttframe

    def get_summarize_day(self):
        self.cdate =datetime.date.today().strftime("%Y-%m-%d")
        # self.cdate = "2016-12-16"
        self.ttframe = self.get_price_frame()
        self.ttframe['up10']=(self.ttframe['preclose'].astype(np.float64)*1.1).round(2)
        self.ttframe['dn10']=(self.ttframe['preclose'].astype(np.float64)*0.9).round(2)
        self.ttframe.to_csv(os.path.join(self.dir,'daily_summary_%s.csv'%self.cdate))
        self.utframe=self.ttframe[self.ttframe.close>=self.ttframe.up10]['stockid']       ##涨停
        self.nuframe=self.ttframe[(self.ttframe.high==self.ttframe.up10) & (self.ttframe.high != self.ttframe.close)]['stockid']  ##高点涨停
        self.dtframe=self.ttframe[self.ttframe.close==self.ttframe.dn10]['stockid']       ##跌停
        self.ndframe=self.ttframe[(self.ttframe.low==self.ttframe.dn10) & (self.ttframe.low != self.ttframe.close)]['stockid']    ##低点跌停
        s1=pd.DataFrame(self.utframe.values)
        s2=pd.DataFrame(self.nuframe.values)
        s3=pd.DataFrame(self.dtframe.values)
        s4=pd.DataFrame(self.ndframe.values)

        self.sframe=pd.concat([s1,s2,s3,s4],axis=1)
        self.sframe.columns=['up10','high10','dn10','low10']
        self.sframe.to_csv(os.path.join(self.dir,"%s_summary_results.csv"%self.cdate))
        self.get_backsee(self.sframe)


    def get_backsee(self,sframe):
        # step 1: write "ZT_stocks,ZT_num,DT_stocks,DT_num" base info to mongo
        [dicts,ZT_list,DT_list,HD_list,LD_list]=self.baseInfo(sframe)

        # step 2: ZT/DT stocks, that got big amount
        ZT_overMonut=self.overMount(ZT_list)
        dicts['ZT_Mount']=ZT_overMonut
        DT_overMonut=self.overMount(DT_list)
        dicts['DT_Mount']=DT_overMonut

        # step 3: generate Add_newStocks, actulZtStocks, freshStocks, openedFreshedStocks
        print dicts['date']
        yesterday = common.get_last_date(dicts['date'])
        yesterday = common.format_date(yesterday,"%Y%m%d")
        yesResults = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday})
        freshStocks = []
        if yesResults.has_key("freshStocks"):
            freshStocks = yesResults['freshStocks'].split("_")

        ## 新股加入到mongo中，每天刷新一次
        Collect_NewStocks.fresh_newStockWebsite()
        todResults = self.mongodb.stock.ZDT_by_date.find_one({"date":dicts['date']})
        newAddStocks = []
        if todResults.has_key("Add_newStocks"):
            newAddStocks = todResults['Add_newStocks'].keys()
            freshStocks.extend(newAddStocks)

        ## 自然涨停股票
        dicts['actulZtStocks'] = "_".join([x for x in dicts['ZT_stocks'].split("_") if x not in freshStocks])

        ## 连续涨停的次新股
        freshStocks = [x for x in freshStocks if x in dicts['ZT_stocks']]
        freshStocks.extend(newAddStocks)
        dicts['freshStocks'] = "_".join(list(set(freshStocks)))

        ## 开板次新股
        dicts['openedFreshedStocks'] = "_".join([x for x in freshStocks if x not in dicts['ZT_stocks'] and x not in newAddStocks])
        self.mongodb.stock.ZDT_by_date.update({"date":dicts['date']},{"$set":dicts},True)




    #放量股
    # tframe格式为：
    #        time        date   stockid  close  preclose   high    low  vol  amount       rate
    # 0  11:15:12  2016-04-07  002785  27.20     25.08  27.50  25.00  115516   3010000   8.45
    # 1  11:15:12  2016-04-07  603866  33.36     32.94  33.75  32.95   34952   117000   1.28
    # return string
    def overMount(self,tlist):
        if len(tlist)<1:
            return ""
        tframe=common.get_sina_data(tlist)
        Mlist = ""
        # Mlist=[tframe.loc[i,"stcid"] for i in tframe.index.values if common.volStatus(tframe.loc[i,"stcid"],tframe.loc[i,'date'],tframe.loc[i,'vol'])]
        return "_".join(Mlist)



    ## 基本信息
    # 	up10	high10	dn10	low10
    # 0	600803	600961	300028	600654
    # 1	520	2101	0	0
    # 2	300474	300088	0	0
    # 3	300176	2341	0	0
    # 4	300484	603366	0	0
    def baseInfo(self,tframe):
        dicts={}
        ZT_list=[str(int(x)) if (len(str(int(x)))==6) else'0'*(6-len(str(int(x))))+str(int(x)) for x in tframe['up10'].values if x>0]
        DT_list=[str(int(x)) if (len(str(int(x)))==6) else'0'*(6-len(str(int(x))))+str(int(x)) for x in tframe['dn10'].values if x>0]
        HD_list=[str(int(x)) if (len(str(int(x)))==6) else'0'*(6-len(str(int(x))))+str(int(x)) for x in tframe['high10'].values if x>0]
        LD_list=[str(int(x)) if (len(str(int(x)))==6) else'0'*(6-len(str(int(x))))+str(int(x)) for x in tframe['low10'].values if x>0]
        ZT_list = list(set(ZT_list))
        DT_list = list(set(DT_list))
        HD_list = list(set(HD_list))
        LD_list = list(set(LD_list))

        dicts['ZT_num']=len(ZT_list)
        dicts['DT_num']=len(DT_list)
        dicts['ZT_stocks']="_".join(ZT_list)
        dicts['DT_stocks']="_".join(DT_list)
        dicts['HD_stocks']="_".join(HD_list)
        dicts['LD_stocks']="_".join(LD_list)
        dicts['date']=common.format_date(self.cdate,"%Y%m%d")
        return [dicts,ZT_list,DT_list,HD_list,LD_list]

    def get_statistic(self,tframe):
        timestamp=tframe.loc[0,'time'].values[0]
        tframe = tframe[tframe.rate > -100]
        tframe['upercent10']=(tframe['preclose'].astype(np.float64)*1.1).round(2)
        tframe['dpercent10']=(tframe['preclose'].astype(np.float64)*0.9).round(2)
        uframe=tframe[tframe.close==tframe.upercent10]
        dframe=tframe[tframe.close==tframe.dpercent10]
        p5frame=tframe[tframe.rate>=5]
        n5frame=tframe[tframe.rate<=-5]
        self.stframe.loc[self.count,'time']=timestamp
        self.stframe.loc[self.count,'up10_num']=len(uframe)
        self.stframe.loc[self.count,'dn10_num']=len(dframe)
        self.stframe.loc[self.count,'>+5']=len(p5frame)
        self.stframe.loc[self.count,'<-5']=len(n5frame)
        self.count+=1

if __name__=='__main__':
    redis = redis.Redis(host='localhost', port=6379, db=1)
    z=rtMonitor(redis)
    z.runBatch()
