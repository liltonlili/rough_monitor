##coding:utf-8
import sys
sys.path.append("D:\Money")
from common import *
import mysqldb
import pandas as pd
import os
import datetime
import report_env
import numpy as np
import pymongo
import time
import logging

# fetch_data return
#       TRADE_DATE TICKER_SYMBOL   rate type
# 20    2016-02-02        000019   9.99   ZT
# 124   2016-02-02        000505  10.03   ZT
# 142   2016-02-02        000525   9.97   ZT
# 143   2016-02-02        000526  10.00   ZT
# 169   2016-02-02        000552   9.94   ZT
# 173   2016-02-02        000555   9.99   ZT
# 213   2016-02-02        000606   9.97   ZT
# 266   2016-02-02        000679  10.02   ZT

## 把每天的涨跌停股票、涨跌停个数记录到mongo中

# {"Date":"",               (Here)
#  "ZT_number":"",               (Here)
#  "DT_number":"",               (Here)
#  "PZT_mean":"",
#  "PDT_mean":"",
#  "ZT_stocks":"XX,XX,XX,XX",               (Here)
#  "DT_stocks":"XX,XX,XX,XX",               (Here)
#  "zt_2":"XX,XX,XX",
#  "zt_3":"XX,XX,XX",
#  "zt_4":"XX,XX,XX",
#  "zt_5":"XX,XX,XX",
#  "dt_1":"XX,XX,XX",
#  "dt_2":"XX,XX",
# }




class Gdata:
    ## 起始年，月，终止年，月
    def __init__(self,sy,sm,ey,em,flag=1):
        self.sYear=sy
        self.sMonth=sm
        self.eYear=ey
        self.eMonth=em
        self.outdir = "D:\Money\lilton_code\Market_Mode\history_zt\ZT"
        self.lastzt=[]      ##上个交易日涨停的股票
        self.lastdt=[]      ##上个交易日跌停的股票
        self.sqldb=mysqldata()
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        report_env.init_config(os.path.join("D:\Money\lilton_code\Market_Mode","conf.txt"))
        self.year=report_env.get_prop("year")
        self.month = report_env.get_prop("month")
        self.day = report_env.get_prop("day")
        self.flag = flag
        self.run()

    def run(self):
        dateclass=gm_date(self.sYear,self.sMonth,self.eYear,self.eMonth)
        monthlist=dateclass.calList
        cu_year=0
        cu_month=0
        Tframe=None
        pre_month=0
        for date in monthlist:
            print date
            year=datetime.datetime.strptime(date,"%Y/%m/%d").year
            month=datetime.datetime.strptime(date,"%Y/%m/%d").month
            if pre_month == 0:
                pre_month = month
            if month!= pre_month:
                print "middle save:%s"%date
                # store into mongo each month
                self.P2M(Tframe)
                Tframe=pd.DataFrame()
                pre_month = month
            if cu_year==year and cu_month == month:
                pass
            else:
                if self.flag:
                    if (int(year)< int(self.year)) or (int(year) == int(self.year) and int(month) <=int(self.month)):
                        print "Thread history_10 pass %s"%date
                        continue
                if cu_year!=0:
                    Tframe.to_csv(os.path.join("D:\Money\lilton_code\Market_Mode\history_zt\ZDT",filename))
                cu_year=year
                cu_month=month
                filename="%s_%s.csv"%(cu_year,cu_month)
                Tframe=pd.DataFrame()
            try:
                tmp_frame=self.fetch_data(date)
                Tframe=pd.concat([Tframe,tmp_frame],axis=0)
            except Exception,e:
                print e
                break
        if Tframe is not None:
            # Tframe.to_csv(os.path.join("D:\Money\lilton_code\Market_Mode\history_zt\ZDT",filename))
            self.P2M(Tframe)


    # write mongo
    def P2M(self,Tframe):
        for date in np.unique(Tframe.TRADE_DATE.values):
            try:
                ZT_list=list(Tframe[(Tframe.type=="ZT") & (Tframe.TRADE_DATE==date)]["TICKER_SYMBOL"])
                DT_list=list(Tframe[(Tframe.type=="DT") & (Tframe.TRADE_DATE==date)]["TICKER_SYMBOL"])
                ZT_String="_".join([str(x) for x in ZT_list])
                DT_String="_".join([str(x) for x in DT_list])
                ZT_num=len(ZT_list)
                DT_num=len(DT_list)
                dicts={}
                date=date.strftime("%Y%m%d")
                dicts["date"]=date
                dicts["ZT_stocks"]=ZT_String
                dicts["DT_stocks"]=DT_String
                dicts["ZT_num"]=ZT_num
                dicts["DT_num"]=DT_num
                self.mongodb.stock.ZDT_by_date.delete_many({"date":date})
                self.mongodb.stock.ZDT_by_date.insert(dicts)
            except Exception,e:
                logging.getLogger().info(e)


    def fetch_data(self,date):
        table='mkt_equd_adj'
        lzframe=pd.DataFrame()
        ldframe=pd.DataFrame()
        sql='select TICKER_SYMBOL,TRADE_DATE,CLOSE_PRICE,ACT_PRE_CLOSE_PRICE from %s where TRADE_DATE="%s"'%(table,date)
        adata=self.sqldb.dydb_query(sql)
        (ztframe,dtframe)=get_zdt(adata,'CLOSE_PRICE','ACT_PRE_CLOSE_PRICE')
        if len(self.lastzt)>0:
            lzframe=self.get_detail(self.lastzt,'ZT',adata)
        if len(self.lastdt)>0:
            ldframe=self.get_detail(self.lastdt,'DT',adata)
        Aframe=pd.concat([ztframe,dtframe,lzframe,ldframe],axis=0)
        self.lastzt=ztframe['TICKER_SYMBOL'].values
        self.lastdt=dtframe['TICKER_SYMBOL'].values
        return Aframe

    def get_detail(self,stock_list,type,tframe):
        if type =='ZT':
            marker='PZT'
        else:
            marker='PDT'
        copyframe=tframe.set_index('TICKER_SYMBOL')
        tmpframe=copyframe.loc[stock_list,:]
        tmpframe['type']=np.array([marker]*len(tmpframe))
        tmpframe.reset_index(range(len(tmpframe)),inplace=True)
        tmpframe['rate']=100*(tmpframe['CLOSE_PRICE'].astype(np.float64)-tmpframe['ACT_PRE_CLOSE_PRICE'].astype(np.float64))/tmpframe['ACT_PRE_CLOSE_PRICE'].astype(np.float64)
        tmpframe['rate']=tmpframe['rate'].round(decimals=2)
        tmpframe=tmpframe[['TRADE_DATE','TICKER_SYMBOL','rate','type']]
        return tmpframe

if __name__ == "__main__":
    z=Gdata(2016,03,2016,03)