#coding:utf8
import sys
sys.path.append("D:\Money")
import pandas as pd
import numpy as np
import os
import datetime
import time
import requests
from pandas import DataFrame,Series
import pymongo
import common
import redis


class top_statistic:
    def __init__(self):
        cday = datetime.date.today().strftime('%Y/%m/%d')
        self.today = datetime.date.today().strftime('%Y%m%d')
        # cday = "2016/05/20"
        # self.today = "20160520"
        self.redis = redis.Redis(host='localhost', port=6379, db=1)
        calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
        del calframe['0']
        calframe.columns=['Time']
        calList=calframe['Time'].values
        calList=list(calList)
        yesterday=calList[calList.index(cday)-1]

        yesterday_mongo = datetime.datetime.strptime(yesterday,'%Y/%m/%d').strftime("%Y%m%d")
        yesterday=datetime.datetime.strptime(yesterday,'%Y/%m/%d').strftime("%Y-%m-%d")

        self.yesterday=yesterday
        ## get info of yesterday
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        self.up10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['actulZtStocks'].split("_")
        self.hi10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['HD_stocks'].split("_")
        self.low10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['LD_stocks'].split("_")
        self.dn10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['DT_stocks'].split("_")
        self.count=0
        


    def get_statistics(self,stock_list,timestamp):
        stock_lists=[]
        for code in stock_list:
            if len(code) < 2:
                continue
            code=int(code)
            code = '0'*(6-len(str(code)))+str(code)
            stock_lists.append(code)
        # 改用redis 接口
        tframe = common.get_price_from_redis(stock_lists, self.redis)
        tframe = tframe[tframe.rate > -15]
        tframe['up10']=(tframe['preclose'].astype(np.float64)*1.1).round(2)
        tframe['dn10']=(tframe['preclose'].astype(np.float64)*0.9).round(2)
        up_10=len(tframe[tframe.up10==tframe.close])
        dn_10=len(tframe[tframe.dn10==tframe.close])
        pos=len(tframe[tframe.rate > 0])
        pos_rate=round(float(pos)/len(tframe),2)
        mean=tframe['rate'].mean()
        std_rate=round(tframe['rate'].std(),2)
        pos_rate=round(pos_rate,2)
        mean=round(mean,2)
        return (up_10,dn_10,pos_rate,mean,tframe)
    
    def run(self):
        Sframe=DataFrame()
        while(True):
            timestamp=time.strftime("%X",time.localtime())
            dir = 'D:\Money\Realtime'
            if (len(self.up10_list) > 1) or (len(self.up10_list) == 1 and self.up10_list[0] != u''):
                (up_10,dn_10,pos_rate,mean,ztframe)=self.get_statistics(self.up10_list,timestamp)
                Sframe.loc[self.count,'time']=timestamp
                Sframe.loc[self.count,'zt']=up_10
                Sframe.loc[self.count,'dt']=dn_10
                Sframe.loc[self.count,'pos_rate']=pos_rate
                Sframe.loc[self.count,'mean']=mean

            if (len(self.hi10_list) > 1) or (len(self.hi10_list) == 1 and self.hi10_list[0] != u''):
                (up_10,dn_10,pos_rate,mean,hiframe)=self.get_statistics(self.hi10_list,timestamp)
                Sframe.loc[self.count,'hzt']=up_10
                Sframe.loc[self.count,'hdt']=dn_10
                Sframe.loc[self.count,'hposr']=pos_rate
                Sframe.loc[self.count,'hmean']=mean

            if (len(self.dn10_list) > 1) or (len(self.dn10_list) == 1 and self.dn10_list[0] != u''):
                (up_10,dn_10,pos_rate,mean,dtframe)=self.get_statistics(self.dn10_list,timestamp)
                Sframe.loc[self.count,'ddt']=dn_10
                Sframe.loc[self.count,'dposr']=pos_rate
                Sframe.loc[self.count,'dmean']=mean


            Sframe.to_csv(os.path.join(dir,'%s_statistics.csv'%self.yesterday))


            ttime=time.localtime()
            thour=ttime.tm_hour
            tmin=ttime.tm_min

            ## 记录当天有肉，被坑情况
            if not ztframe.empty:
                meatFrame = ztframe[ztframe.rate > 3]
                holeFrame = ztframe[ztframe.rate < -2]
                meatList = "_".join(list(meatFrame['stockid'].values))
                holeList = "_".join(list(holeFrame['stockid'].values))
                self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{"meatList":meatList,
                                                                                    "holeList":holeList}},True,True)
            if 'ddt' not in Sframe.columns:
                Sframe.loc[self.count,'dmean'] = 1000
                Sframe.loc[self.count,'dposr'] = 1000

            if 'zt' not in Sframe.columns:
                Sframe.loc[self.count,'pos_rate']=1000
                Sframe.loc[self.count,'mean']=1000

            if 'hzt' not in Sframe.columns:
                Sframe.loc[self.count,'hposr']=1000
                Sframe.loc[self.count,'hmean']=1000

            if (thour > 15) or (thour == 15 and tmin > 10):
                while not self.mongodb.stock.ZDT_by_date.find({"date":self.today}):
                    time.sleep(60)

                self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{"pre_dtm":Sframe.loc[self.count,'dmean'],
                                                                                       "pre_dtpos":Sframe.loc[self.count,'dposr'],
                                                                                       "pre_ztm":Sframe.loc[self.count,'mean'],
                                                                                       "pre_ztpos":Sframe.loc[self.count,'pos_rate'],
                                                                                       "pre_htm":Sframe.loc[self.count,'hmean'],
                                                                                       "pre_htpos":Sframe.loc[self.count,'hposr']}},True,True)
                del Sframe['dmean']
                print Sframe            ##实时显示当前情况
                self.genCsv()
                self.calCons()

                break
            del Sframe['dmean']
            print Sframe            ##实时显示当前情况
            self.count+=1
            time.sleep(30)

    # 计算连板个数
    def calCons(self):
        predate = common.get_last_date(self.today)
        common.update_numZD(self.today,predate,self.mongodb)
        print "calculate continus number finished!"

    def genCsv(self):
        ##生成daydayup.csv,用来复盘
        detailinfos = self.mongodb.stock.ZDT_by_date.find_one({"date":self.today})
        ztStocks = detailinfos['actulZtStocks'].split("_")
        dtStocks = detailinfos['DT_stocks'].split("_")
        meatStocks = detailinfos['meatList'].split("_")
        holeStocks = detailinfos['holeList'].split("_")
        hdStocks = detailinfos['HD_stocks'].split("_")

        i = 0
        Aframe=DataFrame()
        for ztStock in ztStocks:
            Aframe.loc[i,'stock']=ztStock
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='ZT'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(ztStock)
            i+=1

        for hdStock in hdStocks:
            Aframe.loc[i,'stock']=hdStock
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='HD'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(hdStock)
            i+=1

        for dtStock in dtStocks:
            Aframe.loc[i,'stock']=dtStock
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='DT'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(dtStock)
            i+=1

        for meatStock in meatStocks:
            Aframe.loc[i,'stock']=meatStock
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='meat'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(meatStock)
            i+=1

        for holeStock in holeStocks:
            Aframe.loc[i,'stock']=holeStock
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='hole'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(holeStock)
            i+=1

        Aframe.to_csv(os.path.join("D:\Money\modeResee","daydayup.csv"),encoding='gb18030')
        print "Generate daydayup csv finished!"

if __name__=="__main__":
    z=top_statistic()
    z.run()