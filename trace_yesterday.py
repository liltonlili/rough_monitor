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


class top_statistic:
    def __init__(self):
        cday = datetime.date.today().strftime('%Y/%m/%d')
        self.today = datetime.date.today().strftime('%Y%m%d')
        # cday = "2016/05/20"
        # self.today = "20160520"
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
        

    def parse_content(self,content,timestamp):
        Inframe=pd.DataFrame()
        i = 0
        strarray=content.split(';')
        for item in strarray:
            item_array=item.split(',')
            if len(item_array)<10:
                continue
            stockid = item_array[0][14:20]
            stockid = item_array[0].split('=')[0].split('str_')[1][2:]
            close = item_array[3]
            preclose = item_array[2]
            if close == '0.00':
                continue
            Inframe.loc[i,'time']=timestamp
            Inframe.loc[i,'stcid']=stockid
            Inframe.loc[i,'close']=round(float(close),2)
            Inframe.loc[i,'preclose']=preclose
            i+=1
        Inframe['rate']=100*(Inframe['close'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
        Inframe['rate']=Inframe['rate'].round(decimals=2)
        Inframe['up10']=(Inframe['preclose'].astype(np.float64)*1.1).round(2)
        Inframe['dn10']=(Inframe['preclose'].astype(np.float64)*0.9).round(2)
        return Inframe


    def get_data(self,stock_list,timestamp):
        slist=','.join(stock_list)
        url = "http://hq.sinajs.cn/list=%s"%slist
        try:
            r=requests.get(url)
        except:
            r=requests.get(url)
        content=r.content.decode('gbk')
        Dframe=self.parse_content(content,timestamp)
        return Dframe
    

    def get_statistics(self,stock_list,timestamp):
        stock_lists=[]       
        for code in stock_list:
            if len(code) < 2:
                continue
            code=int(code)
            code = '0'*(6-len(str(code)))+str(code)
            if code[0:2] == '60':
                code = 'sh'+code
            else:
                code = 'sz'+code
            stock_lists.append(code)
        if len(stock_list)>200:
            tframe_list=[]
            for i in range(len(stock_list)/200+1):
                if 200*i > len(stock_list):
                    break
                tframe=self.get_data(stock_lists[200*i:200*(i+1)],timestamp)
                tframe_list.append(tframe)
            tframe=pd.concat(tframe_list,axis=0)
        else:
            tframe=self.get_data(stock_lists,timestamp)
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
                meatList = "_".join(list(meatFrame['stcid'].values))
                holeList = "_".join(list(holeFrame['stcid'].values))
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