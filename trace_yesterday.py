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
import LearnFrom
import matplotlib.pyplot as plt

import matplotlib as mpl
from matplotlib import *
from matplotlib.font_manager import FontProperties
myfont = mpl.font_manager.FontProperties(fname=os.path.join(u'C:/Windows/Fonts','wqy-microhei.ttc'))

class top_statistic:
    def __init__(self):
        cday = datetime.date.today().strftime('%Y/%m/%d')
        self.today = datetime.date.today().strftime('%Y%m%d')
        # cday = "2016/11/11"
        # self.today = "20161111"
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
        ttime=time.localtime()
        thour=ttime.tm_hour
        tmin=ttime.tm_min
        detailinfos = self.mongodb.stock.ZDT_by_date.find_one({"date":self.today})
        if (thour < 18):
            ##生成daydayup.csv,用来复盘
            Aframe = self.get_csv(detailinfos, png_enable = 0)
            Aframe.to_csv(os.path.join("D:\Money\modeResee","daydayup.csv"),encoding='gb18030')
            # 将每日的csv copy到复盘目录中
            c_date = self.today
            constant_dir = os.path.join(u'D:/Money/modeResee/复盘', c_date)
            Aframe.to_csv(os.path.join(constant_dir, "daydayup.csv"),encoding='gb18030')
            print "Generate daydayup csv finished!"

        while True:
            ttime=time.localtime()
            thour=ttime.tm_hour
            tmin=ttime.tm_min
            if (thour > 18) or (thour == 18 and tmin > 5):
                Aframe = self.get_csv(detailinfos, png_enable = 1)
                Aframe.to_csv(os.path.join("D:\Money\modeResee","daydayup.csv"),encoding='gb18030')
                break
            else:
                print "Sleeping"
                time.sleep(1800)

        # 将每日的csv copy到复盘目录中
        c_date = self.today
        constant_dir = os.path.join(u'D:/Money/modeResee/复盘', c_date)
        Aframe.to_csv(os.path.join(constant_dir, "daydayup.csv"),encoding='gb18030')

    def get_csv(self, detailinfos, png_enable = 0):
        ztStocks = detailinfos['actulZtStocks'].split("_")
        dtStocks = detailinfos['DT_stocks'].split("_")
        meatStocks = detailinfos['meatList'].split("_")
        holeStocks = detailinfos['holeList'].split("_")
        hdStocks = detailinfos['HD_stocks'].split("_")

        i = 0
        Aframe=DataFrame()
        c_date = self.today
        constant_dir = os.path.join(u'D:/Money/modeResee/复盘', c_date)
        if not os.path.exists(constant_dir):
            os.makedirs(constant_dir)

        for ztStock in ztStocks:
            Aframe.loc[i,'stock']=ztStock
            Aframe.loc[i,'name'] = common.QueryStockMap(id = ztStock)[0]
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='ZT'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(ztStock)
            if png_enable == 1:
                generate_fp_pic(ztStock, constant_dir, Aframe.loc[i,'news'], self.today)
            i+=1

        for hdStock in hdStocks:
            Aframe.loc[i,'stock']=hdStock
            Aframe.loc[i,'name'] = common.QueryStockMap(id = hdStock)[0]
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='HD'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(hdStock)
            if png_enable == 1:
                generate_fp_pic(hdStock, constant_dir, Aframe.loc[i,'news'], self.today)
            i+=1

        for dtStock in dtStocks:
            Aframe.loc[i,'stock']=dtStock
            Aframe.loc[i,'name'] = common.QueryStockMap(id = dtStock)[0]
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='DT'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(dtStock)
            if png_enable == 1:
                generate_fp_pic(dtStock, constant_dir, Aframe.loc[i,'news'], self.today)
            i+=1

        for meatStock in meatStocks:
            Aframe.loc[i,'stock']=meatStock
            Aframe.loc[i,'name'] = common.QueryStockMap(id = meatStock)[0]
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='meat'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(meatStock)
            if png_enable == 1:
                generate_fp_pic(meatStock, constant_dir, Aframe.loc[i,'news'], self.today)
            i+=1

        for holeStock in holeStocks:
            Aframe.loc[i,'stock']=holeStock
            Aframe.loc[i,'name'] = common.QueryStockMap(id = holeStock)[0]
            Aframe.loc[i,'reason']='0'
            Aframe.loc[i,'type']='hole'
            Aframe.loc[i,'desc']='record'
            Aframe.loc[i,'news'] = common.get_latest_news(holeStock)
            if png_enable == 1:
                generate_fp_pic(holeStock, constant_dir, Aframe.loc[i,'news'], self.today)
            i+=1
        return Aframe

# def generate_fp_pic(stockid, dir, news):
#     if int(stockid) == 0:
#         return
#     fig = plt.figure()
#
#     # 画个股分时图
#     ax1 = fig.add_subplot(221)
#     endDate = datetime.datetime.now().strftime("%Y%m%d")
#     # endDate = "20161017"
#     yesterday = common.get_lastN_date(endDate, 1)
#
#     # 画出分时图，并标注涨幅
#     stock_dv = common.get_minly_frame(stockid, endDate, id_type =1)
#     yeframe = common.get_mysqlData([stockid],[yesterday])
#     if len(yeframe) > 0:
#         pre_close = yeframe.loc[0,'CLOSE_PRICE']
#     else:
#         pre_close = 0
#     LearnFrom.plot_dealDetail(stock_dv, ax1, rotation=30, fontsize=5, mount_flag=1, pre_close = pre_close)
#
#     [sname, sid] = common.QueryStockMap(id = stockid)
#     ax1.set_title(sname, fontproperties=myfont)
#     ax1.grid(True)
#
#     # 画上证分时图， 不标注涨幅
#     ax2 = fig.add_subplot(223)
#     sh_dv = common.get_minly_frame(stockid, endDate, id_type =0)
#     LearnFrom.plot_dealDetail(sh_dv, ax2, rotation=30, fontsize=5, mount_flag=1)
#     ax2.grid(True)
#
#     ax3 = fig.add_subplot(222)
#     ax4 = fig.add_subplot(224)
#     # 将新闻画在后面
#     texts = news.split("\n")
#     tn = len(texts)
#
#     n1 = int(tn/2)
#     texts1 = texts[:n1]
#
#     texts2 = texts[n1:]
#     n2 = tn - n1
#     common.plot_text(ax3, texts1, fontsize=8)
#     common.plot_text(ax4, texts2, fontsize=8)
#     plt.savefig(os.path.join(dir,u'%s.png'%stockid), dpi=300)

def generate_fp_pic(stockid, dir, news, endDate):
    if len(str(stockid)) <3:
        return
    print stockid
    fig = plt.figure(figsize=(16,8))

    # 画个股分时图
    ax1 = fig.add_subplot(231)
    # endDate = datetime.datetime.now().strftime("%Y%m%d")
    # endDate = "20161017"
    yesterday = common.get_lastN_date(endDate, 1)

    # 画出分时图，并标注涨幅
    stock_dv = common.get_minly_frame(stockid, endDate, id_type =1)
    yeframe = common.get_mysqlData([stockid],[yesterday])
    if len(yeframe) > 0:
        pre_close = yeframe.loc[0,'CLOSE_PRICE']
    else:
        pre_close = 0
    LearnFrom.plot_dealDetail(stock_dv, ax1, rotation=30, fontsize=5, mount_flag=1, pre_close = pre_close)

    [sname, sid] = common.QueryStockMap(id = stockid)
    ax1.set_title(sname, fontproperties=myfont)
    ax1.grid(True)

    ax5 = fig.add_subplot(233)
    ax6 = fig.add_subplot(236)
    point = 10
    start_date = common.get_lastN_date(endDate, 120)
    # 画上个股日线图
    ggstock = common.get_daily_frame(stockid, start_date, endDate, id_type = 1)
    shstock = common.get_daily_frame(stockid, start_date, endDate, id_type = 0)
    LearnFrom.plot_candlestick(ggstock,ax5,point =point,mount_flag = 1,  rotation=30, fontsize=5)  # 个股日线图
    LearnFrom.plot_candlestick(shstock,ax6,point =point,mount_flag = 1,  rotation=30, fontsize=5)  # 个股日线图
    # 画上证分时图， 不标注涨幅
    ax2 = fig.add_subplot(234)
    sh_dv = common.get_minly_frame(stockid, endDate, id_type =0)
    LearnFrom.plot_dealDetail(sh_dv, ax2, rotation=30, fontsize=5, mount_flag=1)
    ax2.grid(True)

    ax3 = fig.add_subplot(232)
    ax4 = fig.add_subplot(235)
    # 将新闻画在后面
    texts = news.split("\n")
    tn = len(texts)

    n1 = int(tn/2)
    texts1 = texts[:n1]

    texts2 = texts[n1:]
    n2 = tn - n1
    common.plot_text(ax3, texts1, fontsize=8)
    common.plot_text(ax4, texts2, fontsize=8)
    plt.savefig(os.path.join(dir,u'%s.png'%stockid), dpi=300)





if __name__=="__main__":
    z=top_statistic()
    z.run()