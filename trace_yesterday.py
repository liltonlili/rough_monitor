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
    def __init__(self, day = datetime.date.today().strftime("%Y%m%d"), force_dump=0):
        # day = "20170519"
        cday = common.format_date(day, "%Y/%m/%d")
        self.force_dump = force_dump
        self.today = common.format_date(day, "%Y%m%d")
        self.redis = redis.Redis(host='localhost', port=6379, db=1)
        # 如果不是当天，则需要重新写redis的值
        actual_day = datetime.date.today()
        actual_day = common.format_date(actual_day, "%Y%m%d")
        if self.today != actual_day:
            self.redis_history_value(self.today)

        yesterday = common.get_last_date(cday)
        yesterday_mongo = common.format_date(yesterday, "%Y%m%d")
        yesterday = common.format_date(yesterday, "%Y-%m-%d")

        self.yesterday=yesterday
        ## get info of yesterday
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        self.up10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['actulZtStocks'].split("_")
        self.hi10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['HD_stocks'].split("_")
        self.low10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['LD_stocks'].split("_")
        self.dn10_list = self.mongodb.stock.ZDT_by_date.find_one({"date":yesterday_mongo})['DT_stocks'].split("_")
        self.count=0

    #           TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE  \
    # 0        000001           平安银行  2017-02-27             9.50        9.50
    #
    #    HIGHEST_PRICE  LOWEST_PRICE  CLOSE_PRICE  ACT_PRE_CLOSE_PRICE  rate
    # 0           9.50          9.42         9.43                 9.50 -0.74
    # 从数据库中读取数据， 更新到redis中
    def redis_history_value(self, histday):
        # 读取到所有当天股票的相关值
        histdate_frame = common.get_mysqlData([], [histday])
        histdate_frame['rate'] = 100*(histdate_frame['CLOSE_PRICE'] - histdate_frame['ACT_PRE_CLOSE_PRICE'])/histdate_frame['ACT_PRE_CLOSE_PRICE']
        histdate_frame['rate'] = histdate_frame['rate'].round(2)

        # 清空redis数据
        self.redis.flushdb()

        # 更新redis的数据
        for idx in histdate_frame.index.values:
            close = histdate_frame.loc[idx, 'CLOSE_PRICE']
            preclose = histdate_frame.loc[idx, 'ACT_PRE_CLOSE_PRICE']
            high = histdate_frame.loc[idx, 'HIGHEST_PRICE']
            low = histdate_frame.loc[idx, 'LOWEST_PRICE']
            rate = histdate_frame.loc[idx, 'rate']
            key_name = histdate_frame.loc[idx, 'TICKER_SYMBOL']
            self.redis.set(key_name, [close, preclose, high, low, rate])
        print "update history price in redis finished, day:%s" %histday

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
        if len(tframe) != 0:
            pos_rate=round(float(pos)/len(tframe),2)
        else:
            pos_rate = 0
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

            # if 1:
            if (thour > 18) or (thour == 18 and tmin > 10) or self.force_dump == 1:
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
                common.resee_info_gjsh()    # 收集复盘信息
                # 这一步需要从数据库中读取概念信息
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
        while 1:
            detailinfos = self.mongodb.stock.ZDT_by_date.find_one({"date":self.today})
            if u'actulZtStocks' in detailinfos.keys():
                break
            else:
                time.sleep(60)
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
            # if (thour > 17) or (thour == 17 and tmin > 5):
            if (thour > 18) or (thour == 18 and tmin > 5) or self.force_dump == 1:
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

        # 生成 "bat"， 用来复盘后生成html文件
        with open(os.path.join(constant_dir, u"1坚持自己.bat"), 'wb') as fHandler:
            fHandler.write(ur"@start cmd /k python D:\Money\lilton_code\Market_Mode\rocketup\strategy\intelligent_eye.py")
            # fHandler.write(ur"@start cmd /k python D:\Money\lilton_code\Market_Mode\rocketup\strategy\manage_cache_L1.py")


    def get_csv(self, detailinfos, png_enable = 0):
        ztStocks = detailinfos['actulZtStocks'].split("_")
        dtStocks = detailinfos['DT_stocks'].split("_")
        meatStocks = detailinfos['meatList'].split("_")
        holeStocks = detailinfos['holeList'].split("_")
        hdStocks = detailinfos['HD_stocks'].split("_")

        # 拿到数据库中股金神话的复盘信息
        # 由于股金神话暂停了，所以暂时不再拿复盘信息
        self.gjsh_mirror_info = {}
        # gjsh_mirror_info = self.mongodb.stock.mirror_info.find({"date":self.today, "source":"GJSH_B"})
        # if gjsh_mirror_info.count() > 0:
        #     self.gjsh_mirror_info = gjsh_mirror_info[0]
        # else:
        #     self.gjsh_mirror_info = {}

        # 拿到金融界上的news和复盘信息, dict为：{stockid:[concept, reason]}
        stock_news_dict = common.get_jrj_news()
        xgb_stock_news_dict = common.get_xgb_news()
        i = 0
        Aframe=DataFrame()
        c_date = self.today
        constant_dir = os.path.join(u'D:/Money/modeResee/复盘', c_date)
        if not os.path.exists(constant_dir):
            os.makedirs(constant_dir)

        type_list = ['ZT', 'HD', 'DT', 'meat', 'hole']
        stock_list = [ztStocks, hdStocks, dtStocks, meatStocks, holeStocks]
        for t in range(0, len(type_list)):
            stocks_list = stock_list[t]
            stock_type = type_list[t]
            for stockid in stocks_list:
                print "stockid:", stockid
                stockid = common.regulize_stockid(stockid)
                if len(stockid) != 6:
                    continue

                Aframe.loc[i,'stock'] = stockid
                Aframe.loc[i,'name'] = common.QueryStockMap(id = stockid)[0]
                Aframe.loc[i,'reason']='0'
                Aframe.loc[i,'type']=stock_type
                Aframe.loc[i,'desc']='record'


                # 先获得对于的group以及stocklist
                [group, group_stocklist] = common.find_concept(stockid, self.today)
                # print group_stocklist
                if len(group_stocklist) > 8:  # 太长了显示出来也没用
                    anotation = "too long"
                else:
                    anotation_list = [common.QueryStockMap(x)[0] for x in group_stocklist]
                    anotation_list = [x for x in anotation_list if x != 0]
                    anotation = ",".join(anotation_list)

                # 从L1, L2中得到的group为空，且金融界有相关的group，则用金融界的记录
                if len(group_stocklist) < 1 and stockid in stock_news_dict.keys():
                    group = stock_news_dict[stockid][0]    # 说明数据库还没记录概念，但金融界网站已经记录了

                # 如果还是没有，则选用选股宝的信息
                if len(group)<1 and stockid in xgb_stock_news_dict.keys():
                    group = xgb_stock_news_dict[stockid][0].split("|")[0].replace("+", "_")


                # 将概念和相关股票打印进去
                Aframe.loc[i, 'group'] = group
                inL1 = common.exist_in_cache(stockid, group, 1)
                inL2 = common.exist_in_cache(stockid, group, 2)
                Aframe.loc[i, 'inL1'] = inL1
                Aframe.loc[i, 'inL2'] = inL2
                Aframe.loc[i, 'anotation'] = anotation
                Aframe.loc[i, 'reason2'] = ''
                news = common.get_latest_news(stockid)


                # 将选股宝的信息整合
                if stockid in xgb_stock_news_dict.keys():
                    Aframe.loc[i, 'source_xgb'] = xgb_stock_news_dict[stockid][0]
                    news = "%s\n%s" % (xgb_stock_news_dict[stockid][0], news)

                # 将股金神话的信息整合
                if stockid in self.gjsh_mirror_info.keys():
                    Aframe.loc[i, 'source_gjsh'] = self.gjsh_mirror_info[stockid]['reason']
                    big_concept = ""
                    if 'big_concept' in self.gjsh_mirror_info[stockid].keys():
                        big_concept = self.gjsh_mirror_info[stockid]['big_concept']
                    news = "%s\n%s\n%s" % (big_concept, self.gjsh_mirror_info[stockid]['reason'], news)


                # 将金融界的信息整合
                if stockid in stock_news_dict.keys():
                    news = "%s\n%s" % (stock_news_dict[stockid][1], news)

                Aframe.loc[i, 'news'] = news
                if png_enable == 1:
                    generate_fp_pic(stockid, constant_dir, Aframe.loc[i,'news'], self.today)
                i += 1
        return Aframe


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
    # # 补历史
    # dateStart = '20170517'
    # dateEnd = '20170517'
    # dateDicts = common.get_mongoDicts(dateStart=dateStart,dateEnd=dateEnd)
    # for dateDict in dateDicts:
    #     cdate=dateDict['date']
    #     print cdate
    #     z=top_statistic(day = cdate, force_dump = 1)
    #     z.run()

    # z=top_statistic(day=datetime.date.today().strftime("%Y%m%d"), force_dump=1)
    z=top_statistic(day="20170721", force_dump=1)
    z.run()