#coding:utf8
import pymongo
import pandas as pd
from pandas import DataFrame,Series
import numpy as np
import datetime
import common
import time
from Tkinter import *
from tkMessageBox import *
import multiprocessing

class BuyMonitor:
    def __init__(self):
        mongoUrl = "localhost"
        self.freshList = set()
        self.forcusList = set()
        self.nearNewHigh = set()
        self.stockList = set()
        self.mongodb = pymongo.MongoClient(mongoUrl)
        self.today = datetime.date.today().strftime("%Y%m%d")
        # self.today = "20160520"
        self.yestoday = common.get_last_date(self.today)
        self.yestoday = common.format_date(self.yestoday,"%Y%m%d")
        self.updateNewHighlist()
        while(True):
            ttime=time.localtime()
            thour=ttime.tm_hour
            tmin=ttime.tm_min
            if (thour > 9) or (thour == 9 and tmin >= 30):
                break
            time.sleep(10)

        self.updatefreshlist()      # 次新股
        self.updateForcusList()     # 关注股

    def run(self):
        self.monitor()

    # 昨日的自然涨停股
    def updateForcusList(self):
        result = self.mongodb.stock.ZDT_by_date.find_one({"date": self.yestoday})
        if "actulZtStocks" in result.keys():
            self.forcusList.update(result['actulZtStocks'].split("_"))
        print "update forcus stocks finished!"


    def updateNewHighlist(self):
        results = self.mongodb.stock.HighestPrice.find({})
        dframe = pd.DataFrame()
        count = 0
        for result in results:
            dframe.loc[count,'stcid'] = result['stcid']
            dframe.loc[count,'Hprice'] = result['Hprice']
            count += 1

        stocklist = dframe['stcid'].values
        dframe.set_index('stcid',inplace=True)
        zframe = common.get_sina_data(stocklist)
        zframe = zframe[['stcid','close']]
        zframe.set_index('stcid',inplace=True)
        dframe = pd.concat([dframe,zframe],axis=1)
        dframe.reset_index(len(dframe),inplace=True)
        dframe.columns = ['stcid','Hprice','close']
        dframe['hrate'] = dframe['close'].astype(np.float64)/dframe['Hprice'].astype(np.float64)
        dmframe = dframe[dframe.hrate > 0.95]
        duframe = dframe[dframe.hrate >= 1]
        freshstocks = self.mongodb.stock.ZDT_by_date.find_one({"date":self.yestoday})['freshStocks'].split("_")

        self.nearNewHigh.update([x for x in dmframe.stcid.values if x not in freshstocks])
        self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{"nearNewHigh":"_".join(self.nearNewHigh),"date":self.today}},True)

        for i in duframe.index.values:
            stcid = duframe.loc[i,'stcid']
            price = duframe.loc[i,'close']
            self.mongodb.stock.HighestPrice.update({"stcid":stcid},{"$set":{"Hdate":common.format_date(self.today,"%Y/%m/%d"),"Hprice":price}},True)
            print "update highest for %s @ %s"%(stcid,price)


    # 半年内的次新股
    def updatefreshlist(self):
        tardate = common.get_lastN_date(self.today, 120)
        tardate = common.format_date(tardate, "%Y%m%d")

        results = self.mongodb.stock.ZDT_by_date.find({"date": {"$gte": tardate, "$lte": self.today},"Add_newStocks": {"$exists": True}})
        for result in results:
            tmp = result['Add_newStocks'].keys()
            if u'未公布' in tmp:
                tmp.remove(u'未公布')
            self.freshList.update(tmp)
        tmp = list(self.freshList)
        todayfresh = self.mongodb.stock.ZDT_by_date.find_one({'date':self.yestoday})['freshStocks']
        tmp = [x for x in self.freshList if x not in todayfresh]
        self.freshList = set(tmp)
        self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{"monitorFreshStocks":"_".join(self.freshList),"date":self.today}},True)
        print "update Monitored fresh stocks finished!"

    def monitor(self):
        self.freshList=list(self.freshList)
        self.forcusList=list(self.forcusList)
        self.nearNewHigh = list(self.nearNewHigh)
        # root = Tk()
        # root.withdraw()  #hide window
        self.fresh_prestr=''
        self.forcus_prestr=""
        while(True):
            time.sleep(6)
            ttime=time.localtime()
            thour=ttime.tm_hour
            tmin=ttime.tm_min


            timestamp=time.strftime("%X",time.localtime())

            print timestamp
            # p = multiprocessing.Process(target=self.freshList, args=(u'次新8个点以上提示',"gt",8.5,self.fresh_prestr))
            # p.start()
            self.HandleWindow(self.freshList,u'次新7个点以上提示',"gt",7,"TelledFreshStocks")
            self.HandleWindow(self.forcusList,u'连板股7个点以上提示',"gt",7,"TelledForcusStocks")
            self.HandleWindow(self.nearNewHigh,u'接近新高股8个点以上提示',"gt",8,"NewHighStocks")

            # ##强势股（次新和连板）8.8个点提示
            self.HandleHigher()
            if (thour > 15) or (thour == 15 and tmin > 1):
                break

    def HandleHigher(self):
        result = self.mongodb.stock.ZDT_by_date.find_one({"date":self.today})

        stocklist = set()
        if "TelledFreshStocks" in result.keys():
            stocklist.update(result['TelledFreshStocks'].split("_"))
        if "TelledForcusStocks" in result.keys():
            stocklist.update(result['TelledForcusStocks'].split("_"))
        if "TelledForcusStocks" in result.keys():
            stocklist.update(result['nearNewHigh'].split("_"))
        stocklist = list(stocklist)
        excludelist = []
        if "HigherExclude" in result.keys():
            excludelist = result['HigherExclude'].split("_")
        stocklist = [x for x in stocklist if x not in excludelist]
        self.HandleWindow(list(stocklist),u"强势股8.8个点以上提示","gt",8.8, "HigherExclude")

    def HandleWindow(self, stocklist, message, operate, number, keys=None):
        if len(stocklist) < 1 or (len(stocklist) == 1 & (stocklist[0]==u'')):
            return
        telllist = common.WindowShow(stocklist, operate, number, message)
        tellstr = "_".join(telllist)
        if tellstr == '_':
            tellstr = None
        result = self.mongodb.stock.ZDT_by_date.find_one({"date":self.today})
        prestr = ''
        if keys in result.keys():
            prestr = result[keys]
        if tellstr is None or len(tellstr) < 3:
            return
        print tellstr
        if len(prestr) > 0:
            tellstr = prestr+"_"+tellstr

        if keys is not None:
            self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{keys:tellstr}})
        meaning = [stocklist.remove(x) for x in telllist]  #删除已经提示个股

if __name__ == '__main__':
    z = BuyMonitor()
    z.run()