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
        self.stockList = set()
        self.mongodb = pymongo.MongoClient(mongoUrl)
        self.today = datetime.date.today().strftime("%Y%m%d")
        self.yestoday = common.get_last_date(self.today)
        self.yestoday = common.format_date(self.yestoday,"%Y%m%d")
        self.updatefreshlist()      # 次新股
        self.updateForcusList()     # 关注股

    def run(self):
        self.monitor()

    # 昨日的自然涨停股
    def updateForcusList(self):
        result = self.mongodb.stock.ZDT_by_date.find_one({"date": self.yestoday})
        if "actulZtStocks" in result.keys():
            self.forcusList.update(result['actulZtStocks'].split("_"))

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
        root = Tk()
        root.withdraw()  #hide window
        self.fresh_prestr=''
        self.forcus_prestr=""
        while(True):
            time.sleep(6)
            ttime=time.localtime()
            thour=ttime.tm_hour
            tmin=ttime.tm_min
            if (thour > 15) or (thour == 15 and tmin > 1):
                break

            timestamp=time.strftime("%X",time.localtime())
            print timestamp
            # p = multiprocessing.Process(target=self.freshList, args=(u'次新8个点以上提示',"gt",8.5,self.fresh_prestr))
            # p.start()
            self.HandleWindow(self.freshList,u'次新8个点以上提示',"gt",8.5,self.fresh_prestr)
            self.HandleWindow(self.forcusList,u'连板股8个点以上提示',"gt",8.5,self.forcus_prestr)

    def HandleWindow(self, stocklist, message, operate, number, prestr):
        if len(stocklist) < 1:
                return
        telllist = common.WindowShow(stocklist, operate, number, message)
        tellstr = "_".join(telllist)
        if len(prestr) > 0:
            tellstr = prestr+"_"+tellstr
        prestr = tellstr
        self.mongodb.stock.ZDT_by_date.update({"date":self.today},{"$set":{"TelledStocks":tellstr}})
        meaning = [stocklist.remove(x) for x in telllist]  #删除已经提示个股

if __name__ == '__main__':
    z = BuyMonitor()
    z.run()