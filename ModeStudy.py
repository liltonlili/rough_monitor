#coding:utf8
import pandas as pd
from pandas import DataFrame
import time
import common
import pymongo
import os
import LearnFrom
import multiprocessing


def monitored_fresh(date):
    tardate = common.get_lastN_date(date,120)
    freshList = set()
    results = mongodb.stock.ZDT_by_date.find({"date": {"$gte": tardate, "$lte": date},"Add_newStocks": {"$exists": True}})
    for result in results:
        tmp = result['Add_newStocks'].keys()
        if u'未公布' in tmp:
            tmp.remove(u'未公布')
        freshList.update(tmp)
    tmp = list(freshList)
    todayfresh = mongodb.stock.ZDT_by_date.find_one({'date':date})['freshStocks']
    tmp = [x for x in freshList if x not in todayfresh]
    freshList = list(set(tmp))
    mongodb.stock.ZDT_by_date.update({"date":date},{"$set":{"monitorFreshStocks":"_".join(freshList),"date":date}},True)
    return freshList


def study_plot(stcid,date,plot_dir):
    # print stcid,date
    tmp_array = [date,None,stcid,u'买入',100]
    LearnFrom.study_plot(tmp_array,plot_dir)
    nextdate = common.get_lastN_date(date,-1)
    tmp_array = [nextdate,None,stcid,u'卖出',100]
    LearnFrom.study_plot(tmp_array,plot_dir)
    print "finish %s"%stcid

if __name__ == "__main__":
    # plot_dir = "D:\Money\lilton_code\Market_Mode\studyModule"
    plot_dir = "D:\Money\lilton_code\Market_Mode\study_fresh_fail_Module"
    mongourl = "localhost"
    mongodb = pymongo.MongoClient(mongourl)
    start = "20160122"
    results = mongodb.stock.ZDT_by_date.find({"date":{"$gte":start}})
    for result in results:
        date = result['date']
        print date
        if "monitorFreshStocks" not in result.keys():
            freshList = monitored_fresh(date)
        else:
            freshList = result['monitorFreshStocks'].split("_")
        # todayZt = result['ZT_stocks'].split("_")
        todayZt = result['HD_stocks'].split("_")
        tarList = [x for x in freshList if x in todayZt]
        for stcid in tarList:
            study_plot(stcid,date,plot_dir)
