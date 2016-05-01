##coding:utf-8
import sys
sys.path.append("D:\Money")
from common import *
import pandas as pd
import os
import datetime
import report_env
import numpy as np
import pymongo
import time
import logging
import requests
from lxml import etree


def parse_content(content):
    root = etree.HTML(content)
    trs = root.xpath('//table[@class="table_data"]//tr')
    stock_ids = root.xpath('//table[@class="table_data"]//tr/td[2]/a/text()')
    del stock_ids[0]    ## 表格头
    stock_dates = root.xpath('//table[@class="table_data"]//tr/td[12]/text()')
    stock_types = root.xpath('//table[@class="table_data"]//tr/td[13]/text()')      ##一般都是首发
    if (len(stock_ids) != len(stock_dates)) or (len(stock_dates) != len(stock_types)):
        print "the length is not the same, stop to recheck"
        raise Exception
    return [stock_ids,stock_dates,stock_types]



def update_Mongo(stockId,stockDate):
    global mongodb
    sResults = mongodb.stock.ZDT_by_date.find_one({"date":stockDate})
    if sResults is not None and sResults.has_key("Add_newStocks") and stockId in sResults['Add_newStocks']:
        return 1
    else:
        mongodb.stock.ZDT_by_date.update({"date":stockDate},{"$set":{"Add_newStocks.%s"%stockId:True}},True)
        print "add 1 new stock on %s, %s"%(stockDate,stockId)
        return 1



pages = range(1,35)
mongoUrl = "localhost"
mongodb = pymongo.MongoClient(mongoUrl)
for page in pages:
    url = "http://data.cfi.cn/cfidata.aspx?sortfd=&sortway=&curpage=%s&fr=content&ndk=A0A1934A1939A1946A1982&xztj=&mystockt="%page
    print "begin get page %s"%page
    for i in range(0,5):
        r = requests.get(url=url)
        if r.status_code == 200:
            break
        else:
            time.sleep(2)
    print "connected %s successfully"%page
    content = r.content

    try:
        [stock_ids,stock_dates,stock_types] = parse_content(content)
    except Exception,e:
        print e

    for i in range(len(stock_ids)):
        stockId = stock_ids[i].strip()
        stockDate = stock_dates[i].strip()
        if stockDate == "--":
            continue
        else:
            update_Mongo(stockId,stockDate)




