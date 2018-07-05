# coding:utf8
__author__ = 'li.li'

import pymongo
import pandas as pd
import numpy as np
import os
import sys
sys.path.append("D:\Money")
import time
from common import *

mongo_url = "localhost"
mongodb = pymongo.MongoClient(mongo_url)
mongo_url2='mongodb://dpipe:dpipe@nosql04-dev.datayes.com'
mongodb2 = pymongo.MongoClient(mongo_url2)

#test
tlist=mongodb.stock.stock_concept.find_one({'code':'000001'})



# Tframe=pd.read_csv("D:\Money\lilton_code\Market_Mode\concept.csv",encoding='gbk')
# del Tframe["Unnamed: 0"]
# for index in Tframe.index:
#     item={}
#     code=Tframe.loc[index,'code']
#     item['concept1']=Tframe.loc[index,'concept1']
#     item['concept2']=Tframe.loc[index,'concept2']
#     item['concept3']=Tframe.loc[index,'concept3']
#     item['concept4']=Tframe.loc[index,'concept4']
#     item['concept5']=Tframe.loc[index,'concept5']
#     item['concept6']=Tframe.loc[index,'concept6']
#     item['name']=Tframe.loc[index,'name']
#     code=str(code)
#     if len(code)<6:
#         code='0'*(6-len(code))+code
#     item['code']=code
#     mongodb.stock.stock_concept.insert(item)

Tframe=pd.read_csv("D:\Money\\tenPercent\\record.csv",encoding='gbk')
del Tframe["Unnamed: 0"]
for index in Tframe.index:
    item={}
    code=Tframe.loc[index,'code']
    item['date']=Tframe.loc[index,'date']
    item['concept1']=Tframe.loc[index,'concept1']
    item['concept2']=Tframe.loc[index,'concept2']
    item['concept3']=Tframe.loc[index,'concept3']
    item['concept4']=Tframe.loc[index,'concept4']
    item['concept5']=Tframe.loc[index,'concept5']
    item['concept6']=Tframe.loc[index,'concept6']
    code=str(code)
    if len(code)<6:
        code='0'*(6-len(code))+code
    item['code']=code
    if code[0:2]=='60':
        code='SH'+code+'CN'
    else:
        code = 'SZ'+code+'CN'
    name = mongodb2.reports_db.stock.find_one({"stockid":code})['stock_name']
    item['name']=name
    print item['name'],item['code']
    mongodb.stock.resee_everyday.insert(item)