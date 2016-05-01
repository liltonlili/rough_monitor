#coding:utf8
import pandas as pd
import pymongo
import os
import datetime

##将excel(2列)录入mongo中，格式为：
## {"date":XXX,"record":{"stock_name":"reason","stock_name":"reason"}
dir = "D:\\Money\\modeResee"
aframe=pd.read_csv(os.path.join(dir,"daydayup.csv"),encoding='gbk')

mongo_url = "localhost"
mongodb = pymongo.MongoClient(mongo_url)

date_str=datetime.date.today().strftime("%Y%m%d")
today_dict={"date":date_str}

## my record
rframe=aframe[(aframe.desc=='record') & (aframe.reason != "0")]
tmp_dict={}
for i in rframe.index.values:
    stock=rframe.loc[i,"stock"]
    stock = str(int(stock))
    stock="0"*(6-len(str(stock)))+str(stock)
    tmp_dict[stock]=rframe.loc[i,'reason']

# ## 昨日涨停，今日大肉
# meatframe=aframe[aframe.desc=='meat']
# meat_dict={}
# for i in meatframe.index.values:
#     stock=meatframe.loc[i,"stock"]
#     stock="0"*(6-len(str(stock)))+str(stock)
#     meat_dict[stock]=meatframe.loc[i,'reason']
#
# ## 昨日涨停，今日大坑
# holeframe=aframe[aframe.desc=='hole']
# hole_dict={}
# for i in holeframe.index.values:
#     stock=holeframe.loc[i,"stock"]
#     stock="0"*(6-len(str(stock)))+str(stock)
#     hole_dict[stock]=holeframe.loc[i,'reason']
#
# ## 今日强势板块
# stroframe = aframe[aframe.desc=='strong']
# strong_dict={}
# for i in stroframe.index.values:
#     stock=stroframe.loc[i,"stock"]
#     strong_dict[stock]=stroframe.loc[i,'reason']

##ZT_Mount
task=mongodb.stock.ZDT_by_date.find_one({"date":date_str})
ZTM_list=task['ZT_Mount'].split("_")
DTM_list=task['DT_Mount'].split("_")
ZTM_dict={}
DTM_dict={}
for ZTM_stock in ZTM_list:
    ZTM_dict[ZTM_stock]='ZT_Mount'

for DTM_stock in DTM_list:
    DTM_dict[DTM_stock]='DT_Mount'

today_dict['record']=tmp_dict
today_dict['ZTMount']=ZTM_dict
today_dict['DTMount']=DTM_dict
# today_dict['Meat']=meat_dict
# today_dict['Hole']=hole_dict
# today_dict['Strong_region']=strong_dict

mongodb.stock.resee_everyday.save(today_dict)
