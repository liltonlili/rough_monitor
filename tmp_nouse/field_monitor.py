# coding:utf8
__author__ = 'li.li'
import pymongo
import pandas as pd
import numpy as np
import os
import sys
sys.path.append("D:\Money")
from common import *
import requests
from pandas import DataFrame
import time
import datetime


class fieldMonitor:
    def __init__(self,condition):
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        self.name=condition
        self.run(condition)

    def get_data(self,stock_list,timestamp,i):
        dir = 'D:\Money\Realtime\concept'
        slist=','.join(stock_list)
        url = "http://hq.sinajs.cn/list=%s"%slist
        try:
            r=requests.get(url)
        except:
            r=requests.get(url)
        content=r.content.decode('gbk')
        Dframe=self.parse_content(content,timestamp)
        # Dframe.to_csv(os.path.join(dir,'%s_%s_realtime.csv'%(self.name,i)),encoding='utf8')
        print Dframe.sort_values(by=['concept2','rate'],ascending=False)[['time','stcid','name','rate','concept2']]
        return Dframe

    def parse_content(self,content,timestamp):
        Inframe=DataFrame()
        i = 0
        strarray=content.split(';')
        print content
        for i in range(len(strarray)):
            item=strarray[i]
            item_array=item.split(',')
            if len(item_array)<10:
                continue
            # stockid = item_array[0][14:20]
            stockid = item_array[0].split('=')[0].split('str_')[1][2:]
            stockname= item_array[0].split('="')[1]
            close = item_array[3]
            preclose = item_array[2]
            if close == '0.00':
                continue

            Inframe.loc[i,'time']=timestamp
            Inframe.loc[i,'stcid']=stockid
            Inframe.loc[i,'name']=stockname
            Inframe.loc[i,'close']=close
            Inframe.loc[i,'preclose']=preclose
            Inframe.loc[i,'concept1']=self.concept1_list[i]
            Inframe.loc[i,'concept2']=self.concept2_list[i]
            i+=1
        print Inframe
        Inframe['rate']=100*(Inframe['close'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
        Inframe['rate']=Inframe['rate'].round(decimals=2)
        return Inframe


    def run(self,condition):
        query="select * from stock_concept where %s"%condition
        # print query
        # test=u'房地产'
        # query='select * froom stock_concept where concept2=%s'%test
        # adata=self.sqldb.localdb_query(query)

        tmp_result=self.mongodb.stock.resee_everyday.find(self.name)
        stock_list=[]
        self.concept1_list=[]
        self.concept2_list=[]
        print self.name
        for item in tmp_result:
            code = item['code']
            if code[:2] == '60':
                code = 'sh'+code
            else:
                code = 'sz'+code
            stock_list.append(code)
            self.concept1_list.append(item['concept1'])
            self.concept2_list.append(item['concept2'])
        # tmp_list=['0'*(6-len(str(x)))+str(x) for x in adata['code'].values]
        # stock_list=[]
        # for x in tmp_list:
        #     if x[:2] == '60' or x[:2] == '90':
        #         x='sh'+x
        #     else:
        #         x='sz'+x
        #     stock_list.append(x)
        # self.concept1_list=adata['concept1']
        # self.concept2_list=adata['concept2']
        i=0
        # print stock_list
        # time.sleep(2000)
        while(True):
            timestamp=time.strftime("%X",time.localtime())
            self.get_data(stock_list,timestamp,i)
            time.sleep(10)

# if __name__=='__main__':
#     print u'1: 强势， 2：看好，3：关注，4：巨量'
#     content = input("input:")
#     print u'请输入日期'
#     date=input("date:")
#     if content == 1:
#         tname={"concept1":"强势","date":date}
#     elif content == 2:
#         tname={"concept2":"看好","date":date}
#     elif content == 3:
#         tname={"concept2":"关注","date":date}
#     elif content == 4:
#         tname={"concept4":"巨量","date":date}
#     # tname = sys.argv[1]
#     test=fieldMonitor(tname)

if __name__=='__main__':
    # print u'1: 关注， 2：涨停放量，3：跌停放量，4：巨量'
    # content = input("input:")
    # print u'请输入日期'
    # date=input("date:")
    content=1
    date = "20160414"
    if content == 1:
        tname={"concept1":"强势","date":date}
    elif content == 2:
        tname={"concept2":"看好","date":date}
    elif content == 3:
        tname={"concept2":"关注","date":date}
    elif content == 4:
        tname={"concept4":"巨量","date":date}
    # tname = sys.argv[1]
    test=fieldMonitor(tname)