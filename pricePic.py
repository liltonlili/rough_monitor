#coding:utf8
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import common
import pymongo
from pandas import Series,DataFrame
mongoUrl = "localhost"
mongodb = pymongo.MongoClient(mongoUrl)


# # 输入stock_list，日期，就可以将分时图画出来
# def plot_compare(date,stock_list):
#     sqldb=common.mysqldata()
#     stock_list = [str(x).replace('sh',"").replace('sz',"").replace(".","").replace("SH","").replace("SZ","") for x in stock_list]
#     stock_list = [int(x) for x in stock_list]
#     stock_list = str(stock_list).replace("[","(").replace("]",")")
#     date = common.format_date(date,"%Y%m%d")
#     date = int(date)
#     table = "equity_price%s"%str(date)[:6]
#     sql = "select datadate, ticker, datatime, lastprice from %s where ticker in %s and datadate = %s"%(table,stock_list,date)
#     print sql

def max_value(numDict):
    keys=['+6','6','5','4','3','2']
    for key in keys:
        if numDict[key] > 0:
            return key
    return 0

#
def watch_history(dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
    global mongodb
    dataFrame = pd.DataFrame()
    dateArr = []
    pre_ztm = []
    pre_ztpos = []
    ztnumArr = []
    dtnumArr = []
    maxconZt = []
    maxconDt = []
    querys = common.get_mongoDicts(dateEq=dateEq,dateStart=dateStart,dateEnd=dateEnd)
    for query in querys:
        dateArr.append(query['date'])
        # print query['date']
        pre_ztm.append(query['pre_ztm'])
        if query['pre_ztpos'] > 1:
            query['pre_ztpos'] = None
        pre_ztpos.append(query['pre_ztpos'])
        ztnumArr.append(query['ZT_num'])
        dtnumArr.append(query['DT_num'])
        maxu = max_value(query['num_ucont'])
        maxd = max_value(query['num_dcont'])

        if maxu== '+6':
            maxu = 7
        else:
            maxu = int(maxu)

        if maxd == '+6':
            maxd = 7
        else:
            maxd = int(maxd)

        maxconZt.append(maxu)
        maxconDt.append(maxd)

    dataFrame=pd.concat([Series(dateArr),Series(pre_ztm),Series(pre_ztpos),Series(ztnumArr),Series(dtnumArr),Series(maxconZt),Series(maxconDt)],axis=1)
    dataFrame=dataFrame.dropna()
    dataFrame.columns=['date','pre_ztm','pre_ztpos','ZT_num','DT_num','maxZT','maxDT']
    print dataFrame
    common.plotFrame(dataFrame,x='date',y=[['pre_ztm','pre_ztpos'],['ZT_num','DT_num'],['maxZT','maxDT']],titles=['zt_yes','ZDT_num','maxCon'],point=10)

if __name__ == "__main__":
    watch_history(dateStart="20100108",dateEnd="20160504")