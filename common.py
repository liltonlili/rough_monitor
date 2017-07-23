#coding:utf8
from mysqldb import *
import pandas as pd
import datetime
import os
import numpy as np
import tushare as ts
import time
import requests
import pymongo
from pandas import DataFrame,Series
import matplotlib.pyplot as plt
from Tkinter import *
from tkMessageBox import *
import Tkinter as tk
import json
import matplotlib as mpl
from matplotlib import *
from matplotlib.font_manager import FontProperties
from lxml import etree
from selenium import webdriver
sys.path.append("D:/projects/report_download/src/lib")
import http_downloader

# myFmt = mpl.mdates.DateFormatter('%Y-%m-%d')
myfont = mpl.font_manager.FontProperties(fname=os.path.join(u'C:/Windows/Fonts','wqy-microhei.ttc'))
mpl.rcParams['axes.unicode_minus'] = False
mongourl = "localhost"
global mongodb, backsee_csv
mongodb = pymongo.MongoClient(mongourl)
backsee_csv = u'D:/Money/modeResee/复盘'



def connectdb():
    mydbs='a'
    # mydb='b'
    # dydb='c'
    # mydbs=mysqldb({'host': 'db-bigdata.wmcloud-qa.com', 'user': 'app_bigdata_ro', 'pw': 'Welcome_20141217', 'db': 'bigdata', 'port': 3312})
    mydb = mysqldb({'host': '10.21.232.43', 'user': 'app_gaea_ro', 'pw': 'Welcome20150416', 'db': 'MarketDataL1', 'port': 5029})  ##分笔，分钟级
    dydb  = mysqldb({'host': 'db-datayesdb-ro.wmcloud.com', 'user': 'app_gaea_ro', 'pw': 'EQw6WquhnCKPp8Li', 'db': 'datayesdbp', 'port': 3313})
    # cardb = mysqldb({'host': 'db-news.wmcloud-stg.com', 'user': 'app_bigdata_ro', 'pw': 'Welcome_20141217', 'db': 'news', 'port': 3310})
    cardb = 'A'
    souhudbs = mysqldb({'host': 'db-datayesdb-ro.wmcloud.com', 'user': 'app_gaea_ro', 'pw': 'EQw6WquhnCKPp8Li', 'db': 'datayesdb', 'port': 3313})
    # souhudbi = mysqldb({'host': 'db-bigdata.wmcloud-qa.com', 'user': 'app_bigdata_ro', 'pw': 'Welcome_20141217', 'db': 'bigdata', 'port': 3312})
    souhudbi = mydbs
    # localdb = mysqldb({'host': '127.0.0.1', 'user': 'root', 'pw': '', 'db': 'stock', 'port': 3306})
    localdb = None
    return (mydbs,mydb,dydb,localdb,cardb,souhudbs,souhudbi)

class mysqldata:
    def __init__(self):
        (self.mydbs,self.mydb,self.dydb,self.localdb,self.cardb,self.souhudbs,self.souhudbi)=connectdb()

    def dydbs_query(self,sqlquery):
        self.mydbs.db.ping(True)
        self.mydbs.db.cursor()
        return pd.read_sql(sqlquery,con=self.mydbs.db)

    def dydb_query(self,sqlquery):
        self.dydb.db.cursor()
        return pd.read_sql(sqlquery,con=self.dydb.db)

    def mydb_query(self,sqlquery):
        self.mydb.db.ping(True)
        self.mydb.db.cursor()
        return pd.read_sql(sqlquery,con=self.mydb.db)

    def localdb_query(self,sqlquery):
        self.localdb.db.ping(True)
        self.localdb.cursor()
        return pd.read_sql(sqlquery,con=self.localdb.db)

    def car_query(self,sqlquery):
        self.cardb.db.ping(True)
        self.cardb.cursor()
        return pd.read_sql(sqlquery,con=self.cardb.db)

    def souhus_query(self,sqlquery):
        self.souhudbs.db.ping(True)
        self.souhudbs.cursor()
        return pd.read_sql(sqlquery,con=self.souhudbs.db)

    def souhui_query(self,sqlquery):
        self.souhudbi.db.ping(True)
        self.souhudbi.cursor()
        return pd.read_sql(sqlquery,con=self.souhudbi.db)
    ## stockid,stockname,concept1,concept2,concept3,concept4,concept5,concept6
    def generate_localdb_query(self,*args):
        id=args[0]
        name=args[1]
        item1=args[2]
        arrs=['NPC','NPC','NPC','NPC','NPC']
        for i in range(3,len(args)):
            arrs[i-3]=args[i]
        sqlcomd="insert into `stock_concept` values(%s,'%s','%s','%s','%s','%s','%s','%s')"%(id,name,item1,arrs[0],arrs[1],arrs[2],arrs[3],arrs[4])
        return sqlcomd


    def localdb_write(self,sqlquery):
        try:
            self.localdb.upload(sqlquery)
            return 1
        except Exception,e:
            return e

global mysqldb
mysqldb = mysqldata()

## [(2010,1),(2010,2)]
def get_ympair(sty,stm,eny,enm):
    tlist=[]
    cur_y=sty
    cur_m=stm
    while(True):
        tlist.append((cur_y,cur_m))
        if cur_m == 12:
            cur_m = 1
            cur_y += 1
        else:
            cur_m += 1

        if cur_y > eny:
            break
        elif (cur_y==eny) and (cur_m > enm):
            break
    return tlist

def get_day(value,str_date):
    calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
    del calframe['0']
    calframe.columns=['Time']
    daylist=list(calframe['Time'].values)
    date_value=calframe[calframe.Time<=str_date].tail(1).Time.values
    index=daylist.index(date_value)
    tar_index=index+value
    tar_date=calframe.loc[tar_index,'Time']
    return tar_date




## 根据起始年月和终止年月，得到日期列表
## ["2015/01/01",'2015/01/02",...]
class gm_date:
    def __init__(self,sty,stm,eny,enm):
        self.sty=sty
        self.stm=stm
        self.eny=eny
        self.enm=enm
        self.run()

    def run(self):
        self.month_list=[]
        tmp_y=self.sty
        tmp_m=self.stm
        calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
        del calframe['0']
        calframe.columns=['Time']
        start_day=datetime.date(year=self.sty,month=self.stm,day=1).strftime("%Y/%m/%d")
        try:
            end_day=datetime.date(year=self.eny,month=self.enm,day=31).strftime("%Y/%m/%d")
        except:
            try:
                end_day=datetime.date(year=self.eny,month=self.enm,day=30).strftime("%Y/%m/%d")
            except:
                try:
                    end_day=datetime.date(year=self.eny,month=self.enm,day=29).strftime("%Y/%m/%d")
                except:
                    try:
                        end_day=datetime.date(year=self.eny,month=self.enm,day=28).strftime("%Y/%m/%d")
                    except:
                            end_day=datetime.date(year=self.eny,month=self.enm,day=27).strftime("%Y/%m/%d")
        calframe=calframe[(calframe.Time>=start_day) & (calframe.Time<=end_day)]
        calList=calframe['Time'].values
        self.calList=list(calList)





## 根据起始年月和终止年月，得到日期列表
## ["2015/01/01",'2015/01/02",...]
def getDate(startdate,enddate):
    startdate = format_date(startdate,"%Y/%m/%d")
    enddate = format_date(enddate,"%Y/%m/%d")
    calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
    del calframe['0']
    calframe.columns=['Time']
    calframe=calframe[(calframe.Time>=startdate) & (calframe.Time<=enddate)]
    calList=calframe['Time'].values
    return list(calList)


## 输入格式不限
## 根据股票代码和日期获取交易数据
## 如果stock_list长度为0，则选出所有股票
## 输出dataFrame格式为：

#      TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE    HIGHEST_PRICE   LOWEST_PRICE  CLOSE_PRICE
# 0           000033          *ST新都  2016-04-27            10.38        0.00           0.00          0.00        10.38
# 1           600710          *ST常林  2016-04-27             9.36        0.00         10.61         10.52        10.57

def get_mysqlData(stock_list,date_list):
    global mysqldb
    # mysqldb = mysqldata()
    stock_list = [str(x).replace('sh',"").replace('sz',"").replace(".","").replace("SH","").replace("SZ","") for x in stock_list]
    stock_list = ['0'*(6-len(str(x)))+str(x) for x in stock_list]
    date_list = [format_date(x,"%Y-%m-%d") for x in date_list]
    date_list = str(date_list).replace("[","(").replace("]",")")
    # print stock_list
    # print date_list

    table="vmkt_equd"
    if len(stock_list) > 0:
        stock_list = str(stock_list).replace("[","(").replace("]",")")
        query = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, ACT_PRE_CLOSE_PRICE " \
                "from %s where TICKER_SYMBOL in %s and TRADE_DATE in %s"%(table,stock_list, date_list)
    else:
        query = 'SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, ACT_PRE_CLOSE_PRICE  ' \
                'from %s where TRADE_DATE in %s and TICKER_SYMBOL < "700000" '%(table,date_list)
    # print query
    dataFrame = mysqldb.dydb_query(query)
    return dataFrame



## 执行mysql，返回dataframe
def get_mysqlData_sqlquery(sqlquery):
    # mysqldb = mysqldata()
    global mysqldb
    dataFrame = mysqldb.dydb_query(sqlquery)
    return dataFrame

## 执行mysql，返回dataframe
def get_mydb_sqlquery(sqlquery):
    global mysqldb
    dataFrame = mysqldb.mydb_query(sqlquery)
    return dataFrame

#             open  high  close   low     volume     amount    pre_close   change_rate
# date
# 2014-03-21  4.51  4.67   4.65  4.44  116133536  534399872    XXXX            0.1
# 得到股票在某一天,以及前一天的的交易数据
# 输入date为['2016-03-27']
def get_value(stoc_list,date):
    try:
        date = datetime.datetime.strptime(date,"%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        try:
            date = datetime.datetime.strptime(date,"%Y/%m/%d").strftime("%Y-%m-%d")
        except:
            try:
                date = datetime.datetime.strptime(date,"%Y%m%d").strftime("%Y-%m-%d")
            except:
                pass
    last_date=get_last_date(date)
    vframe=pd.DataFrame()
    global mysqldb
    sqldb = mysqldb
    # sqldb=mysqldata()
    #      TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE    HIGHEST_PRICE   LOWEST_PRICE  CLOSE_PRICE
# 0           000033          *ST新都  2016-04-27            10.38        0.00           0.00          0.00        10.38
# 1           600710          *ST常林  2016-04-27             9.36        0.00         10.61         10.52        10.57
    vframe = get_mysqlData(stoc_list,[date])
    vframe = vframe[['TICKER_SYMBOL','OPEN_PRICE','HIGHEST_PRICE','CLOSE_PRICE','LOWEST_PRICE','PRE_CLOSE_PRICE','TRADE_DATE']]
    vframe.columns=['stcid','open','high','close','low','pre_close','date']
    vframe['change_rate']=100*(vframe['close'].astype(np.float64)-vframe['pre_close'].astype(np.float64))/vframe['pre_close'].astype(np.float64).round(decimals = 2)
    # for stoc in stoc_list:
    #     ## this entry is to get data from tushare but too slow and too many exceptions
    #     # try:
    #     #     tmpframe=ts.get_h_data(stoc,start=last_date,end=date)
    #     # except:
    #     #     print "re-connect"
    #     #     time.sleep(5)
    #     #     tmpframe=ts.get_h_data(stoc,start=last_date,end=date)
    #     ##
    #     try:
    #         ## this entry is to get data from datayes database
    #         tmpframe=get_tushare_frame(stoc,last_date,date,sqldb)
    #         #             stcid open  high  close   low     volume     amount
    #         # date
    #         tframe=tmpframe[tmpframe.index==datetime.datetime.strptime(date,"%Y-%m-%d").date()]
    #         tframe['pre_close']=tmpframe[tmpframe.index==datetime.datetime.strptime(last_date,"%Y-%m-%d").date()].close.values[0]
    #         tframe['change_rate']=100*(float(tframe.close.values[0])-float(tframe.pre_close.values[0]))/float(tframe.pre_close.values[0])
    #         tframe['change_rate']=tframe['change_rate'].round(decimals=2)
    #         vframe=pd.concat([vframe,tframe],axis=0)
    #     except Exception,e:
    #         pass
    return vframe

#             stcid open  high  close   low     volume     amount
# date
# 2014-03-21  601989 4.51  4.67   4.65  4.44  116133536  534399872
# 得到股票在某一天以及前一天的交易数据，数据来源：数据库
# 输入date为['2016-03-27']
## 股票代码为：601989

def get_tushare_frame(stoc,last_date,date,sqldb):
    last_date = format_date(last_date,"%Y-%m-%d")
    date = format_date(date,"%Y-%m-%d")
    table="vmkt_equd"
    # sql='select TRADE_DATE,TICKER_SYMBOL,OPEN_PRICE,HIGHEST_PRICE,CLOSE_PRICE,LOWEST_PRICE,CLOSE_PRICE,CLOSE_PRICE from %s \
    #     where TICKER_SYMBOL="%s" and TRADE_DATE in ("%s","%s") order by TRADE_DATE DESC'%(table,stoc,date,last_date)
    sql = 'select TRADE_DATE,TICKER_SYMBOL,OPEN_PRICE,HIGHEST_PRICE,CLOSE_PRICE,LOWEST_PRICE,CLOSE_PRICE,CLOSE_PRICE from %s \
        where TICKER_SYMBOL = "%s" and TRADE_DATE in ("%s","%s") order by TRADE_DATE DESC'%(table,stoc,date,last_date)
    adata=sqldb.dydb_query(sql)

    adata.columns=['date','stcid','open','high','close','low','volume','amount']
    adata.set_index("date",inplace=True)
    return adata



# date为['2016-03-27'],output为['2016-03-26']
def get_last_date(date):
    calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
    del calframe['0']
    calframe.columns=['Time']
    format_str = format_date(date,"%Y/%m/%d")
    index=calframe[calframe.Time==format_str].index
    last_index=index-1
    last_date=str(calframe.loc[last_index,:]['Time'].values[0])
    last_date = format_date(last_date,"%Y-%m-%d")
    return last_date


# date为['2016-03-27'],output为['2016-03-26']
def get_lastN_date(date,n):
    calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
    del calframe['0']
    calframe.columns=['Time']
    format_str=format_date(date,"%Y/%m/%d")
    # print format_str
    index=min(calframe[calframe.Time >= format_str].index)
    last_index=index-n
    # print date
    # last_date=str(calframe.loc[last_index,:]['Time'].values[0])
    last_date=str(calframe.loc[last_index]['Time'])
    last_date=datetime.datetime.strptime(last_date,"%Y/%m/%d").strftime("%Y-%m-%d")
    return last_date

## inDate: "2018-08-08","2018/08/08","20180808",或者date()类型
def format_date(inDate,formatType):
    inDate = str(inDate)
    try:
        formatDate=datetime.datetime.strptime(inDate,"%Y%m%d").strftime(formatType)
    except:
        try:
            formatDate=datetime.datetime.strptime(inDate,"%Y/%m/%d").strftime(formatType)
        except:
            try:
                formatDate=datetime.datetime.strptime(inDate,"%Y-%m-%d").strftime(formatType)
            except:
                try:
                    formatDate=inDate.strftime(formatType)
                except:
                    formatDate="19890928"
    return formatDate

#     TRADE_DATE    TICKER_SYMBOL  rate       type
# 0   2016-04-30      601989        10.01      ZT
# 1   2016-05-01

## 在一个frame中，根据close和preclose得到涨跌停对象
def get_zdt(tarframe,close,preclose):
    tarframe['ZT']=(tarframe[preclose].astype(np.float64)*1.1).round(2)
    tarframe['DT']=(tarframe[preclose].astype(np.float64)*0.9).round(2)
    tarframe['rate']=100*(tarframe[close].astype(np.float64)-tarframe[preclose].astype(np.float64))/tarframe[preclose].astype(np.float64)
    tarframe['rate']=tarframe['rate'].round(decimals=2)
    ztframe=tarframe[tarframe.CLOSE_PRICE>=tarframe.ZT][['TRADE_DATE','TICKER_SYMBOL','rate']]
    dtframe=tarframe[tarframe.CLOSE_PRICE<=tarframe.DT][['TRADE_DATE','TICKER_SYMBOL','rate']]
    ztframe['type']=np.array(['ZT']*len(ztframe))
    dtframe['type']=np.array(['DT']*len(dtframe))
    return ztframe,dtframe

## 判断股票是否强势,如果成交量比前6日的成交量还要大，暂定为巨量
## date格式任意
## vol为当前成交量，支持盘中
def volStatus(stoc,date,vol):
    try:
        date = format_date(date,"%Y-%m-%d")
        pre_date=get_lastN_date(date,6)
        vframe=ts.get_h_data(stoc,start=pre_date,end=date)
        print "------------------%s,%s,%s-------------------------"%(stoc,pre_date,date)
        vframe.index=pd.to_datetime(vframe.index.values)
        ## remove the same date as compared
        if datetime.datetime.strptime(date,'%Y-%m-%d').date() == vframe.index[0].date():
            vframe.drop(vframe.index.values[0],inplace=True)
        if float(vol) > 1.5*float(max(vframe.volume.values)):
            return 1
        else:
            return 0
    except:
        return 0

#slist 为字符串list，可以包含sh，也可以不包含,位数可以是6位，也可以不为6位
# output
#        time        date   stcid  close  preclose   high    low  vol  amount  rate    name
# 0  11:15:12  2016-04-07  002785  27.20     25.08  27.50  25.00  115516   3.01   8.45    中国重工
# 1  11:15:12  2016-04-07  603866  33.36     32.94  33.75  32.95   34952   1.17   1.28    招商银行
def get_sina_data(slist):
    stoc=slist[0]
    if stoc[:2] in ["sh","sz"]:
        pass
    else:
        slist=[str(x) if (len(str(x))==6) else'0'*(6-len(str(x)))+str(x) for x in slist]
        slist=["sh"+str(x) if str(x)[:2] in ["60","90"] else "sz"+str(x) for x in slist]
    aframe = pd.DataFrame()
    if len(slist) > 200:
        for i in range(0,len(slist)/200+1):
            if i != len(slist)/200:
                tmpslist = slist[200*i:200*(i+1)]
            else:
                tmpslist = slist[200*i:]

            tmpframe = get_little_sina_data(tmpslist)
            aframe = pd.concat([aframe,tmpframe],axis=0)
    else:
        aframe = get_little_sina_data(slist)
    return aframe

def get_little_sina_data(slist):
    str_list=",".join(slist)
    url = "http://hq.sinajs.cn/list=%s"%str_list
    #     print url
    try:
        r=requests.get(url)
        content=r.content.decode('gbk')
        Dframe=parse_content(content)
        return Dframe
    except:
        while True:
            try:
                ip = http_downloader.get_proxyip(dynamic=False)
                proxies = {'http':'http://{}:{}'.format(ip['host'], ip['port'])}
                r=requests.get(url, proxies=proxies)
                content=r.content.decode('gbk')
                Dframe=parse_content(content)
                return Dframe
            except:
                print "Exception when get, will retry"



## 解析从新浪下载的数据
def parse_content(content,timestamp=time.strftime("%X",time.localtime())):
    Inframe=DataFrame()
    i = 0
    strarray=content.split(';')
    for item in strarray:
        item_array=item.split(',')
        if len(item_array)<10:
            continue
        stockid = item_array[0][14:20]
        stockid = item_array[0].split('=')[0].split('str_')[1][2:]
        stockname = item_array[0].split("=")[1].replace('"','')
        open = item_array[1]
        close = item_array[3]
        preclose = item_array[2]
        high = item_array[4]
        low = item_array[5]
        vol = item_array[8]   ##成交量，股
        amount = item_array[9]  ##成交额，元


        if close == '0.00':
            continue
        Inframe.loc[i,'time']=timestamp
        Inframe.loc[i,'date']=datetime.date.today()
        Inframe.loc[i,'stcid']=stockid
        Inframe.loc[i,'name']=stockname
        Inframe.loc[i,'close']=round(float(close),2)
        Inframe.loc[i,'preclose']=round(float(preclose),2)
        Inframe.loc[i,'high']=round(float(high),2)
        Inframe.loc[i,'low']=round(float(low),2)
        Inframe.loc[i,'vol']=round(float(vol),0)
        Inframe.loc[i,'amount']=round(float(amount),0)
        i+=1

    Inframe['rate']=100*(Inframe['close'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
    Inframe['rate']=Inframe['rate'].round(decimals=2)
    # Inframe['hate']=100*(Inframe['high'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
    # Inframe['hate']=Inframe['hate'].round(decimals=2)
    return Inframe

## 根据日期，更新连续涨跌停个数
def update_numZD(cdate,predate,mongodb):
    num_ucont={"2":0,"3":0,"4":0,"5":0,"6":0,"+6":0}    #连张股票个数统计
    stoc_ucont={"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"+6":[]}   #连涨股票代码
    num_dcont={"2":0,"3":0,"4":0,"5":0,"6":0,"+6":0}    #连张股票个数统计
    stoc_dcont={"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"+6":[]}   #连涨股票代码

    predate = format_date(predate,"%Y%m%d")
    tDicts=mongodb.stock.ZDT_by_date.find_one({"date":predate})
    print predate,cdate
    if tDicts.has_key('details'):
        stoc_ucont=tDicts['details']["stoc_ucont"]
        stoc_dcont=tDicts['details']["stoc_dcont"]

    if tDicts.has_key('num_ucont'):
        num_ucont=tDicts["num_ucont"]
        num_dcont=tDicts["num_dcont"]

    cdate = format_date(cdate,"%Y%m%d")
    cDicts=mongodb.stock.ZDT_by_date.find_one({"date":cdate})
    # zt_list=cDicts['ZT_stocks'].split("_")
    zt_list=cDicts['actulZtStocks'].split("_")  ##去除次新股影响
    dt_list=cDicts['DT_stocks'].split("_")

    ##去除重复的股票
    zt_num = len(set(zt_list))
    zt_list_str="_".join(set(zt_list))
    dt_num = len(set(dt_list))
    dt_list_str="_".join(set(dt_list))

    if zt_num == 1:
        if zt_list[0] == u'':
            zt_num = 0

    if dt_num == 1:
        if dt_list[0] == u'':
            dt_num = 0

    zt_list = list(set(zt_list))
    dt_list = list(set(dt_list))
    if dt_list == [""]:
        dt_list=[]
    (stoc_ucont,num_ucont,stoc_dcont,stoc_dcont)=cont_stat(zt_list,dt_list,num_ucont,stoc_ucont,num_dcont,stoc_dcont)
    dicts={
    "details":{
        "stoc_ucont":stoc_ucont,
        "stoc_dcont":stoc_dcont},
    "num_ucont":num_ucont,
    "num_dcont":num_dcont
    }
    write_mongo(mongodb,cdate,dicts,zt_num,zt_list_str,dt_num,dt_list_str)


## 用来计算连续涨跌停个数
def cont_stat(zt_list,dt_list,num_ucont,stoc_ucont,num_dcont,stoc_dcont):
        ## 涨停
        pre_up6=stoc_ucont['+6']+stoc_ucont['6']
        stoc_ucont['+6']=[x for x in zt_list if x in pre_up6]
        num_ucont['+6']=len(stoc_ucont['+6'])
        for num in [6,5,4,3,2]:
            last_num="%s"%(num-1)
            num = "%s"%num
            pre_up=stoc_ucont[last_num]
            stoc_ucont[num]=[x for x in zt_list if x in pre_up]
            num_ucont[num]=len(stoc_ucont[num])
        stoc_ucont["1"]=zt_list

        ##跌停
        pre_dn6=stoc_dcont['+6']+stoc_dcont['6']
        stoc_dcont['+6']=[x for x in dt_list if x in pre_dn6]
        num_dcont['+6']=len(stoc_dcont['+6'])
        for num in [6,5,4,3,2]:
            last_num="%s"%(num-1)
            num = "%s"%num
            pre_dn=stoc_dcont[last_num]
            stoc_dcont[num]=[x for x in dt_list if x in pre_dn]
            num_dcont[num]=len(stoc_dcont[num])
        stoc_dcont["1"]=dt_list
        return (stoc_ucont,num_ucont,stoc_dcont,stoc_dcont)

def write_mongo(mongo,date,dicts,ztNum,ztStr,dtNum,dtStr):
    mongo_obj=mongo.report
    try:
        format_date=datetime.datetime.strptime(date,"%Y-%m-%d").strftime("%Y%m%d")
    except:
        try:
            format_date=datetime.datetime.strptime(date,"%Y/%m/%d").strftime("%Y%m%d")
        except:
            format_date=datetime.datetime.strptime(date,"%Y%m%d").strftime("%Y%m%d")
    obj_mongo=mongo.stock.ZDT_by_date.find_one({"date":format_date})
    if obj_mongo:

        mongo.stock.ZDT_by_date.update_one({"date":format_date},
                                   {"$set":{
                                       "details":dicts['details'],
                                       "num_ucont":dicts["num_ucont"],
                                       "num_dcont":dicts["num_dcont"],
                                       "ZT_num":ztNum,
                                       "DT_num":dtNum,
                                       # "ZT_stocks":ztStr,
                                       # "DT_stocks":dtStr
                                    }
                                   })
        print "successfully update,%s"%format_date

## 根据日期类型，查询mongo中的类型
def get_mongoDicts(dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
    mongoUrl = "localhost"
    mongodb = pymongo.MongoClient(mongoUrl)
    if dateEq != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":dateEq})
    elif dateEnd != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":{"$gte":dateStart,"$lte":dateEnd}})
    elif dateStart != "19890928":
        dateDicts=mongodb.stock.ZDT_by_date.find({"date":{"$gte":dateStart}})
    else:
        dateDicts = None

    return dateDicts


##画图
# 通过x指定x轴
# 通过y=[[],[]]指定每一个子subplot中的列
# point代表多少个刻度
# marker代表是否用o标注数据
def plotFrame(dataFrame,x='',y=[],titles=[],point=100, marker=False):

    colors = ['r','b','y','g','m','c','y','k']
    fz = 8

    listLabels = ''
    if x != "":
        listLabels = dataFrame[x].values
    step = len(dataFrame)/point
    baseline = range(len(dataFrame)/step+1)
    baseline = [int(x) * step for x in baseline]
    print baseline
    fig = plt.figure(figsize=(12,8))
    count = 221

    for groupNum in range(len(y)):
        groups = y[groupNum]
        ax = fig.add_subplot(count)
        if len(titles) >0:
            ax.set_title("%s"%titles[groupNum])

        lines=[]
        des=[]

        group = groups[0]
        line1,=ax.plot(dataFrame.index,dataFrame[group].values,'r-',label=group)
        if marker:
            line12,=ax.plot(dataFrame.index,dataFrame[group].values,'ro',label=group)
        ax.grid(True)
        ax.set_xticks(baseline)
        ax.set_ylim([min(dataFrame[group].values),max(dataFrame[group].values)])
        if len(listLabels) > 1:
            ax.set_xticklabels(listLabels[baseline],fontsize=fz)
        ax.set_xlim([min(dataFrame.index.values),max(dataFrame.index.values)])
        lines.append(line1)
        des.append(group)

        if len(groups)  == 2:
            group = groups[1]
            ax2=ax.twinx()
            line2,=ax2.plot(dataFrame.index,dataFrame[group].values,'b-',label=group)
            if marker:
                line22,=ax2.plot(dataFrame.index,dataFrame[group].values,'bo',label=group)
            ax2.grid(True)
            ax2.set_xticks(baseline)
            ax2.set_ylim([min(dataFrame[group].values),max(dataFrame[group].values)])
            lines.append(line2)
            des.append(group)

        ax.legend(lines,des)
        # ax1.legend([line11,line12,line13],('up10%','dn10%','sh_change'),'upper left',ncol=3)
        # plt.show()
        count += 1

    plt.show()

#查找数据库中的股票,name可以是股票代码，也可以是股票名字
# 返回是['中国重工', 601989]
def QueryStockMap(id='',name=''):
    global mongodb
    try:
        if id != '':
            id = regulize_stockid(id)
            queryResult = mongodb.stock.stockmap.find({"stockid":id}).sort("updatetime",pymongo.DESCENDING)[0]
            if queryResult is not None:
                return [queryResult['stock_name'],queryResult['stockid']]
            else:
                print "[WARNING], QueryStockMap for id:%s, will return 0" % id
                return [0,0]
        elif name != '':
            queryResult = mongodb.stock.stockmap.find({"stock_name":name}).sort("updatetime",pymongo.DESCENDING)[0]
            if queryResult is not None:
                return [queryResult['stock_name'],queryResult['stockid']]
            else:
                print "[WARNING], QueryStockMap for name:%s, will return 0" % name
                return [0,0]
        else:
            print "[WARNING], QueryStockMap return 0 for unexpected reason, id:%s, name:%s" % (id, name)
            return [0,0]
    except:
        print "[WARNING], QueryStockMap return 0 for unexpected reasons, id:%s, name:%s" % (id, name)
        return [0,0]


def regulize_stockid(id):
    if id != id:
        return ""
    elif len(str(id).replace(u' ', u'')) == 0:
        return ""
    else:
        return '0'*(6-len(str(int(float(id))))) + str(int(float(id)))


# 根据条件，对stockList中的股票进行弹窗提示，返回符合条件的List,股票代码
def WindowShow(stockList, operate, number, message):
    result = set()
    curframe = get_sina_data(stockList)
    number = float(number)
    if "g" in operate:
        tarframe = curframe[curframe.rate >= number]
    elif "l" in operate:
        tarframe = curframe[curframe.rate <= number]

    if len(tarframe) < 1:
        return result

    telllist = tarframe.stcid.values
    tellname = tarframe.name.values
    tellstr = "_".join(telllist)
    tellnamestr = "_".join(tellname)
    # showinfo(message, "---- %s ------\n%s\n%s" % (message,tellstr, tellnamestr))
    # 太多了，暂时不处理，需要更新系统
    if message != u'接近新高股8个点以上提示':
        showinfos("---- %s ------\n\n%s\n\n%s" % (message,tellstr, tellnamestr))
    return telllist

def showinfos(message):
    root = tk.Tk()
    # root.withdraw()
    root.title("Say Hello")
    label = tk.Label(root, text=message)
    label.pack(side="top", fill="both", expand=True, padx=20, pady=20)
    button = tk.Button(root, text="OK", command=lambda: root.destroy())
    button.pack(side="bottom", fill="none", expand=True)
    root.mainloop()
    print "will return"


def get_realtime_news(stock):
    count = 3
    news = ""
    if stock[:2] == '60':
        refer = "http://www.yuncaijing.com/quote/sh%s.html"%stock
    else:
        refer = "http://www.yuncaijing.com/quote/sz%s.html"%stock
    headers = {
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding":"gzip, deflate",
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Host":"www.yuncaijing.com",
        "Origin":"http://www.yuncaijing.com",
        "Referer":"http://www.yuncaijing.com/quote/sz002436.html",
        "X-Requested-With":"XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0"
    }
    datas = {
        "code":stock
    }
    # datas = "code=002436"
    url = "http://www.yuncaijing.com/stock/get_lines/yapi/ajax.html"
    proxies = {'http':'http://10.20.205.162:1080'}
    r = requests.post(url = url, headers = headers, data=datas, proxies=proxies)
    # r = requests.post(url = url, headers=headers, data=datas)
    while r.status_code != 200 or count < 0:
        r = requests.post(url = url, headers = headers, data=datas)
        time.sleep(5)
        count -= 1
    if r.status_code != 200:
        return news
    contents = json.loads(r.content)
    jsonContent = contents['data']['lEvtNews']
    news = parse_news(jsonContent)
    return news

def get_hist_news(stock):
    count = 10
    news = ""
    headers = {
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding":"gzip, deflate",
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Host":"www.yuncaijing.com",
        "Origin":"http://www.yuncaijing.com",
        "Referer":"http://www.yuncaijing.com",
        "X-Requested-With":"XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0"
    }
    datas = {
        "code":stock
    }
    url = "http://www.yuncaijing.com/stock/get_klines/yapi/ajax.html"
    proxies = {'http':'http://10.20.205.162:1080'}
    r = requests.post(url = url, headers = headers, data=datas, proxies=proxies)
    # r = requests.post(url = url, headers = headers, data=datas)
    while r.status_code != 200 or count < 0:
        r = requests.post(url = url, headers = headers, data=datas)
        time.sleep(5)
        count -= 1
    contents = json.loads(r.content)
    jsonContent = contents['data']['kEvtNews']
    news = parse_news(jsonContent)
    return news

def parse_news(jsonContent):
    news = ""
    if jsonContent is None:
        return news
    count = 5
    try:
        keys = jsonContent.keys()
        keys.sort()
        for key in keys:
            if count < 1:
                break
            # news = news + "%s %s, %s\n" % (jsonContent[key]['date'], jsonContent[key]['time'], jsonContent[key]['title'])
            news = news + "%s, %s\n" % (jsonContent[key]['day'], jsonContent[key]['title'])
            count -= 1
    except:
        for dictContent in jsonContent:
            if count < 1:
                break
            news += "%s, %s\n" % (dictContent['day'].replace("<kbd>","").replace("<\\/kbd>",""), dictContent['title'])
            count -= 1
    return news


# 得到金融界上面的复盘信息，比云财经的要精确
def get_jrj_news():
    bs = webdriver.Ie()
    bs.get("http://stock.jrj.com.cn/ztbjm/ztbjm.shtml")
    time.sleep(5)
    news_dict = {}
    r = etree.HTML(bs.page_source)
    bs.close()
    zt_datas = r.xpath('//div[@id="zt_data"]//div[@class="detailBox"]')
    for zt_data in zt_datas:
        try:
            reason = zt_data.xpath('./p[2]/text()')[0]
            stockid = zt_data.xpath('./div[2]/div[2]/div/table/tbody/tr[3]/@name')[0]
            print stockid
            if len(stockid.replace(" ", "")) < 3:
                continue
            concept = zt_data.xpath('./div[2]/div[2]/div/table/tbody/tr[1]/th/span/@name')[0]
            stockname = zt_data.xpath('./div[2]/div[2]/div/table/tbody/tr[3]/td[1]/a/text()')[0]
            concept = concept.replace(stockname, u'')
            news_dict[stockid] = [concept, reason]
        except:
            pass
    return news_dict


# 得到选股宝上面的复盘信息
def get_xgb_news():
    bs = webdriver.Ie()
    news_dict = {}

    for url in ['http://ban.xuangubao.cn/#/pool/1', 'http://ban.xuangubao.cn/#/pool/2']:
        bs.get(url)
        time.sleep(5)
        r = etree.HTML(bs.page_source)


        # 解析数据
        zt_datas = r.xpath('//div[@class="body___33tuy"]/div')
        for zt_data in zt_datas:
            try:
                reason = zt_data.xpath('./div[3]/div/p/text()')[0]
                stockid = zt_data.xpath('./div[2]/div/p[2]/text()')[0].replace(".SZ","").replace(".SS","")
                news_dict[stockid] = [reason, reason]
            except:
                pass
    bs.close()
    return news_dict







def get_latest_news(stock):
    stock = '0'*(6-len(stock))+stock
    count = 10
    print "get news for %s" % stock
    real_content = ''
    hist_content = ''
    # tmp
    # return ""
    try:
        # 首先看实时新闻
        real_content = get_realtime_news(stock)
    except:
        real_content = 'no valid news\n'

    try:
        # 再看历史新闻
        hist_content = get_hist_news(stock)
    except Exception,err:
        hist_content = 'no valid news\n'

    news = real_content + hist_content
    return news

'''
time stockid close preclose high rate
原始的frame，rate为-100是存在的
'''
def get_price_from_redis(stock_lists, rediser):
        ttframe = DataFrame()
        timestamp=time.strftime("%X", time.localtime())
        count = 0
        # print timestamp
        redis_stocks = rediser.keys()
        for stockid in stock_lists:
            # print stockid
            # if str(stockid) == '601200':
            #     continue
            if stockid != stockid:
                continue
            if stockid not in redis_stocks:
                continue
            value_str = rediser.get(stockid)
            if 'nan' in value_str:
                # print stockid
                continue
            redis_value = eval(value_str)
            if len(redis_value) != 5:
                continue
            [close, preclose, high, low, rate] = redis_value
            # if int(rate) < -15:
            #     rate = 0
            ttframe.loc[count, 'time'] = timestamp
            ttframe.loc[count, 'stockid'] = stockid
            ttframe.loc[count, 'close'] = close
            ttframe.loc[count, 'preclose'] = preclose
            ttframe.loc[count, 'high'] = high
            ttframe.loc[count, 'low'] = low
            ttframe.loc[count, 'rate'] = rate
            count += 1
        return ttframe

## 添加均线,利用CLOSE_PRICE,得到mean5,mean10,mean20...
def add_mean(dframe):
    for ma in [5,10,20,60,120]:
        dframe['mean%s'%ma]=dframe['CLOSE_PRICE'].rolling(window=ma).mean()

# 得到股票代码在某些天内的日线数据
# 格式匹配交割单图
# 1代表个股，0代表指数
def get_daily_frame(code, start_date, end_date, id_type = 1):
    if id_type == 1:
        code = "0"*(6-len(str(int(code))))+str(int(code))
        sql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, \
        DEAL_AMOUNT from vmkt_equd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,end_date,code)
        sub = get_mysqlData_sqlquery(sql)
    elif id_type == 0:
        idxcode = "000001"
        idxsql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_INDEX, OPEN_INDEX, HIGHEST_INDEX, LOWEST_INDEX, CLOSE_INDEX, \
        TURNOVER_VOL from vmkt_idxd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,end_date,idxcode)
        sub = get_mysqlData_sqlquery(idxsql)
        sub.columns=[["TICKER_SYMBOL", "SEC_SHORT_NAME", "TRADE_DATE", "PRE_CLOSE_PRICE", "OPEN_PRICE", "HIGHEST_PRICE", "LOWEST_PRICE", "CLOSE_PRICE", "DEAL_AMOUNT"]]
    add_mean(sub)
    return sub


def get_minly_tushare_frame(stockid, endDate, id_type =1):
    if id_type == 1:
        stockid = "0"*(6-len(str(stockid))) + str(stockid)
        endDate = format_date(endDate, "%Y-%m-%d")
        dframe = ts.get_tick_data(code=stockid,date = endDate)
        dframe['min'] = dframe['time'].apply(lambda x: x.split(":")[1])
        dframe['hour'] = dframe['time'].apply(lambda x: x.split(":")[0])
        dframe['sec'] = dframe['time'].apply(lambda x: x.split(":")[2])
        dframe['bartime'] = dframe['hour'] + ":" + dframe['min']

        # closeprice
        closeprice = dframe[(dframe.sec == '00') | (dframe.hour == '15')][['price','bartime']]
        closeprice.set_index('bartime', inplace=True)

        # volume, 成交量，股
        volume = dframe['volume'].groupby([dframe['min'], dframe['hour']]).sum().reset_index()
        volume['bartime'] = volume['hour'] + ":" + volume['min']
        volume.set_index('bartime', inplace=True)
        volume['volume'] = 100 * volume['volume']
        volumes = volume['volume']
        zframe = pd.concat([closeprice, volumes], axis=1)
        zframe.sort_index()
        zframe.fillna(method = 'pad', inplace=True)
        zframe.fillna(method = 'bfill', inplace=True)

        len_frame = len(zframe)
        # datadate
        today = format_date(endDate, "%Y%m%d")
        datadate = np.array([today] * len_frame)

        # ticker
        ticker = np.array([stockid] * len_frame)

        # secoffset
        secoffset = np.array([0] * len_frame)
        # exchangecd
        exchangecd = np.array(['CHINA'] * len_frame)

        openprice = np.array(zframe['price'])
        highprice = openprice
        lowprice = openprice
        value = volumes
        vwap = openprice
        shortnm = np.array(['CHINA'] * len_frame)

        zframe['datadate'] = datadate
        zframe['ticker'] = ticker
        zframe['secoffset'] = secoffset
        zframe['openprice'] = openprice
        zframe['highprice'] = highprice
        zframe['lowprice'] = lowprice
        zframe['value'] = value
        zframe['vwap'] = vwap
        zframe['exchangecd'] = exchangecd
        zframe['shortnm'] = shortnm
        zframe.reset_index(len(zframe), inplace=True)
        zframe.columns = ['bartime', 'closeprice', 'volume', 'datadate', 'ticker', 'secoffset', 'openprice', 'highprice','lowprice','value', 'vwap', 'exchangecd', 'shortnm']
        return zframe




# 得到股票代码在某天内的分钟级数据
# 格式匹配交割单图
# 1代表个股，0代表指数
def get_minly_frame(stockid, endDate, id_type =1):
    try:
        tableTime = format_date(endDate,"%Y%m")
        endDate = format_date(endDate,"%Y%m%d")
        if id_type == 1:
            stockid = "0"*(6-len(str(int(stockid))))+str(int(stockid))
            # table = "equity_pricefenbi%s"%tableTime
            table = "MarketDataTDB.equity_pricemin%s"%tableTime
            dtsql = "SELECT * from %s where ticker = %s and datadate = %s"%(table,stockid,endDate)
            # print dtsql
            dtv = get_mydb_sqlquery(dtsql)
            if len(dtv) == 0:
                table = "MarketDataL1.equity_pricemin%s"%tableTime
                dtsql = "SELECT * from %s where ticker = %s and datadate = %s"%(table,stockid,endDate)
                dtv = get_mydb_sqlquery(dtsql)
        else:
            table = "MarketDataTDB.equity_pricemin%s"%tableTime
            zssql = 'SELECT * from %s where datadate = %s and ticker = 1 and shortnm = "上证指数"'%(table,endDate)
            dtv = get_mydb_sqlquery(zssql)
            if len(dtv) == 0:
                table = "MarketDataTDB.equity_pricemin%s"%tableTime
                zssql = 'SELECT * from %s where datadate = %s and ticker = 1 and shortnm = "上证综指"'%(table,endDate)
                dtv = get_mydb_sqlquery(zssql)
        return dtv
    except:
        dtv = get_minly_tushare_frame(stockid, endDate, id_type)
        return dtv

def plot_text(ax, texts, fontsize = 20):
    total_text = []

    for text in texts:
        text = text.replace(r"</kbd>", "")
        if len(text) > 28 and len(text) < 56:
            total_text.append(text[:27])
            total_text.append(text[27:])
        elif len(text) > 56:
            total_text.append(text[:27])
            total_text.append(text[28:56])
            total_text.append(text[56:])
        else:
            total_text.append(text)
    texts = total_text
    texts.reverse()
    n = len(texts)
    n = n + 2
    tmp_texts = [' ']
    tmp_texts.extend(texts)
    texts = tmp_texts
    texts.append(" ")
    t_list = []
    for i in range(n):
        s_list = pd.Series([i+1]*6)
        t_list.append(s_list)
    fframe = pd.concat(t_list, axis=1)

    i = 0
    for col in fframe.columns:
        ax.plot(fframe.index.values, fframe[col], 'w')
        ax.annotate(texts[i], xy=(0, i+1), fontproperties=myfont, fontsize = fontsize)
        i += 1
    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['top'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.axis('off')


# 给出一个stockid，日期
# 找到对应的concept，以及相关股票id
# 先从前10日的csv中找，如果没找到，则从L1中找，如果没找到，再到L2中找，再没找到，就为空
# 从前10日的csv找到概念之后，先去L1找关联股票，找不到时候再去L2
# 返回['concept', [关联股票ids]]
def find_concept(stockid, date):
    concept = ""
    ids = []
    stockid = str(int(float(stockid)))
    stockid = '0'*(6-len(stockid)) + stockid
    csv_concept = get_csv_concept(stockid, date)
    if csv_concept == '':       # 在前10日的daydayup中找不到
        L1_concept = get_cacheinfos_by_id(stockid, cache=1)
        if L1_concept[0] == "":     # 在L1中也找不到
            L2_concept = get_cacheinfos_by_id(stockid, cache=2)
            concept = L2_concept[0]
            ids = L2_concept[1]
        else:
            concept = L1_concept[0]
            ids = L1_concept[1]
    else:
        concept = csv_concept
        ids = find_ids(concept)
    return [concept, ids]


# 找到对应的股票
def find_ids(concept):
    ids = get_id_from_concept(concept, cache=1)
    if len(ids) == 0:
        ids = get_id_from_concept(concept, cache=2)
    return ids



# 根据日期，从csv中找到股票的concept，默认是从昨天到5天之前
# 返回是'concept'，如果找不到，则是''
def get_csv_concept(stockid, date, period=10):
    global backsee_csv
    concept = ''
    for i in range(1,period+1):
        search_date = get_lastN_date(date, i)   # 搜索日期
        search_date = format_date(search_date, "%Y%m%d")
        try:
            dframe = pd.read_csv(os.path.join(backsee_csv, "%s/daydayup.csv" % search_date), encoding='gbk')
            dframe.dropna(subset=['stock'], inplace=True)
            dframe['stockid'] = dframe['stock'].apply(lambda x: int(float(x)))      # 都转换成整数型
            dframe.dropna(subset=['group'], inplace=True)   # 将不含group的行删除
            tframe = dframe[dframe.stock == int(float(stockid))]
            columns = list(tframe.columns)
            n_group = columns.index(u'group')
            if len(tframe) > 0:
                concept = tframe.iloc[0, n_group]
                break
        except Exception, err:
            print "Search Error for daydayup on %s, err:%s, will skip this day" % (search_date, err)
    return concept

# 从数据库cache中找到股票对于的concept以及关联stocks
# 返回['概念名称', '相关概念的所有股票列表']
# 如果没找到，则返回['', []]
def get_cacheinfos_by_id(stockid, cache=1):
    global mongodb
    if cache == 1:
        result = mongodb.concepts.L1.find_one({"stockid": stockid})
    else:
        result = mongodb.concepts.L2.find_one({"stockid": stockid})

    if result is None:  # 说明在cache中，没有该股票
        return ["", []]
    else:
        return result['concept'], get_id_from_concept(result['concept'], cache)


# 从数据库cache中，根据concept得到所有的股票ID，返回list，如果L1中不存在该概念，则返回[]
def get_id_from_concept(concept, cache=1):
    global mongodb
    ids = []

    if cache == 1:
        results = mongodb.concepts.L1.find({"concept": concept})
    else:
        results = mongodb.concepts.L2.find({"concept": concept})

    if results.count() == 0:
        return ids
    else:
        for result in results:
            ids.append(result['stockid'])
        return ids


# 判断股票、概念组合是否在cache中
def exist_in_cache(stockid, concept, cache = 1):
    global mongodb
    if concept == "":
        return 0
    else:
        stockid = '0'*(6-len(str(int(float(stockid))))) + str(int(float(stockid)))
        if cache == 1:
            result = mongodb.concepts.L1.find_one({"stockid": stockid, "concept":concept})
        else:
            result = mongodb.concepts.L2.find_one({"stockid": stockid, "concept":concept})
        if result is None:
            return 0
        else:
            return 1


# 抽取股金神话中的复盘数据
def extract_gjsh_records(contents, tdate):
    contents = etree.HTML(contents)
    lines = contents.xpath('//div[@class="detail"]/p/text()')
    match_str = r'\d{6}'
    extract_infos = {"source":"GJSH", "date":tdate}
    for eline in lines:
        # print eline
        if re.search(match_str, eline):
            # print "hit"
            eline = eline.replace("\t", " ")
            line_infos = eline.split(" ")
            line_infos = [x for x in line_infos if len(x)>0]
            stockid = line_infos[0]
            stockname = line_infos[1]
            for idx in range(2, len(line_infos)):
                if u':' in line_infos[idx]:
                    # print 'hit2'
                    zt_time = line_infos[idx]
                    zt_reason = "_".join(line_infos[idx+1:])
                    extract_infos[stockid] = {"name":stockname, "reason":zt_reason, "zttime":zt_time, "type":"ZT"}
                    break
    print "Get ZT INFO from GJSH: %s in total" %(len(extract_infos) - 1)
    return extract_infos


# 抽取股金神话中的复盘数据
# 增加了大概念，即 xxxx概念：
def extract_gjsh_records_big_concept(contents, tdate):
    contents = etree.HTML(contents)
    lines = contents.xpath('//div[@class="detail"]/p/text()')
    if len(lines) == 0:
        lines = contents.xpath('//div[@class="detail"]/text()')
    match_str = r'\d{6}'
    extract_infos = {"source":"GJSH_B", "date":tdate}
    big_concept = ""
    last_line = ""
    last_valid_status = False
    first_type_concept = False
    for eline in lines:
        eline = eline.strip()
        eline = eline.replace(".SH", "").replace(".SZ", "").replace(".sh", "").replace(".sz", "")
        # 第一种含有big_concept的类型，即包含：
        if u'：' in eline:
            line_infos = eline.split(u'：')
            if line_infos[1].strip() == u'':
                big_concept = line_infos[0]
                first_type_concept = True # 标志着都是通过“：”来显示big concept
                # print big_concept

        try:
            if re.search(match_str, eline):
                eline = eline.replace("\t", " ").replace(u"\xa0", u' ')
                line_infos = eline.split(" ")
                line_infos = [x for x in line_infos if len(x)>0]
                stockid = line_infos[0]
                stockname = line_infos[1]
                for idx in range(2, len(line_infos)):
                    if u':' in line_infos[idx]:

                        # 第二种含有big_concept的类型，即该行符合要求，如果上一行不符合要求，则上一行可能是（再排除掉点评之类的）
                        # 条件一： 上一行不是300043 xxx 之类的
                        # 条件二： 上一行不为空
                        # 条件三： 不存在类型1的big concept
                        if not last_valid_status and len(last_line) > 1 and not first_type_concept and u'点评' not in last_line:
                            big_concept = last_line.strip()
                        zt_time = line_infos[idx]
                        zt_reason = "_".join(line_infos[idx+1:])
                        extract_infos[stockid] = {"name":stockname, "reason":zt_reason, "zttime":zt_time, "type":"ZT", "big_concept":big_concept}
                        last_valid_status = True
                        if len(eline) > 0:
                            last_line = eline
                        break
            else:
                last_valid_status = False
                if len(eline.strip()) > 0:
                    last_line = eline
        except:
            pass
    print "Get ZT INFO from GJSH: %s in total" %(len(extract_infos) - 1)
    return extract_infos


# 爬取股金神话的复盘信息
# 存储到FP_INFO_DAILY
# {"600036":{'name':名字, 'reason':原因, 'time':涨停时间},"":{}, xxxxxx,  "source":"GJSH"}
def resee_info_gjsh(tdate=datetime.datetime.today().strftime("%Y%m%d")):
    tdate = format_date(tdate, "%Y%m%d")
    tyear = tdate[:4]
    tmonth = str(int(tdate[4:6]))
    tday = str(int(tdate[6:]))

    general_headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate, sdch",
        "Accept-Language":"zh-CN,zh;q=0.8",
        "Connection":"keep-alive",
        "Host":"www.xueqiu.com",
        "Upgrade-Insecure-Requests":"1",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }

    general_headers1 = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate, br",
        "Accept-Language":"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection":"keep-alive",
        "Host":"xueqiu.com",
        "Upgrade-Insecure-Requests":"1",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
    }

    main_page = "https://www.xueqiu.com"
    s = requests.Session()
    s.get(main_page, headers=general_headers)


    # 查看股金神话的页面
    page1 = "https://xueqiu.com/statuses/search.json?q=%E6%B6%A8%E5%81%9C%E6%9D%BF%E5%A4%8D%E7%9B%98&page=1&uid=4172966218&sort=time"

    r = s.get(page1, headers=general_headers1)
    if r.status_code != 200:
        print "Error: 访问股金神话主页失败!"

    # content = r.content.decode("utf-8")
    content = json.loads(r.content)
    user_id = 0
    file_id = 0
    for infos in content['list']:
        text = infos['title']
        if tyear in text and tmonth in text and tday in text:
            # print text
            user_id = infos['user_id']
            file_id = infos['id']
            break
    next_url = "https://xueqiu.com/%s/%s" % (user_id, file_id)

    global mongodb
    status = False
    # 开始获取信息
    t_count = 5
    while t_count > 0:
        try:
            r = s.get(next_url, headers = general_headers1)
            if r.status_code != 200:
                raise Exception("访问股金神话具体页面失败")
            record_infos = extract_gjsh_records_big_concept(r.content.decode("utf-8"), tdate)
            # 更新到mongo中
            mongodb.stock.mirror_info.update({"source":"GJSH_B", "date":tdate}, {"$set":record_infos}, upsert=True)
            status = True
            break
        except:
            t_count -= 1
            time.sleep(3)
    return status
