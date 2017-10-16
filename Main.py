# coding:utf8
__author__ = 'li.li'
import common
import time
import datetime
import pymongo
import numpy as np
import pandas as pd
import os

# interface #
##################################################################################################################
## 更新时间区间内的涨跌停股票、涨跌停股票数
# def update_ZDT_stocksNum_ALL(self, dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):

# step 1
# 更新指定日的涨停股票，跌停股票，涨停数，跌停数
# def update_ZDT_stocksNum(self,stock_list,date_list):

# step 1
## 在指定日期区间内，找到一字涨停的新股，更新到mongo中的freshStocks, openedFreshStocks, actulZtStocks
# def update_freshStocks(self,dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):

# step 2
# 给定日期区间内，计算连续涨跌停个数，并去重zt_stocks, dt_stocks, 重新计算个数
# def update_ZDT_contsNum(self, dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):

# step 2
# 从指定日期开始，计算meat,hole, 以及pre的情况
# def update_ZDT_yesterday(self,dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
####################################################################################################################


class MainProc:

    def __init__(self):
        pass

    ## 更新时间区间内的涨跌停股票、涨跌停股票数
    def update_ZDT_stocksNum_ALL(self, dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
        dateDicts = common.get_mongoDicts(dateEq=dateEq,dateStart=dateStart,dateEnd=dateEnd)
        if dateDicts == 1:
            print "Error, can't find the date"
            return 1
        else:
            for dateDict in dateDicts:
                cdate=[dateDict['date']]
                self.update_ZDT_stocksNum([],cdate)

    def update_ZDT_stocksNum(self,stock_list,date_list):
        # step 1
        # 更新指定日的涨停股票，跌停股票，涨停数，跌停数
        # stock_list = []
        # date_list = ['20160428']
        # print "calculating ZDT stocks and numbers for %s"%date_list
        # print "%s,%s"%(stock_list,date_list)
        fullFrame = common.get_mysqlData(stock_list,date_list)
        #      TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE    HIGHEST_PRICE   LOWEST_PRICE  CLOSE_PRICE
        # 0           000033          *ST新都  2016-04-27            10.38        0.00           0.00          0.00        10.38
        # 1           600710          *ST常林  2016-04-27             9.36        0.00         10.61         10.52        10.57

        fullFrame['ztprice']=fullFrame['PRE_CLOSE_PRICE'] * 1.1
        fullFrame['dtprice']=fullFrame['PRE_CLOSE_PRICE'] * 0.9
        fullFrame['ztprice'] = fullFrame['ztprice'].round(decimals=2)
        fullFrame['dtprice'] = fullFrame['dtprice'].round(decimals=2)

        mongo_url="localhost"
        mongodb=pymongo.MongoClient(mongo_url)
        for date in np.unique(fullFrame['TRADE_DATE']):
            str_date = datetime.datetime.strptime(str(date),"%Y-%m-%d").strftime("%Y%m%d")
            print "update ZDT stocks and number for %s"%str_date
            dayFrame = fullFrame[fullFrame.TRADE_DATE==date]
            ztList=dayFrame[dayFrame.CLOSE_PRICE>=dayFrame.ztprice]['TICKER_SYMBOL'].values
            ztList = list(set(ztList))
            ztList=["0"*(6-len(str(x)))+str(x) for x in ztList]
            ztnum = len(ztList)
            ztList="_".join(ztList)

            dtList=dayFrame[dayFrame.CLOSE_PRICE<=dayFrame.dtprice]['TICKER_SYMBOL'].values
            dtList=["0"*(6-len(str(x)))+str(x) for x in dtList]
            dtList = list(set(dtList))
            dtnum = len(dtList)
            dtList="_".join(dtList)

            hiList=dayFrame[(dayFrame.HIGHEST_PRICE>=dayFrame.ztprice) & (dayFrame.CLOSE_PRICE<dayFrame.ztprice)]['TICKER_SYMBOL'].values
            hiList=["0"*(6-len(str(x)))+str(x) for x in hiList]
            hiList = list(set(hiList))
            hiList="_".join(hiList)

            lowList=dayFrame[(dayFrame.LOWEST_PRICE==dayFrame.dtprice) & (dayFrame.CLOSE_PRICE>dayFrame.dtprice)]['TICKER_SYMBOL'].values
            lowList=["0"*(6-len(str(x)))+str(x) for x in lowList]
            lowList = list(set(lowList))
            lowList="_".join(lowList)

            # print dayFrame[(dayFrame.LOWEST_PRICE==dayFrame.dtprice) & (dayFrame.CLOSE_PRICE>dayFrame.dtprice)]

            dicts={
                "ZT_stocks":ztList,
                "DT_stocks":dtList,
                "HD_stocks":hiList,
                "LD_stocks":lowList,
                "ZT_num":ztnum,
                "DT_num":dtnum,
                "date":str_date
            }
            mongodb.stock.ZDT_by_date.update({"date":dicts['date']},{"$set":dicts},True)


    ## 在指定日期区间内，找到一字涨停的新股，更新到mongo中的freshStocks
    def update_freshStocks(self,dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
        # step 2
        # 给定日期区间内，计算连续涨跌停个数，并去重zt_stocks, dt_stocks, 重新计算个数
        # 需要依赖数据库中已经有的涨跌停股票代码以及连续涨跌停数（如果没有连续值，则从0开始）
        mongoUrl = "localhost"
        mongodb = pymongo.MongoClient(mongoUrl)
        try:
            dateDicts = common.get_mongoDicts(dateEq=dateEq,dateStart=dateStart,dateEnd=dateEnd)

            for dateDict in dateDicts:
                cdate=dateDict['date']
                predate=common.get_last_date(cdate)
                self.oneDate_freshStocks(cdate,predate,mongodb)
        except Exception,e:
            print "Exception error: %s"%e

    def oneDate_freshStocks(self,cdate,predate,mongodb):
        predate = common.format_date(predate,"%Y%m%d")
        cdate = common.format_date(cdate,"%Y%m%d")
        yesResult = mongodb.stock.ZDT_by_date.find_one({"date":predate})
        yesFreshStocks = []

        ## 今日的次新一字板股，应该是昨天的freshStock(连续过来的新股）以及昨天Add_newStocks中的股票
        if yesResult is not None and yesResult.has_key("freshStocks"):
            yesFreshStocks.extend(yesResult['freshStocks'].split("_"))
        if yesResult is not None and yesResult.has_key("Add_newStocks"):
            yesFreshStocks.extend(yesResult['Add_newStocks'].keys())


        ##再把今天上市的新股加进来
        todayNewadds = []
        todayResult = mongodb.stock.ZDT_by_date.find_one({"date":cdate})
        if todayResult.has_key("Add_newStocks"):
            yesFreshStocks.extend(todayResult['Add_newStocks'].keys())
            todayNewadds.extend(todayResult['Add_newStocks'].keys())

        ##去重
        yesFreshStocks=list(set(yesFreshStocks))

        ##和涨停股票进行比较
        todayZTs = todayResult['ZT_stocks'].split("_")      ##今日涨停股票
        freshStocks = [x for x in yesFreshStocks if x in todayZTs]      ##今日的次新一字涨停股
        # 对于不在todayZTs中的新股，如果是临时停牌或者其它原因，不应该去除
        recheckStocks = [x for x in yesFreshStocks if x not in todayZTs]
        for stocks in recheckStocks:
            recheckFrame = common.get_daily_frame(stocks, cdate, cdate)
            if recheckFrame.loc[0, 'HIGHEST_PRICE'] == 0:
                print "will add %s to freshlist because abnormal situation" % stocks
                freshStocks.append(stocks)
        freshStocks.extend(todayNewadds)
        openedFreshStocks = [x for x in yesFreshStocks if x not in todayZTs and x not in todayNewadds]    ##次新股中今天的开板股票
        actulZtStocks = [x for x in todayZTs if x not in yesFreshStocks]    ##去除次新股的今日自然涨停股票

        freshStocks = list(set(freshStocks))
        freshStocks = "_".join(freshStocks)
        openedFreshStocks = "_".join(openedFreshStocks)
        actulZtStocks = "_".join(actulZtStocks)
        dicts = {
            "freshStocks":freshStocks,
            "actulZtStocks":actulZtStocks,
            "openedFreshStocks":openedFreshStocks
        }
        mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":dicts},True)
        print "update 次新/涨停去除次新 for %s"%cdate

    def update_ZDT_contsNum(self, dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
        # step 2
        # 给定日期区间内，计算连续涨跌停个数，并去重zt_stocks, dt_stocks, 重新计算个数
        # 需要依赖数据库中已经有的涨跌停股票代码以及连续涨跌停数（如果没有连续值，则从0开始）
        mongoUrl = "localhost"
        mongodb = pymongo.MongoClient(mongoUrl)
        dateDicts = common.get_mongoDicts(dateEq=dateEq,dateStart=dateStart,dateEnd=dateEnd)

        for dateDict in dateDicts:
            cdate=dateDict['date']
            print "handle continus ZDT number for %s"%cdate
            cdate=datetime.datetime.strptime(cdate,"%Y%m%d").strftime("%Y-%m-%d")
            predate=common.get_last_date(cdate)
            common.update_numZD(cdate,predate,mongodb)


    def update_ZDT_yesterday(self,dateEq='19890928',dateStart = '19890928', dateEnd = '19890928'):
        # step 2
        ## 从指定日期开始，计算meat,hole, 以及pre的情况
        mongoUrl = "localhost"
        mongodb = pymongo.MongoClient(mongoUrl)
        dateDicts = common.get_mongoDicts(dateEq=dateEq,dateStart=dateStart,dateEnd=dateEnd)

        for dateDict in dateDicts:
            cdate=dateDict['date']
            cdate=datetime.datetime.strptime(cdate,"%Y%m%d").strftime("%Y-%m-%d")
            predate=common.get_last_date(cdate)
            self.get_meat(cdate,predate,mongodb)

    def get_statistics(self,stock_list,timestamp):
        stock_lists=stock_list
        # print stock_lists
        if len(stock_list)>200:
            tframe_list=[]
            for i in range(len(stock_list)/200+1):
                if 200*i > len(stock_list):
                    break
                tframe=common.get_value(stock_lists[200*i:200*(i+1)],timestamp)
                tframe_list.append(tframe)
            tframe=pd.concat(tframe_list,axis=0)
        else:
            tframe=common.get_value(stock_lists,timestamp)
        up_10=0
        dn_10=0
        pos=len(tframe[tframe.change_rate > 0])
        pos_rate=round(float(pos)/len(tframe),2)
        mean=tframe['change_rate'].mean()
        pos_rate=round(pos_rate,2)
        mean=round(mean,2)
        return (up_10,dn_10,pos_rate,mean,tframe)

    def get_meat(self,cdate,predate,mongodb):
        Sframe=pd.DataFrame()
        count = 0
        predate = datetime.datetime.strptime(predate,"%Y-%m-%d").strftime("%Y%m%d")
        print predate
        cdate = datetime.datetime.strptime(cdate,"%Y-%m-%d").strftime("%Y%m%d")
        cdateinfo = mongodb.stock.ZDT_by_date.find_one({"date":cdate})
        predateinfo = mongodb.stock.ZDT_by_date.find_one({"date":predate})
        # pre_ztlist = predateinfo['ZT_stocks'].split("_")
        pre_ztlist = predateinfo['actulZtStocks'].split("_")
        pre_dtlist = predateinfo['DT_stocks'].split("_")
        timestamp = time.strftime("%X",time.localtime())
        ztframe=pd.DataFrame()

        if pre_ztlist[0] != u'':
            # print "handling zt list... of %s"%pre_ztlist
            (up_10,dn_10,pos_rate,mean,ztframe)=self.get_statistics(pre_ztlist,cdate)
            Sframe.loc[count,'time']=timestamp
            Sframe.loc[count,'zt']=up_10
            Sframe.loc[count,'dt']=dn_10
            Sframe.loc[count,'pos_rate']=pos_rate
            Sframe.loc[count,'mean']=mean

        if pre_dtlist[0] != u'':
            # print "handling dt list... of %s"%pre_dtlist
            (up_10,dn_10,pos_rate,mean,dtframe)=self.get_statistics(pre_dtlist,cdate)
            Sframe.loc[count,'ddt']=dn_10
            Sframe.loc[count,'dposr']=pos_rate
            Sframe.loc[count,'dmean']=mean

        if not ztframe.empty:
            meatFrame = ztframe[ztframe.change_rate > 3]
            holeFrame = ztframe[ztframe.change_rate < -2]
            meatList = "_".join(list(meatFrame['stcid'].values))
            holeList = "_".join(list(holeFrame['stcid'].values))
            mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"meatList":meatList,
                                                                                "holeList":holeList}},True,True)

        if 'ddt' not in Sframe.columns:         ##无跌停
            Sframe.loc[count,'dmean'] = 1000
            Sframe.loc[count,'dposr'] = 1000
        if 'zt' not in Sframe.columns:
            Sframe.loc[count,'pos_rate'] = 1000
            Sframe.loc[count,'mean'] = 1000

        mongodb.stock.ZDT_by_date.update({"date":cdate},{"$set":{"pre_dtm":Sframe.loc[count,'dmean'],
                                                                           "pre_dtpos":Sframe.loc[count,'dposr'],
                                                                           "pre_ztm":Sframe.loc[count,'mean'],
                                                                           "pre_ztpos":Sframe.loc[count,'pos_rate']}},True,True)
        print "update meat/hole for %s"%cdate

if __name__ == "__main__":
    MP=MainProc()
    equal_date = "20170728"
    date_start = "20170728"
    date_end = "20170801"
    direction = 2
    if direction == 1:
        MP.update_ZDT_stocksNum_ALL(dateEq=equal_date)
        MP.update_freshStocks(dateEq=equal_date)
        MP.update_ZDT_contsNum(dateEq=equal_date)
        MP.update_ZDT_yesterday(dateEq=equal_date)
    else:
        MP.update_ZDT_stocksNum_ALL(dateStart=date_start, dateEnd=date_end)
        MP.update_freshStocks(dateStart=date_start, dateEnd=date_end)
        MP.update_ZDT_contsNum(dateStart=date_start, dateEnd=date_end)
        MP.update_ZDT_yesterday(dateStart=date_start, dateEnd=date_end)

    # calframe=pd.read_csv(os.path.join("D:\Money","cal.csv"))
    # del calframe['0']
    # calframe.columns=['Time']
    # mongo_url="localhost"
    # mongodb=pymongo.MongoClient(mongo_url)
    # for date in calframe.Time.values:
    #     date = common.format_date(date,"%Y%m%d")
    #     if date < "20170103" or date > "20170213":
    #         continue
    #     queryResult = mongodb.stock.ZDT_by_date.find_one({"date":date})
    #     if queryResult is None:
    #         MP.update_ZDT_stocksNum([],[date])
    #         print date



