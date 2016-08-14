# coding:utf8
import common
import pandas as pd
import numpy as np
import datetime
import time
import pymongo

# 只run一次，得到一个dict，key为股票id，value为{对应的最高价日期:N天内的最高价格（后复权)}

class ExtractNewHigh:
    def __init__(self):
        mongoUrl="localhost"
        self.period = 120
        self.mongodb = pymongo.MongoClient(mongoUrl)
        self.today = datetime.date.today().strftime("%Y%m%d")
        self.tday = common.get_lastN_date(self.today,self.period)
        self.highFrame = pd.DataFrame()
        self.dateList = common.getDate(self.tday,self.today)

    #      TICKER_SYMBOL SEC_SHORT_NAME  TRADE_DATE  PRE_CLOSE_PRICE  OPEN_PRICE    HIGHEST_PRICE   LOWEST_PRICE  CLOSE_PRICE
# 0           000033          *ST新都  2016-04-27            10.38        0.00           0.00          0.00        10.38
# 1           600710          *ST常林  2016-04-27             9.36        0.00         10.61         10.52        10.57

    def run(self):
        for date in self.dateList:
            print date
            tmpframe = common.get_mysqlData('',[date])
            tmpframe = tmpframe[['TICKER_SYMBOL','TRADE_DATE','HIGHEST_PRICE','ACT_PRE_CLOSE_PRICE','PRE_CLOSE_PRICE']]
            tmpframe.set_index("TICKER_SYMBOL",inplace=True)
            if not self.highFrame.empty:
                campframe = pd.concat([self.highFrame,tmpframe],axis=1)
                self.highFrame = self.compare(campframe)
            else:
                self.highFrame = tmpframe[['TRADE_DATE','HIGHEST_PRICE']]
                self.highFrame.columns=['Hdate','Hprice']

        self.highFrame.reset_index(range(len(self.highFrame)),inplace=True)
        self.highFrame.columns = ['stcid','Hdate','Hprice']
        self.highFrame.to_csv("verify.csv")
        self.highFrame = pd.read_csv("verify.csv")
        self.saveToMongo()

    def saveToMongo(self):
        for i in self.highFrame.index:
            dict = {}
            stcid = str(self.highFrame.loc[i,'stcid'])
            dict['stcid'] = "0"*(6-len(stcid))+stcid
            dict['Hdate'] = self.highFrame.loc[i,'Hdate']
            dict['Hprice'] = self.highFrame.loc[i,'Hprice']
            dict['Period'] = self.period
            self.mongodb.stock.HighestPrice.update({"stcid":dict['stcid']},{"$set":dict},True)

    ## TICKER_SYMBOL Hdate Hprice TRADE_DATE HIGHEST_PRICE
    def compare(self,dframe):
        ## 进行复权处理
        dframe.fillna(value=1,inplace=True)
        dframe['crate']=dframe['ACT_PRE_CLOSE_PRICE'].astype(np.float64)/dframe['PRE_CLOSE_PRICE'].astype(np.float64)
        dframe['Hprice']=dframe['Hprice'].astype(np.float64)/dframe['crate']
        dframe['Hprice']=dframe['Hprice'].round(decimals = 2)

        #High price交换
        aframe = dframe[dframe.Hprice >= dframe.HIGHEST_PRICE]
        bframe = dframe[dframe.Hprice < dframe.HIGHEST_PRICE]
        bframe['Hprice']=bframe['HIGHEST_PRICE']
        bframe['Hdate']=bframe['TRADE_DATE']
        return pd.concat([aframe,bframe],axis=0)[['Hdate','Hprice']]




if __name__ == "__main__":
    z = ExtractNewHigh()
    z.run()