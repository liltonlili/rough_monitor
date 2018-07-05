##coding:utf-8
import sys
sys.path.append("D:\Money")
from common import *
import mysqldb
import pandas as pd
import os
import datetime
import report_env

class fetch_min_data:
    def __init__(self,sy,sm,ey,em,flag=1):
        self.sYear=sy
        self.sMonth=sm
        self.eYear=ey
        self.eMonth=em
        self.sqldb=mysqldata()
        self.readdir="D:\Money\lilton_code\Market_Mode\history_zt\ZDT"
        self.writedir="D:\Money\lilton_code\Market_Mode\history_zt\ZDT\minute"
        report_env.init_config(os.path.join("D:\Money\lilton_code\Market_Mode","conf.txt"))
        self.year=report_env.get_prop("year")
        self.month = report_env.get_prop("month")
        self.flag = flag
        self.run()

    def run(self):
        dateclass=gm_date(self.sYear,self.sMonth,self.eYear,self.eMonth)
        monthlist=dateclass.calList
        cu_year=0
        cu_month=0
        for date in monthlist:
            print date
            year=datetime.datetime.strptime(date,"%Y/%m/%d").year
            month=datetime.datetime.strptime(date,"%Y/%m/%d").month
            if self.flag:
                if (int(year)< int(self.year)) or (int(year) == int(self.year) and int(month) <=int(self.month)):
                    print "Thread fetch_data pass %s"%date
                    continue
            cu_year=year
            cu_month=month
            filename="%s_%s.csv"%(cu_year,cu_month)
            dataframe=pd.read_csv(os.path.join(self.readdir,filename))
            tmp_frame=pd.DataFrame()
            try:
                tmp_frame=self.fetch_data(date,dataframe,cu_year,cu_month)
                dates=datetime.datetime.strptime(date,'%Y/%m/%d').strftime("%Y%m%d")
            except:
                pass
            if len(tmp_frame)>5:
                tmp_frame.to_csv(os.path.join(self.writedir,"%s.csv"%dates))


    def fetch_data(self,date,dataframe,year,month):
        dates=datetime.datetime.strptime(date,'%Y/%m/%d')
        date=dates.strftime("%Y-%m-%d")
        date2=dates.strftime("%Y%m%d")
        tarframe=dataframe[dataframe.TRADE_DATE==date]
        stocklist=tuple(tarframe['TICKER_SYMBOL'].values)
        month=str(month)
        if len(month)!=2:
            month='0'+month
        sql='select * from equity_pricemin%s%s where ticker in %s and datadate=%s'%(year,month,stocklist,date2)
        adata=self.sqldb.mydb_query(sql)
        return adata

if __name__ == "__main__":
    z=fetch_min_data(2016,02,2016,02)
