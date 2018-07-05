##coding:utf-8
import sys
sys.path.append("D:\Money")
from common import *
import mysqldb
import pandas as pd
import os
import datetime
import tushare as ts
import report_env
import pymongo

class statistic:
    def __init__(self,sy,sm,ey,em,flag=1):
        self.sYear=sy
        self.sMonth=sm
        self.eYear=ey
        self.eMonth=em
        self.daydir="D:\Money\lilton_code\Market_Mode\history_zt\ZDT"
        self.statisdir = "D:\Money\lilton_code\Market_Mode\history_zt\ZDT\statis_resus"
        self.contup={} ## continue up 10%
        self.contdn={} ## continue down 10%
        self.lastup=[]
        self.lastdn=[]
        self.lastsh=0
        self.lastsz=0
        mongo_url = "localhost"
        self.mongodb = pymongo.MongoClient(mongo_url)
        self.flag = flag
        report_env.init_config(os.path.join("D:\Money\lilton_code\Market_Mode","conf.txt"))
        self.year=report_env.get_prop("year")
        self.month = report_env.get_prop("month")
        self.stats_num()


    # statistic the number of 10 everyday
    ## 统计每天的连板股个数，昨日涨停的平均收益
    def stats_num(self,):
        dateclass=gm_date(self.sYear,self.sMonth,self.eYear,self.eMonth)
        monthlist=dateclass.calList
        prev_date = ""
        num_ucont={"2":0,"3":0,"4":0,"5":0,"6":0,"+6":0}    #连张股票个数统计
        stoc_ucont={"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"+6":[]}   #连涨股票代码
        num_dcont={"2":0,"3":0,"4":0,"5":0,"6":0,"+6":0}    #连张股票个数统计
        stoc_dcont={"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"+6":[]}   #连涨股票代码
        for date in monthlist:
            year=datetime.datetime.strptime(date,"%Y/%m/%d").year
            month=datetime.datetime.strptime(date,"%Y/%m/%d").month
            if self.flag:
                if (int(year)< int(self.year)) or (int(year) == int(self.year) and int(month) <=int(self.month)):
                    # print "Thread statistic pass %s"%date
                    continue
            format_date=datetime.datetime.strptime(date,"%Y/%m/%d").strftime("%Y-%m-%d")
            prev_date=get_last_date(format_date)

            ## 昨日涨跌停股票列表
            (pre_ztlist,pre_dtlist)=self.get_zdtlist(prev_date)
            (ztlist,dtlist)=self.get_zdtlist(date)

            ## 昨日涨停股票今日收益情况
            if len(pre_ztlist)==1 and len(pre_ztlist[0]) < 1:
                pre_zt_value=1000
                pre_zt_mean=1000
                pre_zt_posr=1000
            else:
                pre_zt_value=get_value(pre_ztlist,format_date)
                pre_zt_mean=pre_zt_value['change_rate'].mean()
                pre_zt_posr=round(float(len(pre_zt_value[pre_zt_value.change_rate > 0]))/len(pre_zt_value),2)
            ## 昨日涨停股票今日收益情况
            if len(pre_dtlist) == 1 and len(pre_dtlist[0])<1:
                pre_dt_value=1000
                pre_dt_mean=1000
                pre_dt_posr=1000
            else:
                pre_dt_value=get_value(pre_dtlist,format_date)
                pre_dt_mean=pre_dt_value['change_rate'].mean()
                pre_dt_posr=round(float(len(pre_dt_value[pre_dt_value.change_rate > 0]))/len(pre_dt_value),2)

            # ##今日涨跌停股票收益情况
            # zt_value=get_value(ztlist,format_date)
            # dt_value=get_value(dtlist,format_date)

            ## 连涨连跌统计
            (stoc_ucont,num_ucont,stoc_dcont,stoc_dcont)=self.cont_stat(ztlist,dtlist,num_ucont,stoc_ucont,num_dcont,stoc_dcont)
            dicts={
                "details":{
                    "stoc_ucont":stoc_ucont,
                    "stoc_dcont":stoc_dcont},
                "num_ucont":num_ucont,
                "num_dcont":num_dcont,
                "pre_ztm":pre_zt_mean,
                "pre_ztpos":pre_zt_posr,
                "pre_dtm":pre_dt_mean,
                "pre_dtpos":pre_dt_posr
            }
            write_mongo(self.mongodb,date,dicts)

    # num_ucont={"2":0,"3":0,"4":0,"5":0,"6":0,"+6":0}    #连张股票个数统计
    # stoc_cont={"1":[],"2":[],"3":[],"4":[],"5":[],"6":[],"+6":[]}   #连涨股票代码
    def cont_stat(self,zt_list,dt_list,num_ucont,stoc_ucont,num_dcont,stoc_dcont):
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
        for num in [2,3,4,5,6]:
            last_num="%s"%(num-1)
            num = "%s"%num
            pre_dn=stoc_dcont[last_num]
            stoc_dcont[num]=[x for x in zt_list if x in pre_dn]
            num_dcont[num]=len(stoc_dcont[num])
        stoc_dcont["1"]=dt_list
        return (stoc_ucont,num_ucont,stoc_dcont,stoc_dcont)

    def get_zdtlist(self,date):
        try:
            format_date=datetime.datetime.strptime(date,"%Y-%m-%d").strftime("%Y%m%d")
        except:
            try:
                format_date=datetime.datetime.strptime(date,"%Y/%m/%d").strftime("%Y%m%d")
            except:
                format_date=datetime.datetime.strptime(date,"%Y%m%d").strftime("%Y%m%d")
        obj_mongo=self.mongodb.stock.ZDT_by_date.find_one({"date":format_date})
        zt_list=[]
        dt_list=[]
        if obj_mongo:
            zt_str=obj_mongo["ZT_stocks"]
            dt_str=obj_mongo["DT_stocks"]
            zt_list=zt_str.split("_")
            dt_list=dt_str.split("_")
        return (zt_list,dt_list)

def write_mongo(mongo,date,dicts):
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
        print "begin to update,%s"%format_date
        mongo.stock.ZDT_by_date.update_one({"date":format_date},
                                           {"$set":{
                                               "details":dicts['details'],
                                               "num_ucont":dicts["num_ucont"],
                                               "num_dcont":dicts["num_dcont"],
                                                "pre_ztm":dicts["pre_ztm"],
                                                "pre_ztpos":dicts["pre_ztpos"],
                                                "pre_dtm":dicts["pre_dtm"],
                                                "pre_dtpos":dicts["pre_dtpos"]}
                                           })

if __name__ == "__main__":
    z=statistic(2013,02,2014,12)