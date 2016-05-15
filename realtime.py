__author__ = 'li.li'
## --*-- coding:utf8 --*--
import pandas as pd
from pandas import DataFrame
import os
import requests
import time
import numpy as np
import multiprocessing
import getTenpercent
import tushare as ts

frame_list=[]
class rtMonitor:
    def __init__(self):
        dir = 'D:\Money\lilton_code'
        while(True):
            zs=ts.get_today_all()
            if len(zs) > 1000:
                zs.to_csv(os.path.join(dir,'stock_list.csv'),encoding='utf8')
                break
            time.sleep(5)
        stock_list=pd.read_csv(os.path.join(dir,'stock_list.csv'))
        stock_list=stock_list['code'].values
        stock_lists=[]
        self.stframe=DataFrame()
        self.count=0

        for code in stock_list:
            code = '0'*(6-len(str(code)))+str(code)
            if code[0:2] == '60':
                code = 'sh'+code
            else:
                code = 'sz'+code
            stock_lists.append(code)
        self.stock_lists=stock_lists
        self.groupID=range(len(stock_lists)/200+1)

    def get_data(self,stock_list,timestamp,i):
        dir = 'D:\Money\Realtime'
        slist=','.join(stock_list)
        url = "http://hq.sinajs.cn/list=%s"%slist
    #     print url
        try:
            r=requests.get(url)
        except:
            r=requests.get(url)
        content=r.content.decode('gbk')
        Dframe=self.parse_content(content,timestamp)
        Dframe.to_csv(os.path.join(dir,'%s_realtime.csv'%i))
        # print self.frame_list
        return Dframe

    def parse_content(self,content,timestamp):
        Inframe=DataFrame()
        i = 0
        strarray=content.split(';')
        for item in strarray:
    #         print item
            item_array=item.split(',')
            if len(item_array)<10:
                continue
            stockid = item_array[0][14:20]
            stockid = item_array[0].split('=')[0].split('str_')[1][2:]
            close = item_array[3]
            preclose = item_array[2]
            high = item_array[4]

            if close == '0.00':
                continue
            Inframe.loc[i,'time']=timestamp
            Inframe.loc[i,'stcid']=stockid
            Inframe.loc[i,'close']=close
            Inframe.loc[i,'preclose']=preclose
            Inframe.loc[i,'high']=high
            i+=1
        Inframe['rate']=100*(Inframe['close'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
        Inframe['rate']=Inframe['rate'].round(decimals=2)
        # Inframe['hate']=100*(Inframe['high'].astype(np.float64)-Inframe['preclose'].astype(np.float64))/Inframe['preclose'].astype(np.float64)
        # Inframe['hate']=Inframe['hate'].round(decimals=2)
        return Inframe


    def runBatch(self):

        ttime=time.localtime()
        thour=ttime.tm_hour
        tmin=ttime.tm_min
        if (thour == 10 and tmin < 3) or (thour == 11 and tmin < 3) or (thour == 13 and tmin < 10 and tmin >= 8):
            self.__init__()

        timestamp=time.strftime("%X",time.localtime())
#         i = 0
#         self.get_data(self.stock_lists[i*200:(i+1)*200],timestamp)
        for i in self.groupID:
            # print i
            if (i+1)*200 > len(self.stock_lists):
                subproc=multiprocessing.Process(target=self.get_data,args=(self.stock_lists[i*200:],timestamp,i))
            else:
                subproc=multiprocessing.Process(target=self.get_data,args=(self.stock_lists[i*200:(i+1)*200],timestamp,i))
            # subproc.daemon =True
            subproc.start()
            subproc.join(60)

        self.TTframe=self.get_summary()
        self.get_statistic(self.TTframe)

    def get_summary(self):
        dir = 'D:\Money\Realtime'
        frame_list=[]
        for i in self.groupID:
            tmpframe=pd.read_csv(os.path.join(dir,'%s_realtime.csv'%i))
            del tmpframe['Unnamed: 0']
            frame_list.append(tmpframe)
        ttframe=pd.concat(frame_list,axis=0)
        return ttframe

    def get_statistic(self,tframe):
        timestamp=tframe.loc[0,'time'].values[0]
        tframe['upercent10']=(tframe['preclose'].astype(np.float64)*1.1).round(2)
        tframe['dpercent10']=(tframe['preclose'].astype(np.float64)*0.9).round(2)
        uframe=tframe[tframe.close>=tframe.upercent10]
        dframe=tframe[tframe.close==tframe.dpercent10]
        p5frame=tframe[tframe.rate>=5]
        n5frame=tframe[tframe.rate<=-5]
        self.stframe.loc[self.count,'time']=timestamp
        self.stframe.loc[self.count,'up10_num']=len(uframe)
        self.stframe.loc[self.count,'dn10_num']=len(dframe)
        self.stframe.loc[self.count,'>+5']=len(p5frame)
        self.stframe.loc[self.count,'<-5']=len(n5frame)
        self.count+=1


if __name__=='__main__':
    z=rtMonitor()
    dir = 'D:\Money\Realtime'
    while(True):
        z.runBatch()
        z.stframe.to_csv(os.path.join(dir,'realtime_summary.csv'))
        print z.stframe
        ttime=time.localtime()
        thour=ttime.tm_hour
        tmin=ttime.tm_min
        if (thour > 15) or (thour == 15 and tmin > 5):
            z=getTenpercent.rtMonitor()
            z.runBatch()
            break
        time.sleep(40)

