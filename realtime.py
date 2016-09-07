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
import common
import redis

frame_list = []
class rtMonitor:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=1)
        self.get_stock_list()
        self.stframe = DataFrame()
        self.count = 0

    def get_stock_list(self):
        stock_list = self.redis.keys()
        self.stock_list = [x for x in stock_list if len(str(x)) == 6]
        self.stock_list = list(set(self.stock_list))

    def run(self):
        ttime = time.localtime()
        thour = ttime.tm_hour
        tmin = ttime.tm_min
        if (thour == 10 and tmin < 3) or (thour == 11 and tmin < 3) or (thour == 13 and tmin < 10 and tmin >= 8):
            self.get_stock_list()
        self.TTframe=self.get_price_frame()
        self.get_statistic(self.TTframe)

    '''
    time stockid close preclose high rate
    '''
    def get_price_frame(self):
        ttframe = common.get_price_from_redis(self.stock_list, self.redis)
        return ttframe

    def get_statistic(self,tframe):
        timestamp=tframe.loc[0,'time']
        tframe = tframe[tframe.rate > -15]
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
        z.run()
        # z.stframe.to_csv(os.path.join(dir,'realtime_summary.csv'))
        print z.stframe
        ttime=time.localtime()
        thour=ttime.tm_hour
        tmin=ttime.tm_min
        if (thour > 15) or (thour == 15 and tmin > 5):
            z1=getTenpercent.rtMonitor(z.redis)
            z1.runBatch()
            break
        time.sleep(40)

