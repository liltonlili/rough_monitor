__author__ = 'li.li'
import pandas as pd
import numpy as np
import datetime
import time as tt
import os


class curPoints:
    def __init__(self):
        self.statisdir = "D:\Money\lilton_code\Market_Mode\history_zt\ZDT\statis_resus"
        self.ZDTdir = "D:\Money\lilton_code\Market_Mode\history_zt\ZDT"
        year=datetime.date.today().year
        day = datetime.date.today().day
        month = datetime.date.today().month
        today=datetime.date.today().strftime("%Y/%m/%d")
        lastday = datetime.datetime.strptime(get_day(-1,today),"%Y/%m/%d").strftime("%Y%m%d")

        if month ==1:
            last_month =12
            last_year=year-1
        else:
            last_month=month-1
            last_year=year


        csv1='%s_%s.csv'%(year,month)
        csv2='%s_%s.csv'%(last_year,last_month)

        ##
        Dframe1=pd.read_csv(os.path.join(self.statisdir,csv1))
        Dframe2=pd.read_csv(os.path.join(self.statisdir,csv2))
        Dframe=pd.concat([Dframe1,Dframe2],axis=0)
        ZDTframe=Dframe.sort_values('date')
        ZDTframe.reset_index(len(ZDTframe),inplace=True)
        del ZDTframe['index']
        del ZDTframe['Unnamed: 0']

        ##
        Dframe1=pd.read_csv(os.path.join(self.statisdir,csv1))
        Dframe2=pd.read_csv(os.path.join(self.statisdir,csv2))
        Dframe=pd.concat([Dframe1,Dframe2],axis=0)
        Dframe=Dframe.sort_values('date')
        Dframe.reset_index(len(Dframe),inplace=True)
        del Dframe['index']
        del Dframe['Unnamed: 0']

        ## calculate the base points of yesterday
        index=[0.1,0.2,0.5,1]
        calframe=Dframe[Dframe.date==lastday]
        yes_point=calframe['cont_2up']*index[0]+calframe['cont_3up']*index[1]+calframe['cont_4up']*index[2]+calframe['cont_5up']*index[3]

        ##re-calculate the points
        recframe=Dframe[Dframe.date<=lastday].tail(5)
