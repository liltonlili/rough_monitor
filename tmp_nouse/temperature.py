__author__ = 'li.li'
import pandas as pd
from pandas import DataFrame,Series
import time as tt
import datetime
import sys
import matplotlib.pyplot as plt
sys.path.append("D:\Money")
import numpy as np
from common import *

class get_temperature:
    def __init__(self,syear,smonth,eyear,emonth):
        self.statisdir = 'D:\Money\lilton_code\Market_Mode\history_zt\ZDT\statis_resus'
        self.syear=syear
        self.smonth=smonth
        self.eyear=eyear
        self.emonth=emonth
        self.run()


    def run(self):
        ## read csv
        tlist = get_ympair(self.syear,self.smonth,self.eyear,self.emonth)
        Dframe=DataFrame()

        for (year,month) in tlist:
            file = "%s_%s.csv"%(year,month)
            tmpframe=pd.read_csv(os.path.join(self.statisdir,file))
            del tmpframe['Unnamed: 0']
            Dframe=pd.concat([Dframe,tmpframe],axis=0)

        Dframe.reset_index(range(len(Dframe)),inplace=True)
        del Dframe['index']
        # print Dframe
        ## calculate the temperature
        calframe=Dframe.copy()
        index=[0.1,0.2,0.5,1]
        calframe['points']=calframe['cont_2up']*index[0]+calframe['cont_3up']*index[1]+calframe['cont_4up']*index[2]+calframe['cont_5up']*index[3]
        print calframe[['date','ztnum','lzt_mean','pos_lzt','points']]

        # ## delete the max 3 value
        # indexnum = calframe[calframe.points==calframe.points.max()].index.values[0]
        # calframe.drop(indexnum,inplace=True)
        # indexnum = calframe[calframe.points==calframe.points.max()].index.values[0]
        # calframe.drop(indexnum,inplace=True)
        # indexnum = calframe[calframe.points==calframe.points.max()].index.values[0]
        # calframe.drop(indexnum,inplace=True)

        calframe['shift_lzt_mean']=np.ones(len(calframe))
        calframe['shift_lzt_mean'][0:-1]=calframe['lzt_mean'][1:]

        calframe['shift_pos_lzt']=np.zeros(len(calframe))
        calframe['shift_pos_lzt'][0:-1]=calframe['pos_lzt'][1:]

        threshold=10
        fig=plt.figure()
        ax1=fig.add_subplot(111)
        ##########################################set here for the x axi##############
        index_num=2
        ##############################################################################
        baseline=range(len(calframe)/index_num)
        baseline = [int(x) * index_num for x in baseline]
        datastr=calframe['date'].values

        line11,=ax1.plot(calframe.index.values,calframe.points.values,'k-')
        line11,=ax1.plot(calframe.index.values,calframe.points.values,'ko')
        base=np.ones(len(calframe))*5
        line0,=ax1.plot(calframe.index.values,base,'m-')
        base2=np.ones(len(calframe))*10
        line0,=ax1.plot(calframe.index.values,base2,'c-')

        # line11,=ax1.plot(calframe.index.values,calframe.points.values,'yo')
        ax1.grid(True)
        ax1.set_xticks(baseline)
        ax1.set_xticklabels(datastr[baseline])
        ax1.set_xlim([min(calframe.index.values),max(calframe.index.values)])
        # calframe['points'].plot()
        ax12=ax1.twinx()
        line12,=ax12.plot(calframe.index.values,calframe.shift_lzt_mean.values,'ro')
        line12,=ax12.plot(calframe.index.values,calframe.shift_lzt_mean.values,'r--')
        # for i in calframe.index.values:
        #     if calframe.loc[i,'points']>= threshold:
        #         line12,=ax12.plot(i,calframe.loc[i,"shift_lzt_mean"],'ro')
        #         line12,=ax12.plot(i,calframe.loc[i,"shift_lzt_mean"],'r-')
        #     else:
        #         line12,=ax12.plot(i,calframe.loc[i,"shift_lzt_mean"],'go')
        #         line12,=ax12.plot(i,calframe.loc[i,"shift_lzt_mean"],'g-')

        # line12,=ax12.plot(calframe.index.values,calframe.shift_lzt_mean.values,'b-')
        # line12,=ax12.plot(calframe.index.values,calframe.shift_lzt_mean.values,'bo')
        ax12.set_ylim([min(calframe.shift_lzt_mean.values),max(calframe.shift_lzt_mean.values)])
        ax1.legend([line11,line12],('points','lzt_mean'),ncol=2,loc='bottom center')
        plt.show()
        # print calframe
