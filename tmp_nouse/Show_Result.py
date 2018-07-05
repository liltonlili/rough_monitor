__author__ = 'li.li'
##coding:utf-8
import sys
sys.path.append("D:\Money")
from common import *
import mysqldb
import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt


class show_resut:
    def __init__(self,sy,sm,ey,em):
        self.sYear=sy
        self.sMonth=sm
        self.eYear=ey
        self.eMonth=em
        # self.sqldb=mysqldata()
        self.daydir="D:\Money\lilton_code\Market_Mode\history_zt\ZDT"
        self.minudir="D:\Money\lilton_code\Market_Mode\history_zt\ZDT\minute"
        self.statisdir = "D:\Money\lilton_code\Market_Mode\history_zt\ZDT\statis_resus"

    def read_date(self):
        Dframe=pd.DataFrame()
        tlist = get_ympair(self.sYear,self.sMonth,self.eYear,self.eMonth)
        print self.eMonth
        print tlist
        for (year,month) in tlist:
            file = "%s_%s.csv"%(year,month)
            tmpframe=pd.read_csv(os.path.join(self.statisdir,file))
            del tmpframe['Unnamed: 0']
            Dframe=pd.concat([Dframe,tmpframe],axis=0)
        # print tlist
        Dframe.reset_index(range(len(Dframe)),inplace=True)
        del Dframe['index']
        Dframe.fillna(value=0,inplace=True)
        return  Dframe


    def plot(self):
        Dframe=self.read_date()
        fig=plt.figure(figsize=(12,8))
        ## x index
        baseline = range(len(Dframe)/20+1)
        baseline = [int(x) * 20 for x in baseline]

        fz=8
        ## datestring index
        datestr=Dframe['date'].values
        datestr=np.array([str(x)[2:] for x in datestr])
        ax1=fig.add_subplot(221)
        ## plot ZT VS DT num
        line11,=ax1.plot(Dframe.index,Dframe.ztnum.values,'r-')
        line12,=ax1.plot(Dframe.index,Dframe.dtnum.values,'g-')
        ax1.grid(True)

        ax1.set_xticks(baseline)
        ax1.set_ylim([0,max(Dframe.ztnum.values)])
        ax1.set_xticklabels(datestr[baseline],fontsize=fz)
        ax1.set_xlim([min(Dframe.index.values),max(Dframe.index.values)])
        ax12=ax1.twinx()
        line13,=ax12.plot(Dframe.index,Dframe.sh_change,'b-')
        ax12.grid(True)
        ax12.set_xticks(baseline)
        ax12.set_ylim([min(Dframe.sh_change.values),max(Dframe.sh_change.values)])

        ax1.legend([line11,line12,line13],('up10%','dn10%','sh_change'),'upper left',ncol=3)

        ## plot ZT VS DT num
        ax2=fig.add_subplot(222)
        line21,=ax2.plot(Dframe.index,Dframe.lzt_mean.values,'r-')
        line22,=ax2.plot(Dframe.index,Dframe.ldt_mean.values,'g-')
        ax2.grid(True)

        ax2.set_xticks(baseline)
        ax2.set_xticklabels(datestr[baseline],fontsize=fz)
        ax22=ax2.twinx()
        line23,=ax22.plot(Dframe.index,Dframe.sh_change,'b-')
        ax22.grid(True)
        ax22.set_xticks(baseline)
        ax22.set_ylim([min(Dframe.sh_change.values),max(Dframe.sh_change.values)])
        ax2.legend([line21,line22,line23],('mean_zt','mean_dt','sh_change'),'upper center',ncol=3)
        ## plot lzt_pos, ldt_pos
        ax3=fig.add_subplot(223)
        line31,=ax3.plot(Dframe.index,Dframe.pos_lzt.values,'r-')
        line32,=ax3.plot(Dframe.index,Dframe.pos_ldt.values,'g-')
        ax3.grid(True)

        ax3.set_xticks(baseline)
        ax3.set_xticklabels(datestr[baseline],fontsize=fz)
        ax32=ax3.twinx()
        line33,=ax32.plot(Dframe.index,Dframe.sh_change,'b-')
        ax32.grid(True)
        ax32.set_xticks(baseline)
        ax32.set_ylim([min(Dframe.sh_change.values),max(Dframe.sh_change.values)])
        ax3.legend([line31,line32,line33],('pos_zt','pos_dt','sh_change'),'upper center',ncol=3)

        ## plot continue zt num
        ax4=fig.add_subplot(224)
        line41,=ax4.plot(Dframe.index,Dframe.cont_3up.values,'r-')
        line42,=ax4.plot(Dframe.index,Dframe.cont_3up.values,'g-')
        line43,=ax4.plot(Dframe.index,Dframe.cont_3up.values,'b-')
        line44,=ax4.plot(Dframe.index,Dframe.cont_3up.values,'y-')
        ax4.grid(True)

        ax4.set_xticks(baseline)
        ax4.set_ylim([min(Dframe.cont_3up.values),max(Dframe.cont_3up.values)])
        ax4.set_xticklabels(datestr[baseline],fontsize=fz)
        ax42=ax4.twinx()
        line45,=ax42.plot(Dframe.index,Dframe.sh_change,'m-')
        ax42.grid(True)
        ax42.set_xticks(baseline)
        ax42.set_ylim([min(Dframe.sh_change.values),max(Dframe.sh_change.values)])
        ax4.legend([line41,line42,line43,line44,line45],('cont_2','cont_3','cont_4','cont_5','sh_change'),'upper center',ncol=3)


        plt.savefig("test.pdf")
        plt.show()
        ## plot continue dt num

z=show_resut(2014,0,2010,06)
z.plot()