#coding:utf8
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pandas import DataFrame,Series
import os
import common
from matplotlib.finance import candlestick_ohlc,candlestick2_ohlc
import matplotlib.dates as mdates

## 添加均线,利用CLOSE_PRICE,得到mean5,mean10,mean20...
def add_mean(dframe):
    for ma in [5,10,20,60,120]:
        dframe['mean%s'%ma]=dframe['CLOSE_PRICE'].rolling(window=ma).mean()

def plot_candlestick(sub,ax1,point =10, mount_flag = 0, direction = u'不买不卖'):
    sub=sub[sub.LOWEST_PRICE > 0]
    sub.reset_index(len(sub),inplace=True)
    del sub['index']

    step = len(sub)/point
    if step < 1:
        step = 1
    baseline = range(len(sub)/step+1)
    baseline = [int(x) * step for x in baseline]
    if len(sub) <= baseline[-1]:
        baseline = baseline[:-1]
        # baseline = range(len(sub))
    listLabels = sub['TRADE_DATE'].values
    max_price = max(sub['HIGHEST_PRICE'].values)
    min_price = min(sub['LOWEST_PRICE'].values)
    plot_max = max_price * 1.05
    plot_min = min_price * 0.95
    candlestick2_ohlc(ax1, sub['OPEN_PRICE'],sub['HIGHEST_PRICE'], sub['LOWEST_PRICE'],sub['CLOSE_PRICE'],  width=0.8, colorup='#ff1717',colordown='#53c156', alpha =1);

    if direction == u'卖出':
        ax1.plot(max(sub.index.values),sub['HIGHEST_PRICE'].values[-1],'gv')
    elif direction == u'买入':
        ax1.plot(max(sub.index.values),sub['LOWEST_PRICE'].values[-1],'r^')

    ## 成交量
    # ax2=ax1.twinx()
    ax2 = ax1
    if mount_flag == 1:
        plot_delta = plot_max - plot_min
        bar_height = plot_delta/5
        mount_min = plot_min - bar_height
        sub['Mount']=sub['DEAL_AMOUNT'].astype(np.float64)*bar_height/max(sub['DEAL_AMOUNT'].values)   ##归一化后的成交量
        upframe = sub[sub.CLOSE_PRICE > sub.OPEN_PRICE]
        downframe = sub[sub.CLOSE_PRICE < sub.OPEN_PRICE]
        ax2.bar(downframe.index.values,downframe['Mount'].values,bottom=mount_min,color='#53c156', edgecolor='black',width=0.5)
        ax2.bar(upframe.index.values,upframe['Mount'].values,bottom=mount_min,color='#ff1717', edgecolor='black',width=0.5)
        ax2.set_ylim([mount_min*0.95,plot_max])
    else:
        ax2.set_ylim([plot_min*0.95,plot_max])


    ##均线
    colors = ['r-','m-','b-','c-','y-']
    mas = [5,10,20,60,120]
    linearr = []
    desarr = []
    for count in range(0,5):
        ma = mas[count]
        color = colors[count]
        line,=ax1.plot(sub.index.values,sub['mean%s'%ma].values,color)
#         line,=ax1.plot(sub.index.values,sub['mean5'].values,color='r-')
        linearr.append(line)
        desarr.append("mean%s"%ma)

    ax1.set_ylabel("Price")
    ax1.set_xticks(baseline)
    ax1.set_xticklabels(listLabels[baseline],fontdict={'size':8})
    if max(baseline)> len(sub):
        ax1.set_xlim([min(baseline)-2,max(baseline)+2])
    else:
        ax1.set_xlim([min(baseline)-2,len(sub)+2])
    if direction == u'不买不卖':
        ax1.legend(linearr,desarr,loc='upper left',ncol=3)
    ax1.grid(True)

## 分时图
## 画分笔数据，根据时间来进行marker
## 分笔，分钟都可以，[lastprice,closeprice]二选一，[bartime，datatime]二选一，可能包含volume
## index [lastprice,closeprice]  [bartime，datatime] [volume]
def plot_dealDetail(dframe,ax1,time=None,direction=u'不买不卖', price=None, point=10, mount_flag = 0):

    ##指数去除掉
    if len(np.unique(dframe.exchangecd)) == 2:
        try:
            dframe=dframe[dframe.closeprice< 700]
            dframe.reset_index(len(dframe),inplace=True)
            del dframe['index']
        except:
            dframe=dframe[dframe.lastprice< 700]
            dframe.reset_index(len(dframe),inplace=True)
            del dframe['index']

    step = len(dframe)/point
    baseline = range(len(dframe)/step+1)
    baseline = [int(x) * step for x in baseline]
    if len(dframe) <= baseline[-1]:
        baseline = baseline[:-1]

    if 'datatime' not in dframe.columns:
        dframe['datatime']=dframe['bartime']

    listLabels = dframe['datatime'].values
    if 'lastprice' not in dframe.columns:
        dframe['lastprice']=dframe['closeprice']

    max_price = max(dframe['lastprice'].values)
    min_price = min(dframe['lastprice'].values)
    plot_max = max_price * 1.005
    plot_min = min_price


    line1,=ax1.plot(dframe.index.values,dframe.lastprice.values,'k-')

    ##买卖标注
    if time is not None:
        if len(time) < 8:
            time = '0'+time
        targetframe = dframe[dframe.datatime <= time]
        target_index = targetframe.index.values[-1]
        if direction == u'买入':
            line2,=ax1.plot(target_index,dframe.loc[target_index,'lastprice'],'r^',markersize=10)
        elif direction == u'卖出':
            line2,=ax1.plot(target_index,dframe.loc[target_index,'lastprice'],'cv',markersize=10)

    ## 成交量
    if mount_flag == 1 and "volume" in dframe.columns:
        plot_delta = plot_max - plot_min
        bar_height = plot_delta/5
        mount_min = plot_min - bar_height
        dframe['volume']=dframe['volume'].astype(np.float64)*bar_height/max(dframe['volume'].values)   ##归一化后的成交量
        ax1.bar(dframe.index.values,dframe['volume'].values,bottom=mount_min,color='k', edgecolor='black',width=0.5)
        ax1.set_ylim([mount_min*0.995,plot_max])
    else:
        ax1.set_ylim([plot_min*0.995,plot_max])

    ax1.set_ylabel("Price")
    ax1.set_xticks(baseline)
    ax1.set_xticklabels(listLabels[baseline])
    ax1.set_xlim([min(baseline)-5,max(baseline)+5])
    ax1.grid(True)


def study_plot(tmp_array,dirs):

    ## 日线dir
    daydir = dirs
    minsdir = "%s/mins"%dirs
    bothdir = "%s/both"%dirs

    if os.path.exists(minsdir):
        pass
    else:
        os.mkdir(minsdir)

    if os.path.exists(bothdir):
        pass
    else:
        os.mkdir(bothdir)

    [ddate,dtime,code,direction,price]=tmp_array
    end_date = common.format_date(ddate,"%Y-%m-%d")
    start_date = common.get_lastN_date(end_date,120)
    code = "0"*(6-len(str(int(code))))+str(int(code))

    sql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, \
    DEAL_AMOUNT from vmkt_equd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,end_date,code)
    sub = common.get_mysqlData_sqlquery(sql)

    idxcode = "000001"
    idxsql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_INDEX, OPEN_INDEX, HIGHEST_INDEX, LOWEST_INDEX, CLOSE_INDEX, \
    TURNOVER_VOL from vmkt_idxd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,end_date,idxcode)
    idx = common.get_mysqlData_sqlquery(idxsql)
    idx.columns=sub.columns

    add_mean(sub)
    add_mean(idx)

    tableTime = common.format_date(ddate,"%Y%m")
    endDate = common.format_date(ddate,"%Y%m%d")
    endDate = int(endDate)
    stockid = int(code)

    # table = "equity_pricefenbi%s"%tableTime
    table = "MarketDataTDB.equity_pricemin%s"%tableTime
    dtsql = "SELECT * from %s where ticker = %s and datadate = %s"%(table,stockid,endDate)
    dtv = common.get_mydb_sqlquery(dtsql)
    if len(dtv) == 0:
        table = "MarketDataL1.equity_pricemin%s"%tableTime
        dtsql = "SELECT * from %s where ticker = %s and datadate = %s"%(table,stockid,endDate)
        dtv = common.get_mydb_sqlquery(dtsql)

    table = "MarketDataTDB.equity_pricemin%s"%tableTime
    zssql = 'SELECT * from %s where datadate = %s and ticker = 1 and shortnm = "上证指数"'%(table,endDate)
    sh = common.get_mydb_sqlquery(zssql)
    if len(sh) == 0:
        table = "MarketDataL1.equity_pricemin%s"%tableTime
        zssql = 'SELECT * from %s where datadate = %s and ticker = 1 and shortnm = "上证指数"'%(table,endDate)
        sh = common.get_mydb_sqlquery(zssql)


    ##日线，daydir
    fig1 = plt.figure(figsize=[12,8])

    ##日线图
    point = 10
    # plt.title("%s"%code)
    ax1 = fig1.add_subplot(211)
    plot_candlestick(sub,ax1,point =10,direction = direction,mount_flag = 1)

    ax2 = fig1.add_subplot(212)
    plot_candlestick(idx,ax2,point =10,mount_flag = 1)

    plt.savefig(os.path.join(daydir,"%s_%s_%s.pdf"%(ddate,direction,code)),dpi=300)
    plt.savefig(os.path.join(daydir,"%s_%s_%s.png"%(ddate,direction,code)),dpi=300)
    plt.close()

    ##分时图,分钟线，在dir/mins
    fig2 = plt.figure(figsize=(12,8))
    ax3 = fig2.add_subplot(211)
    plt.title("%s"%code)
    plot_dealDetail(dtv,ax3,time=dtime,direction=direction,mount_flag=1)

    ax4 = fig2.add_subplot(212)
    plot_dealDetail(sh,ax4,time=dtime,direction=direction,mount_flag=1)

    plt.savefig(os.path.join(minsdir,"%s_%s_%s.pdf"%(ddate,direction,code)),dpi=300)
    plt.savefig(os.path.join(minsdir,"%s_%s_%s.png"%(ddate,direction,code)),dpi=300)
    plt.close()

    # #两者都有，在dir/both
    # fig3 = plt.figure(figsize=(12,8))







read_dir = "D:\Money\lilton_code\Market_Mode\learnModule"
read_file = "AH_2015.csv"

##read_content format
# 成交日期	成交时间	证券代码	证券名称	操作	成交数量	成交均价	成交金额	发生金额	本次金额
read_columns=['date','time','code','name','direction','amount','price','cjje','fsje','bcje','null','null']
dframe=pd.read_csv(os.path.join(read_dir,read_file),encoding='gbk')
dframe.columns=read_columns
# dframe.dropna(inplace=True)

exception_log = open(os.path.join("D:\Money\lilton_code\Market_Mode\learnModule","exception.log"),'w')
plot_dir = "D:\Money\lilton_code\Market_Mode\learnModule\%s"%(read_file.replace(".csv",""))
# for date in np.unique(dframe.date.values):
for date in [20150113,20150302,20150312,20150506,20150511,20150713,20150714,20150729,20150807,20150921,20150922,20150923,20150924,20150925,20150928,20150929,20150930,20151008]:
    try:
        date = int(date)
        # if date < 20150201:
        #     continue
        date = 20150302
        #创建文件夹
        store_dir = "%s/%s"%(plot_dir,date)
        if os.path.exists(store_dir):
            pass
        else:
            os.mkdir(store_dir)

        ##逐个画图
        todayframe = dframe[dframe.date==date]
        for index in todayframe.index.values:
            tmp_array = [date,todayframe.loc[index,'time'],todayframe.loc[index,'code'],
                         todayframe.loc[index,'direction'],todayframe.loc[index,'price']]
            study_plot(tmp_array,store_dir)
            print '%s,%s ...'%(date,todayframe.loc[index,'code'])
        print "\n#%s finished"%date
    except Exception,e:
        print "Exceptions: %s, in %s"%(e,date)
        exception_log.write("Exceptions: %s, in %s"%(e,date))
    # break
exception_log.close()