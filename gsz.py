# coding:utf-8
import requests
from lxml import etree
import os
import pandas as pd
import common
import matplotlib.pyplot as plt
from LearnFrom import add_mean, plot_candlestick, plot_dealDetail
import numpy as np

mytype = 2

# 前复权
'''
dataframe为：
TICKER_SYMBOL SEC_SHORT_NAME TRADE_DATE PRE_CLOSE_PRICE OPEN_PRICE HIGHEST_PRICE LOWEST_PRICE CLOSE_PRICE DEAL_AMOUNT
300367          东方网力      2016-03-10   65.1           66.95          68.5        64.01         65.6        18567
'''
def regulate_data(dataframe):
    latest_close = dataframe['CLOSE_PRICE'].values[-1]
    index_array = dataframe.index.values[::-1]
    map_list = []
    last_start = -1
    # print "origin dataframe"
    # dataframe.to_csv("./origin.csv", encoding='gbk')
    last_end = -1
    for index in index_array:
        if index >= 1:
            pre_close = dataframe.loc[index, 'PRE_CLOSE_PRICE']
            last_close = dataframe.loc[index-1, 'CLOSE_PRICE']
            if pre_close != last_close:
                rate = round(float(last_close)/pre_close, 3)
                if last_start == -1:    # 说明是第一个
                    last_start = index - 1
                    last_rate = rate
                else:
                    last_end = index
                    map_list.append([last_rate, [last_start, last_end]])
                    last_start = index - 1
                    last_rate = rate
        else:
            if last_start != -1:
                map_list.append([last_rate, [last_start, 0]])

    last_rate = 1
    for regu_rate, [start, end] in map_list:
        regu_rate = regu_rate * last_rate
        for item in ['PRE_CLOSE_PRICE', 'OPEN_PRICE', 'HIGHEST_PRICE', 'LOWEST_PRICE', 'CLOSE_PRICE']:
            dataframe.loc[end:start, item] = dataframe.loc[end:start, item].astype(np.float64) / regu_rate
            dataframe.loc[end:start, item] = dataframe.loc[end:start, item].round(2)
    # print "after word"
    # dataframe.to_csv("./after.csv", encoding='gbk')
    return dataframe


def study_plot(tmp_array,dirs, name_tail = u'', end_date_interval = 0, start_date_interval = 0):

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
    start_date = common.get_lastN_date(end_date, start_date_interval)
    day_end_date = common.get_lastN_date(end_date, end_date_interval)
    code = "0"*(6-len(str(int(code))))+str(int(code))

    # 日线数据
    sql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_PRICE, OPEN_PRICE, HIGHEST_PRICE, LOWEST_PRICE, CLOSE_PRICE, \
    DEAL_AMOUNT from vmkt_equd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,day_end_date,code)
    sub = common.get_mysqlData_sqlquery(sql)
    sub = regulate_data(sub)

    idxcode = "000001"
    idxsql = "SELECT TICKER_SYMBOL, SEC_SHORT_NAME, TRADE_DATE, PRE_CLOSE_INDEX, OPEN_INDEX, HIGHEST_INDEX, LOWEST_INDEX, CLOSE_INDEX, \
    TURNOVER_VOL from vmkt_idxd where TRADE_DATE >= '%s' and TRADE_DATE <='%s' and TICKER_SYMBOL = '%s'"%(start_date,day_end_date,idxcode)
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

    # ##临时增加，需要保存数据
    # tmp_dir = "D:\Money\lilton_code\Market_Mode\study_fresh_fail_Module\data"
    # dtv.to_csv(os.path.join(tmp_dir,"%s_%s.csv"%(ddate,code)),encoding='utf8')

    [name,tmp_id]=common.QueryStockMap(id=code)
    ##日线，daydir
    fig1 = plt.figure(figsize=[12,8])

    ##日线图
    point = 10
    # plt.title("%s"%code)
    plt.title(u"%s_%s_%s_d.png"%(ddate, code, name_tail))
    ax1 = fig1.add_subplot(211)
    plot_candlestick(sub, ax1, point=10, direction=direction, mount_flag=1, mark_date=end_date)

    ax2 = fig1.add_subplot(212)
    plot_candlestick(idx, ax2, point=10, mount_flag=1, mark_date=end_date)

    # plt.savefig(os.path.join(daydir,"%s_%s_%s.pdf"%(ddate,direction,code)),dpi=300)
    plt.savefig(os.path.join(daydir, "%s_%s_%s_%s_m.png"%(ddate, code, name, name_tail)),dpi=300)
    plt.close()

    ##分时图,分钟线，在dir/mins
    fig2 = plt.figure(figsize=(12,8))
    ax3 = fig2.add_subplot(211)


    plt.title("%s"%code)
    plot_dealDetail(dtv,ax3,time=dtime,direction=direction,mount_flag=1)

    ax4 = fig2.add_subplot(212)
    plot_dealDetail(sh,ax4,time=dtime,direction=direction,mount_flag=1)

    # plt.savefig(os.path.join(minsdir,"%s_%s_%s_%s.pdf"%(ddate,direction,code,name)),dpi=300)
    plt.savefig(os.path.join(minsdir,"%s_%s_%s_%s.png"%(ddate, code, name, name_tail)),dpi=300)
    plt.close()


if mytype == 1:
    pages = 6
    ofile = open(os.path.join("D:\Money\lilton_code\Market_Mode\learnModule\gsz","gsz.csv"), 'wb')

    for page in range(pages):
        page += 1
        url = "http://data.cfi.cn/cfidata.aspx?sortfd=&sortway=&curpage=%s&fr=content&ndk=A0A1934A1939A1957A1966A1970&xztj=&mystock=" % page
        r = requests.get(url)
        if r.status_code == 200:
            contents = r.content.decode("utf-8")
        else:
            print "Fail at page: %s" % page
        root = etree.HTML(contents)
        trs = root.xpath('//tr')
        for tritem in trs:
            tdlist = tritem.xpath("./td")
            for tds in tdlist:
                ofile.write("%s," % tds.xpath("string(.)").encode("utf-8"))
            ofile.write("\n")
    ofile.close()

# plot
elif mytype == 2:
    file_dir = os.path.join("D:\Money\lilton_code\Market_Mode\learnModule\gsz", "")
    ya_dir = u'D:/Money/lilton_code/Market_Mode/learnModule/gsz/ya'
    gqdj_dir = u'D:/Money/lilton_code/Market_Mode/learnModule/gsz/gqdj'
    cqcx_dir = u'D:/Money/lilton_code/Market_Mode/learnModule/gsz/cqcx'
    if not os.path.exists(os.path.join(ya_dir, "")):
        os.mkdir(os.path.join(ya_dir, ""))

    if not os.path.exists(os.path.join(gqdj_dir, "")):
        os.mkdir(os.path.join(gqdj_dir, ""))

    if not os.path.exists(os.path.join(cqcx_dir, "")):
        os.mkdir(os.path.join(cqcx_dir, ""))

    csvframe = pd.read_csv(os.path.join(file_dir, "gsz.csv"), encoding='gbk')
    for i in csvframe.index.values:
        print i
        # if i != 270:
        #     continue
        try:
            ya_date = csvframe.loc[i, 'yaggr']
            cqcx_date = csvframe.loc[i, 'cqcxr']
            gqdj_date = csvframe.loc[i, 'gqdjr']

            ya_date = common.format_date(ya_date, "%Y%m%d")
            cqcx_date = common.format_date(cqcx_date, "%Y%m%d")
            gqdj_date = common.format_date(gqdj_date, "%Y%m%d")
            stockid = csvframe.loc[i, 'code']
            stockid = "0" * (6-len(str(stockid))) + str(stockid)
            name = csvframe.loc[i, 'stcname']
            szzs = csvframe.loc[i, 'szzs']

            detail_time = "10:00:00"
            direction = u'不买不卖'

            # 以预案公告日看结果
            tmp_array = [ya_date, detail_time, stockid, direction, "0"]
            study_plot(tmp_array, ya_dir, szzs, start_date_interval=40, end_date_interval=-40)

            # 以股权登记日看结果
            tmp_array = [gqdj_date, detail_time, stockid, direction,"0"]
            if gqdj_date == u'19890928':
                pass
            else:
                study_plot(tmp_array, gqdj_dir, szzs, start_date_interval=40, end_date_interval=-40)
            print stockid, "finished"
        except Exception, err:
            print err
        # break
