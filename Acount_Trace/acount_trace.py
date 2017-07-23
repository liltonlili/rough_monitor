#coding:utf-8
from selenium import webdriver
import time
from lxml import etree
import shutil
import datetime
import pandas as pd
import os
import numpy as np
import sys
sys.path.append("D:/Money/lilton_code/Market_Mode/history_zt")
import common


user = "540800231818"
passwd = "898989"
store_dir = u'D:/Money/lilton_code/Market_Mode/history_zt/Acount_Trace'


# bs = webdriver.Chrome("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe")
bs = webdriver.Ie()
bs.get("https://jy.xzsec.com/Search/Position")

try:
    elem=bs.find_element_by_id("txtZjzh")
    elem.send_keys(user)

    elem=bs.find_element_by_id("txtPwd")
    elem.send_keys(passwd)

    #弹框输入验证码
    common.showinfos(u'请输入验证码')

except:
    pass
#点击登录确认

# 拿到总资产页面的源码
main_page = bs.page_source

r = etree.HTML(main_page)

# 得到总资产
total_money = r.xpath('//div[@class="v_con hktrade pagetradeb Financial-position"]/div[2]/table/tbody/tr[1]/td[1]/span[2]/span/text()')[0]

# 得到当日参考盈亏
today_earn = r.xpath('//div[@class="v_con hktrade pagetradeb Financial-position"]/div[2]/table/tbody/tr[3]/td[2]/span[2]/span/text()')[0]
bs.close()

# 写入到本地记录中
today = datetime.datetime.today().strftime("%Y/%m/%d")

shutil.copy(os.path.join(store_dir, u'账户资金.csv'), os.path.join(store_dir, u'账户资金_bak.csv'))
dframe = pd.read_csv(os.path.join(store_dir, u'账户资金.csv'), encoding='gbk', index_col=0, dtype={u'日期':np.str})
index_v = int(max(dframe.index.values))+1
dframe.loc[index_v, u'日期'] = today
dframe.loc[index_v, u'账户总资金'] = total_money
dframe.loc[index_v, u'当日盈亏'] = today_earn
dframe.to_csv(os.path.join(store_dir, u'账户资金.csv'), encoding='gbk')
