#coding:utf-8
import numpy as np  
import argparse  
import cv2
import common
# image = cv2.imread('huang.png')
# color = [
#     ([0, 70, 70], [100, 255, 255])#黄色范围~这个是我自己试验的范围，可根据实际情况自行调整~注意：数值按[b,g,r]排布
# ]
# #如果color中定义了几种颜色区间，都可以分割出来
# for (lower, upper) in color:
#     # 创建NumPy数组
#     lower = np.array(lower, dtype = "uint8")#颜色下限
#     upper = np.array(upper, dtype = "uint8")#颜色上限
#
#     # 根据阈值找到对应颜色
#     mask = cv2.inRange(image, lower, upper)
#     output = cv2.bitwise_and(image, image, mask = mask)
#
#     # 展示图片
#     cv2.imshow("images", np.hstack([image, output]))
#     cv2.waitKey(0)

# print common.get_latest_news("002436")

#coding:utf-8
# import matplotlib.pyplot as plt
# import pandas as pd
# import matplotlib
# import numpy as np
# from matplotlib.font_manager import FontProperties
# import os
# import matplotlib as mpl
# from matplotlib import *
#
# import matplotlib.dates as mdates
# myFmt = mdates.DateFormatter('%Y-%m-%d')
# myfont = matplotlib.font_manager.FontProperties(fname=os.path.join(u'C:/Users/li.li/Downloads/医药数据分析代码','wqy-microhei.ttc'))
# mpl.rcParams['axes.unicode_minus'] = False


# def plot_text(ax, texts, n, fontsize = 20):
# # fig = plt.figure()
# # ax1 = fig.add_subplot(111)
# # n = 3
# # texts = [u'这个是测试样例1', u'这个是测试样例2', u'这个是测试样例3']
#     t_list = []
#     for i in range(n):
#         s_list = pd.Series([i+1]*6)
#         t_list.append(s_list)
#     fframe = pd.concat(t_list, axis=1)
#
#     i = 0
#     for col in fframe.columns:
#         ax.plot(fframe.index.values, fframe[col], 'w')
#         ax.annotate(texts[i], xy=(1, i+1), fontproperties=myfont, fontsize = 20)
#         i += 1
#
# # ax1.annotate(['a','b'],())
# plt.show()
# print fframe
import matplotlib.pyplot as plt
import common
# fig = plt.figure()
# ax1 = fig.add_subplot(111)
# n = 3
# texts = [u'这个是测试样例1', u'这个是测试样例2', u'这个是测试样例3']
# common.plot_text(ax1, texts, n)
# plt.show()

# import datetime
# import LearnFrom
# import trace_yesterday
#
# trace_yesterday.generate_fp_pic("002468", "./", "")
# stockid = '002468'
# fig = plt.figure()
# # 画个股分时图
# ax1 = fig.add_subplot(221)
# endDate = "20161027"
# stock_dv = common.get_minly_frame(stockid, endDate, id_type =1)
# yesterday = common.get_lastN_date(endDate, 1)
# print yesterday
# pre_close = common.get_mysqlData([stockid],[yesterday]).loc[0,'CLOSE_PRICE']
# print pre_close
# LearnFrom.plot_dealDetail(stock_dv, ax1, rotation=30, fontsize=5, mount_flag=1, pre_close=pre_close)
# ax1.grid(True)
# # 画上证分时图
# ax2 = fig.add_subplot(223)
# sh_dv = common.get_minly_frame(stockid, endDate, id_type =0)
# LearnFrom.plot_dealDetail(sh_dv, ax2, rotation=30, fontsize=5, mount_flag=1, pre_close=pre_close)
# ax2.grid(True)
# plt.show()

import common
import tushare as ts
mysqls = common.mysqldata()