#coding:utf8
"""
    @author: johny
    @version: 2010.9.14
"""

import urllib2
from HTMLParser import HTMLParser

# def GetStockInfo(stkcode):
#     url = 'http://qt.gtimg.cn/q=' + stkcode
#     req = urllib2.urlopen(url)
#     data = req.read()
#     result = data.split('~')
#
#     """
#     i = 0;
#     for v in result:
#         print "%s" % i, v
#         i += 1
#     """
#
#     # 代码 名称 价格 日涨跌 涨幅
#     return (result[2], result[1], result[3], result[31], result[32])
#
# def GetStockCode(stkindex):
#     url = 'http://stockhtm.finance.qq.com/sstock/ggcx/%s.shtml' % stkindex
#     #print url
#     req = urllib2.urlopen(url)
#     try:
#         parser = StockHtmlParser()
#         parser.feed(req.read())
#     except:
#         print 'exception'
#
#     if parser.data[1:5] == '深圳':
#         return 'sz%s' % stkindex
#     else:
#         return 'sh%s' % stkindex
#
# class StockHtmlParser(HTMLParser):
#     def handle_starttag(self, tag, attrs):
#         if tag == 'span' and attrs :
#             if attrs[0] == ('class', 'fs14'):
#                 self.bfindtag = True
#
#     def handle_data(self, data):
#         if self.bfindtag == True:
#             self.data = data
#             self.bfindtag = False
#
#     def handle_endtag(self, tag):
#         pass
#
#     bfindtag = False
#     data = ''


# import pymongo
# import datetime
#
# import pytz
# def utc_convert(dt_local):
#     try:
#         if type(dt_local) == str:
#             dt_local=datetime.datetime.strptime(dt_local,"%Y-%m-%d %X")
#         localzone = pytz.timezone('Asia/Shanghai')
#         local_dt = localzone.localize(dt_local,is_dst=None)
#         utc_dt = local_dt.astimezone (pytz.utc)
#         return utc_dt
#     except:
#         return None
#
# mongo_con = pymongo.MongoClient('localhost', 27017)
# # mongo_db = mongo_con.stock
# # mongo_col = mongo_db.testTTL
#
# timestamp = datetime.datetime.now()
# # utc_timestamp = datetime.datetime.utcnow()
# timestamp2 = utc_convert(timestamp)
#
# # mongo_con.stock.testTTL.ensure_index("date", expireAfterSeconds=5)
#
# mongo_con.stock.testTTL.insert({'_id': 'session2', "date": timestamp2, "session": "test session"})
# # mongo_con.stock.testTTL.insert({'_id': 'utc_session', "date": utc_timestamp, "session": "test session"})



# import matplotlib.pyplot as plt
#
# fig = plt.figure()
# ax1 = fig.add_subplot(221)
# print "abc"


# import pymongo
# mongoUrl = "localhost"
# mongodb = pymongo.MongoClient(mongoUrl)
# results = mongodb.stock.ZDT_by_date.find({"date":{"$gt":"20160401"},"openedFreshStocks":{"$exists":True}})
# openedResultsFile = open("./opendResults.csv","w")
# for result in results:
#     array = result['openedFreshStocks'].split("_")
#     print result['date']
#     if len(array)<1:
#         continue
#     openedResultsFile.write("%s,%s\n"%(result['date'],result['openedFreshStocks']))
#
# openedResultsFile.close()
# mongodb.close()


# # -*- coding: utf-8 -*-
# import time, sys
# # 判断python的版本然后import对应的模块
# if sys.version < '3':
#     from Tkinter import *
# else:
#     from tkinter import *
#
# mydelaymin = 3 #窗口提示的延迟时间，以分钟计
#
# #------------------def-------------------
# def showMessage():
#     #show reminder message window
#     root = Tk()  #建立根窗口
#     #root.minsize(500, 200)   #定义窗口的大小
#     #root.maxsize(1000, 400)  #不过定义窗口这个功能我没有使用
#     root.withdraw()  #hide window
#     #获取屏幕的宽度和高度，并且在高度上考虑到底部的任务栏，为了是弹出的窗口在屏幕中间
#     screenwidth = root.winfo_screenwidth()
#     screenheight = root.winfo_screenheight() - 100
#     root.resizable(False,False)
#     #添加组件
#     root.title("Warning!!")
#     frame = Frame(root, relief=RIDGE, borderwidth=3)
#     frame.pack(fill=BOTH, expand=1) #pack() 放置组件若没有则组件不会显示
#     #窗口显示的文字、并设置字体、字号
#     label = Label(frame, text="You have been working 30 minutes! Please have a break!!", \
#         font="Monotype\ Corsiva -20 bold")
#     label.pack(fill=BOTH, expand=1)
#     #按钮的设置
#     button = Button(frame, text="OK", font="Cooper -25 bold", fg="red", command=root.destroy)
#     button.pack(side=BOTTOM)
#
#     root.update_idletasks()
#     root.deiconify() #now the window size was calculated
#     root.withdraw() #hide the window again 防止窗口出现被拖动的感觉 具体原理未知？
#     root.geometry('%sx%s+%s+%s' % (root.winfo_width() + 10, root.winfo_height() + 10,
#         (screenwidth - root.winfo_width())/2, (screenheight - root.winfo_height())/2))
#     root.deiconify()
#     root.mainloop()
#
# #showMessage()
#
#
# while True:
#     time.sleep(mydelaymin) #参数为秒
#     showMessage()

TestMap = {1:0, 2:0, 3:1, 4:1}
for key in TestMap:
    TestMap.pop(key)