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



import matplotlib.pyplot as plt

fig = plt.figure()
ax1 = fig.add_subplot(221)
print "abc"
