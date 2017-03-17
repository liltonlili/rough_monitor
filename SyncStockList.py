# coding:utf8
import pymongo
import requests as rs
from lxml import etree
import datetime


local_mongo = "localhost"
local_server = pymongo.MongoClient(local_mongo)

# 方法一: 从东财网站上把数据同步过来
base_url = "http://quote.eastmoney.com/stocklist.html"
res = rs.get(base_url)
detail_time = datetime.datetime.now().strftime("%Y-%m-%d %X")
if res.status_code != 200:
    print "sync stocklist from east money failed, response code:%s" %res.status_code
else:
    content = res.content.decode("GBK")
    root = etree.HTML(content)
    target_list = root.xpath("/html/body/div[@class='qox']/div[@class='quotebody']/div[@id='quotesearch']//ul//li/a/text()")
    for target in target_list:
        split_list = target.split(u"(")
        stockname = split_list[0]
        stockid = split_list[1].replace(u')', u"")
        local_server.stock.stockmap.update({"stock_name":stockname,"stockid":stockid},
                                              {"$set":{"stock_name":stockname, "stockid":stockid, "updatetime": detail_time}}, True, True, True)
        result = local_server.stock.stockmap.find({"stockid":"600036"})



# 方法二： 定期和生产的数据库进行同步， 数据库那边数据质量不高，暂时不用
# server_mongo = "mongodb://app_reports_db_rw:OxXtSsFeZX0JZEZk@mongodb01-dbp.datayes.com,mongodb02-dbp.datayes.com,mongodb03-dbp.datayes.com/reports_db?readPreference=secondaryPreferred"
# MongoServer = pymongo.MongoClient(server_mongo)
#
# ## begin to synchronize
# stores = MongoServer.reports_db.stock.find({})
# count = 0
# for store in stores:
#     if store['stockid'] in [u'SZ000006CN', u'SZ000019CN']:
#         continue
#     exists = LocalServer.stock.stockmap.find_one({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")})
#     if exists is None:
#         try:
#             store['stockid'] = store['stockid'].replace("SH","").replace("SZ","").replace("CN","")
#             LocalServer.stock.stockmap.update({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")},
#                                               {"$set":store},True, True)
#             count += 1
#             print "Add %s,%s"%(store['stockid'],store['stock_name'])
#         except Exception, err:
#             print "skip update stock map, stockname:%s, stockid:%s" % (store['stock_name'], store['stockid'])
# print "%s stock map is added in total" % count