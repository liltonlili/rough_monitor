# coding:utf8
import pymongo

##定期和生产的数据库进行同步

server_mongo = "mongodb://app_reports_db_rw:OxXtSsFeZX0JZEZk@mongodb01-dbp.datayes.com,mongodb02-dbp.datayes.com,mongodb03-dbp.datayes.com/reports_db?readPreference=secondaryPreferred"
MongoServer = pymongo.MongoClient(server_mongo)

local_mongo = "localhost"
LocalServer = pymongo.MongoClient(local_mongo)

## begin to synchronize
stores = MongoServer.reports_db.stock.find({})
count = 0
for store in stores:
    if store['stockid'] in [u'SZ000006CN', u'SZ000019CN']:
        continue
    exists = LocalServer.stock.stockmap.find_one({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")})
    if exists is None:
        try:
            store['stockid'] = store['stockid'].replace("SH","").replace("SZ","").replace("CN","")
            LocalServer.stock.stockmap.update({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")},
                                              {"$set":store},True, True)
            count += 1
            print "Add %s,%s"%(store['stockid'],store['stock_name'])
        except Exception, err:
            print "skip update stock map, stockname:%s, stockid:%s" % (store['stock_name'], store['stockid'])
print "%s stock map is added in total"%count