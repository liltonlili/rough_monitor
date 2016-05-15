# coding:utf8
import pymongo

##定期和生产的数据库进行同步

server_mongo = "mongodb://dpipe:dpipe@nosql04-dev.datayes.com"
MongoServer = pymongo.MongoClient(server_mongo)

local_mongo = "localhost"
LocalServer = pymongo.MongoClient(local_mongo)

## begin to synchronize
stores = MongoServer.reports_db.stock.find({})
count = 0
for store in stores:
    exists = LocalServer.stock.stockmap.find_one({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")})
    if exists is None:
        store['stockid'] = store['stockid'].replace("SH","").replace("SZ","").replace("CN","")
        LocalServer.stock.stockmap.update({"stock_name":store['stock_name'],"stockid":store['stockid'].replace("SH","").replace("SZ","").replace("CN","")},
                                          {"$set":store},True)
        count += 1
        print "Add %s,%s"%(store['stockid'],store['stock_name'])
print "%s stock map is added in total"%count