#coding:utf-8
import requests as rs
import time
import json
import logging
import pymongo

class CrawlYcjConcept:
    def __init__(self):
        mongourl = "localhost"
        self.mongodb = pymongo.MongoClient(mongourl)

    def crawl(self,conceptDict):
        topic = conceptDict['topic']
        name = conceptDict['name']
        url = "http://www.yuncaijing.com/Subject/concepts_stocks?id=%s"%topic
        count = 5
        while count > 0:
            r = rs.get(url)
            if r.status_code == 200:
                break
            logging.getLogger().error("retry to get %s"%url)
        if r.status_code != 200:
            raise Exception("can't reach %s"%url)

        contents = json.loads(r.content)
        self.parseJsons(contents, name, topic)

    def parseJsons(self,jsons, name, id):
        for cdits in jsons['data']['story_stk']:
            stockid = cdits['code']
            percent = cdits['persent']
            stockname = cdits['stkname']
            reason = cdits['relation_story']
            stock_source = u"云财经"
            stock_concept = name
            id = id
            self.insert_mongo(stockid, stockname, reason, stock_concept, stock_source, percent, id)

    def insert_mongo(self,stockid, stockname, reason, stock_concept, stock_source, percent, id):
        self.mongodb.stock.ycj_concept.update({"source": stock_source, "stockid": stockid, "id": id},
                                              {"$set":
                                                   {"stockname": stockname, "reason": reason, "concept":stock_concept, "percent": percent}}, True, True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(threadName)s Line:%(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.000',
                    filename='./logs/ycj_concept.log',
                    filemode='w')
    crawler = CrawlYcjConcept()
    for i in range(2297, 2298):
        print i
        if i == 432:
            continue
        conceptDict = {
            "topic": "%s"%i,
            "name": ""
        }
        try:
            crawler.crawl(conceptDict)
            logging.getLogger().info("crawl %s finished"%i)
        except Exception,err:
            logging.getLogger().error("Error out when crawl %s, err:%s"%(i, err))
        finally:
            time.sleep(2)