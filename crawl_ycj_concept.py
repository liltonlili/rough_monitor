#coding:utf-8
import requests as rs
import time
import json
import logging
import pymongo
from lxml import etree

headers = {
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding":"gzip, deflate",
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Host":"www.yuncaijing.com",
        "Origin":"http://www.yuncaijing.com",
        "Referer":"http://www.yuncaijing.com/quote/sz002436.html",
        "X-Requested-With":"XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0"
    }


class CrawlYcjConcept:
    def __init__(self):
        mongourl = "localhost"
        self.mongodb = pymongo.MongoClient(mongourl)

    def crawl(self,conceptDict):
        topicid = conceptDict['topic']
        name = conceptDict['name']
        url = "http://www.yuncaijing.com/story/details/id_%s.html"%topicid


        proxies = {'http':'http://10.20.205.162:1080'}

        count = 5
        while count > 0:
            r = rs.get(url = url, headers = headers, proxies=proxies)
            if r.status_code == 200:
                break
            logging.getLogger().error("retry to get %s"%url)
        if r.status_code != 200:
            raise Exception("can't reach %s"%url)
        ycj_stock_list = self.parse_html(r.content, name, topicid)
        # 查询数据库中已有的stock_list，并进行动态更新



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

    def parse_html(self, htmlcontent, name, id):
        root = etree.HTML(htmlcontent)
        stock_list = []
        for x in root.xpath('//table[@class="table table-ycj table-hover today"]/tbody/tr/@data-code'):
            stock_list.append(x)
        return stock_list

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
    for i in range(2505, 2506):
        print i
        if i == 432:
            continue
        conceptDict = {
            "topicid": "%s" % i,
            "name": ""
        }
        try:
            crawler.crawl(conceptDict)
            logging.getLogger().info("crawl %s finished" % i)
        except Exception, err:
            logging.getLogger().error("Error out when crawl %s, err:%s" % (i, err))
        finally:
            time.sleep(2)