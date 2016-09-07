#coding:utf-8
import pymongo
from lxml import etree
from selenium import webdriver
import requests as rs
import re
import time


class CrawlJrjConcept:
    def __init__(self):
        mongourl = "localhost"
        self.mongodb = pymongo.MongoClient(mongourl)
        self.bs = webdriver.Chrome()

    def crawl(self,conceptDict):
        topic = conceptDict['topic']
        name = conceptDict['name']
        url = "http://stock.jrj.com.cn/concept/conceptdetail/conceptDetail_%s.shtml"%topic
        count = 5
        while count > 0:
            self.bs.get(url)
            if u'无法访问' not in self.bs.title:
                break
        tcount = 5
        while tcount > 0:
            try:
                texts = self.bs.find_element_by_id("stockTbody").text.split("\n")
                self.parseTexts(texts, name)
                break
            except Exception, err:
                print err
                tcount -= 1
                time.sleep(3)
                pass

    def parseTexts(self,txts, name):
        for txt in txts:
            if len(txt) < 20 and u'次' in txt:
                continue
            if u'-- --' in txt:
                useless = u'-- --'
            else:
                useless = re.search(re.compile(ur'((\d)+.\d\d (-|)(\d)+.\d\d%)'), txt).group(1)
            txt1 = txt.split(useless)[0]
            txt2 = txt.split(useless)[1]
            pattern1 = ur'(\d{6}) (.*)'
            info1 = re.search(re.compile(pattern1),txt1)
            stockid = info1.group(1)
            stockname = info1.group(2).replace(" ","")
            print stockname
            pattern2 = ur' --'
            reason = txt2.split("--")[0].replace(" ","")
            stock_concept = name
            stock_source = u"金融界"
            self.insert_mongo(stockid, stockname, reason, stock_concept, stock_source)

    def insert_mongo(self,stockid, stockname, reason, stock_concept, stock_source):
        self.mongodb.stock.jrj_concept.update({"source": "stock_source", "stockid": stockid, "concept": stock_concept},
                                              {"$set":
                                                   {"stockname": stockname, "reason": reason}}, True, True)

if __name__ == "__main__":
    crawler = CrawlJrjConcept()
    conceptDict = {
        "topic": "360syh",
        "name": u"360私有化"
    }
    try:
        crawler.crawl(conceptDict)
    except Exception,err:
        print err