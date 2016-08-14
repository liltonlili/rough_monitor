#coding:utf8
import pymongo
import pandas as pd
import numpy as np
from pandas import DataFrame,Series
import time
import common


class field():
    def __init__(self):
        mongoUrl = "localhost"
        self.mongodb = pymongo.MongoClient(mongoUrl)
        self.query = self.mongodb.stock.concept_record.find_one({})
        if self.query is None:
            print "Error: No record found!"
            raise Exception("no record found")

    # show所有的板块名字，以及对于的股票个数
    def showField(self):
        print "## 主题，股票个数 ##"
        print "#####################"
        for key in self.query:
            if key != "_id":
                print key,len(self.query[key])
        print "####################\n"

    # 根据板块名查找对应的股票名
    def queryField(self,fieldName):
        print "######## look up #########################################################"
        targets = self.query[fieldName]
        names = [(common.QueryStockMap(id=x))[0] for x in targets]
        names=" ".join(names)
        print "%s\n%s"%(fieldName,names)
        print targets
        print "#########################################################################\n"

    def insertStock(self,stockidList="",fieldName = ""):
        stockidList=["0"*(6-len(str(x)))+str(x) for x in stockidList]
        if fieldName in self.query:
            valueList = self.query[fieldName]
            newStock = [x for x in stockidList if x not in valueList]
            valueList.extend(newStock)
            targets= {fieldName:valueList}
            self.mongodb.stock.concept_record.update({fieldName:{"$exists":True}},{"$set":targets})
            print "Info: %s added in %s"%(newStock,fieldName)
        else:
            targets = {fieldName:stockidList}
            self.mongodb.stock.concept_record.update({},{"$set":targets},True)
            print "Info: add a field of %s, add elements %s"%(fieldName,stockidList)


if __name__ == "__main__":
    monitor = field()
    # monitor.showField()
    # monitor.queryField(u"猪肉")
    monitor.insertStock(['300368'],u"区块链")
    monitor.queryField(u"区块链")

    # print common.QueryStockMap(name="酒鬼酒")