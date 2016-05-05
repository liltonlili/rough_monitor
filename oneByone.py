#coding:utf8
import pymongo
import pandas as pd
import numpy as np
from pandas import DataFrame,Series
import time


class field():
    def __init__(self):
        mongoUrl = "localhost"
        self.mongodb = pymongo.MongoClient(mongoUrl)
        self.query = self.mongodb.stock.concept_record.find_one({})
        if self.query is None:
            print "Error: No record found!"
            raise Exception("no record found")

    def showFieldList(self):
        for key in self.query:
            if key != "_id":
                print key,len(self.query[key])

    def queryField(self,fieldName):
        targets = self.query[fieldName]
        print "%s\n%s"%(fieldName,targets)

if __name__ == "__main__":
    monitor = field()
    monitor.showFieldList()