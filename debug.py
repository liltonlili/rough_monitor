#coding:utf8
"""
    @author: johny
    @version: 2010.9.14
"""

import urllib2
from HTMLParser import HTMLParser

def GetStockInfo(stkcode):
    url = 'http://qt.gtimg.cn/q=' + stkcode
    req = urllib2.urlopen(url)
    data = req.read()
    result = data.split('~')

    """
    i = 0;
    for v in result:
        print "%s" % i, v
        i += 1
    """

    # 代码 名称 价格 日涨跌 涨幅
    return (result[2], result[1], result[3], result[31], result[32])

def GetStockCode(stkindex):
    url = 'http://stockhtm.finance.qq.com/sstock/ggcx/%s.shtml' % stkindex
    #print url
    req = urllib2.urlopen(url)
    try:
        parser = StockHtmlParser()
        parser.feed(req.read())
    except:
        print 'exception'

    if parser.data[1:5] == '深圳':
        return 'sz%s' % stkindex
    else:
        return 'sh%s' % stkindex

class StockHtmlParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == 'span' and attrs :
            if attrs[0] == ('class', 'fs14'):
                self.bfindtag = True

    def handle_data(self, data):
        if self.bfindtag == True:
            self.data = data
            self.bfindtag = False

    def handle_endtag(self, tag):
        pass

    bfindtag = False
    data = ''