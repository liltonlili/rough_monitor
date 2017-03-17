#!/usr/bin/python
# -*- coding:UTF-8 -*-
from os import path
from scipy.misc import imread
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import os
import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
#
# d = path.dirname(__file__)
#
# # Read the wholedd text.
# # text = open(path.join(d, 'alice_result.txt')).read()
#
# # read the mask / color image
# # taken from http://jirkavinse.deviantart.com/art/quot-Real-Life-quot-Alice-282261010
# alice_coloring = imread(path.join(d, u"轿车.jpg"))
#
# wc = WordCloud(font_path = os.path.join(u'C:/Windows/Fonts','wqy-microhei.ttc'), background_color="white", max_words=2000000, mask=alice_coloring,
#                stopwords=STOPWORDS.add("said"),
#                max_font_size=50, random_state=42)
# # generate word cloud
# # wc.generate(text)
# with open(os.path.join(u'D:/projects/bussiness/car/深度报告', u'轿车.txt'), 'rb') as fHandler:
#     text_dict = eval(fHandler.read())
# print "text_dict"
# wc.generate_from_frequencies(text_dict)
# # create coloring from image
# image_colors = ImageColorGenerator(alice_coloring)
#
# # show
# plt.imshow(wc)
# plt.axis("off")
# plt.figure()
# # recolor wordcloud and show
# # we could also give color_func=image_colors directly in the constructor
# plt.imshow(wc.recolor(color_func=image_colors))
# plt.axis("off")
# plt.figure()
# plt.imshow(alice_coloring, cmap=plt.cm.gray)
# plt.axis("off")
# plt.show()
# #save img
# wc.to_file(path.join(d, "cloudimg.png"))
import common
import requests as rs

dday = "20170227"
histdate_frame = common.get_mysqlData([], [dday])
histdate_frame['rate'] = 100*(histdate_frame['CLOSE_PRICE'] - histdate_frame['ACT_PRE_CLOSE_PRICE'])/histdate_frame['ACT_PRE_CLOSE_PRICE']
histdate_frame['rate'] = histdate_frame['rate'].round(2)
# print common.find_concept("000017", "20161223")[0]
print histdate_frame.head(5)