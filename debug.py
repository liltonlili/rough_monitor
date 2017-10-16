# -*- coding: utf-8 -*-
import numpy as np
import pylab as pl

fig = pl.figure()
x = np.arange(0, 2*np.pi, 0.01)
pl.plot(x, np.sin(x))
pl.xlabel(u'角度')
pl.ylabel(u'幅值')
pl.title(u'正弦波')

fig.axes[0].grid(True)#打开网格
pl.ylim(-1.2,1.2)#设置y轴范围限制

pl.show()