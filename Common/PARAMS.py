# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
import os,re,time,gc,sys,datetime
import logging
import RoyalMountain.Dialect as Dialect
reload(Dialect)

#########################
# parameters
#########################
strDBUsed = 'MySQLDB'
strDataSource = Dialect.dictCDB['strDataSource']
boolUpdate = True
boolClear = True
dtNow = datetime.datetime.now()
TOTAL_MONENY = 10e6

#----- stock
COMMISSION = 5e-3
dictNTotalSecurity = {'000016':25, '000300':100, '000905':200, '50ETF': 5, 'sr.czc':5, 'm.dce':5}

#########################
# directory
#########################
strDirRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dirDataSource = strDirRoot + '/Data/'
if os.path.exists(dirDataSource) is False:
    os.mkdir(dirDataSource)

#---- stock
dirDataSingleStock = '%s/%s/'%(dirDataSource, 'SingleStock')
if os.path.exists(dirDataSingleStock) is False:
    os.mkdir(dirDataSingleStock)

#---- option, straddle
dirDataOptionStraddle = '%s/%s/'%(dirDataSource, 'OptionStraddle')
if os.path.exists(dirDataOptionStraddle) is False:
    os.mkdir(dirDataOptionStraddle)

#---- option, strangle
dirDataOptionStrangle = '%s/%s/'%(dirDataSource, 'OptionStrangle')
if os.path.exists(dirDataOptionStrangle) is False:
    os.mkdir(dirDataOptionStrangle)

#---- option, calendar
dirDataOptionCalendar = '%s/%s/'%(dirDataSource, 'OptionCalendar')
if os.path.exists(dirDataOptionCalendar) is False:
    os.mkdir(dirDataOptionCalendar)

#---- cache
dirDataCache = '%s/%s/'%(dirDataSource, 'Cache')
if os.path.exists(dirDataCache) is False:
    os.mkdir(dirDataCache)





