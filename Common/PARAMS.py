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

#---- option
dirDataOption = '%s/%s/'%(dirDataSource, 'Option')
if os.path.exists(dirDataOption) is False:
    os.mkdir(dirDataOption)



