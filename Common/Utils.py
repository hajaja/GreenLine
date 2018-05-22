# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
import os,re,time,gc,sys,datetime
import logging
import PARAMS
reload(PARAMS)

#########################
# DataBase selection 
#########################
if PARAMS.strDBUsed == 'MySQLDB':
    import UtilsMySQL as UtilsDB
    reload(UtilsDB)
else:
    import UtilsMongoDB as UtilsDB
    reload(UtilsDB)

#########################
# function
#########################
#------- get Exe & OHLCV data
def getTradingData(dictDataSpec):
    dfTrading = UtilsDB.readTradingData(dictDataSpec)
    return dfTrading

def keepOperation(s):
    s = s.ffill().fillna(0)
    indexNonOperation = s.diff()==0
    s[indexNonOperation] = np.nan
    s.iloc[0] = np.nan
    return s


