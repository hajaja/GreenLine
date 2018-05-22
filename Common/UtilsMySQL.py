# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
import os,re,time,gc,sys,datetime
import logging

import StockDataBase as SDB
reload(SDB)

from RoyalMountain.DataBase import MySQLDBAPI
reload(MySQLDBAPI)

# table name
strTB_TEMP = 'TEMP'
DB_NAME = 'stock'   # same as in StockDataBase.DataReader

DB_NAME_PERFORMANCE = 'DailyPerformanceCS'
DB_NAME_POSITION = 'DailyPositionCS'

TB_NAME_AQR = 'AQR'
TB_NAME_HF = 'HF'
    
import RoyalMountain
reload(RoyalMountain)
dfTradingDay = RoyalMountain.TradingDay.getTradingDayEquity()

#-------------- read database
def readTradingData(dictOption):
    d = {}
    d['SecuCode'] = dictOption['SecuCode']
    d['strDTStart'] = dictOption['dtStart'].strftime('%Y%m%d')
    sql = 'select TradingDay,SecuCode,Open,High,Low,Close,TurnoverVolume,TurnoverRatio,ExeOpen,ExeClose,PB,PE,NonRestrictedCap from FactorDaily where TradingDay>="{d[strDTStart]}" and SecuCode="{d[SecuCode]}";'.format(d=d)
    con = MySQLDBAPI.connect(DB_NAME)
    df = pd.read_sql(sql, con)
    df = df[df['Close']>0.0]
    df = df[df['TurnoverVolume']>0]
    for strFactor in df.columns.intersection(['PE', 'PE3', 'PE5']):
        df.ix[df[strFactor]<0, strFactor] = 10000
    con.dispose()
    df['ExeOpen'] = df['ExeClose'] / df['Close'] * df['Open']
    df = df[df['TradingDay'].isin(dfTradingDay.index)]
    
    return df

#------ read index constituent
def readIndexConstituent(IndexCode, dtEnd=datetime.datetime.now()):
    dtStart = dtEnd - datetime.timedelta(31 * 5 + 20, 1)
    strTB = SDB.Utils.strTB_IndexConstituent
    con = MySQLDBAPI.connect(DB_NAME)
    dtEnd = max(dtEnd, datetime.datetime(2007,1,31))
    sql = 'SELECT IndexCode,TradingDay,SecuCode,Weights FROM {0} where IndexCode="{1}" and TradingDay>="{2}" and TradingDay<="{3}"'.format(strTB, IndexCode, dtStart.strftime('%Y%m%d'), dtEnd.strftime('%Y%m%d'))
    df = pd.read_sql(sql, con)
    con.dispose()
    return df
