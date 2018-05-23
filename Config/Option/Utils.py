# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc, shutil
import sqlalchemy
import logging
import random

def keepATM(df):
    StockPrice = df['StockPrice'].values[0]
    listStrike = df['Strike'].unique()

    diffMin = 10000
    for Strike in listStrike:
        diff = abs(Strike - StockPrice)
        if diff < diffMin:
            diffMin = diff
            StrikeATM = Strike
    #dfATM = df[df['Strike'] == StrikeATM]
    df.loc[df['Strike'] == StrikeATM, 'ATM'] = True
    #df = df.drop(['SettleDate', 'trade_date'], axis=1)
    return df

def funcFindStraddle(df):
    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV']
    listColumnTradingC = ['call' + x for x in listColumnTrading]
    listColumnTradingP = ['put' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'SettleDate', 'Strike', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle','ATM']
    dfC = df[df['COP']]
    dfP = df[df['COP']==False]
    dfCRename = dfC[listColumnTrading]
    dfCRename.columns = listColumnTradingC
    dfPRename = dfP[listColumnTrading]
    dfPRename.columns = listColumnTradingP
    dfConcat = pd.concat([dfC[listColumnSingle], dfCRename, dfPRename], axis=1)

    # calculate pair IV difference
    for NDayHist in [5,10,20]:
        dfConcat['IVDiff_%d'%NDayHist] = dfConcat['callIV'] + dfConcat['putIV'] - dfConcat['HVSettle_%d'%NDayHist] * 2
        dfConcat['IVRatio_%d'%NDayHist] = (dfConcat['callIV'] + dfConcat['putIV']) / dfConcat['HVSettle_%d'%NDayHist] * 2
        dfConcat['ReturnExpected_%d'%NDayHist] = dfConcat.apply(lambda row: row['IVDiff_%d'%NDayHist]/max(1,row['NDayToSettle'])*365, axis=1)

    dfConcat = dfConcat.drop(['SettleDate', 'Strike'], axis=1)
    dfConcat['PairName'] = dfConcat.apply(lambda row: '%s_%s'%(row['callcode'],row['putcode']), axis=1)

    # 
    dfConcat['volume'] = dfConcat[['callvolume', 'putvolume']].min(axis=1)
    return dfConcat

def funcFindCalendar(df):
    #
    if len(df['code'].unique()) <= 1:
        return

    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'SettleDate', 'ATM']
    listColumnTradingC = ['near' + x for x in listColumnTrading]
    listColumnTradingP = ['deferred' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'Strike', 'COP', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle']

    df = df.sort_values(by='NDayToSettle')
    dfC = df.iloc[0]
    dfP = df.iloc[1]

    dfCRename = dfC[listColumnTrading]
    dfCRename.index = listColumnTradingC
    dfPRename = dfP[listColumnTrading]
    dfPRename.index = listColumnTradingP
    sConcat = dfCRename.append(dfPRename).append(dfC[listColumnSingle])

    # calculate pair IV difference
    sConcat['IVDiff'] = sConcat['nearIV'] - sConcat['deferredIV']
    sConcat['ReturnExpected'] = sConcat['IVDiff'] / sConcat['NDayToSettle'] * 365
    for NDayHist in [5,10,20]:
        sConcat['IVRatio_%d'%NDayHist] = sConcat['IVDiff'] / sConcat['HVSettle_%d'%NDayHist]

    sConcat = sConcat.drop(['Strike', 'COP'])
    sConcat['PairName'] = '%d_%d'%(int(sConcat['nearcode']), int(sConcat['deferredcode']))
    return sConcat

