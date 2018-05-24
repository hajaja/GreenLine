# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc, shutil
import sqlalchemy
import logging
import random

def keepATM(df, NStrikeATM=1):
    StockPrice = df['StockPrice'].values[0]
    listStrike = df['Strike'].unique()

    #diffMin = 10000
    #for Strike in listStrike:
    #    diff = abs(Strike - StockPrice)
    #    if diff < diffMin:
    #        diffMin = diff
    #        StrikeATM = Strike
    #df.loc[df['Strike'] == StrikeATM, 'ATM'] = True

    dfStrike = pd.DataFrame(listStrike)
    dfStrike.columns = ['Strike']
    dfStrike['Distance'] = (dfStrike['Strike'] - StockPrice).abs()
    dfStrike = dfStrike.set_index('Strike')
    listStrikeATM = dfStrike['Distance'].nsmallest(NStrikeATM).index.values
    df.loc[df['Strike'].isin(listStrikeATM), 'ATM'] = True
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


def findStranglePair(df, NStrikeStrangle=1):
    StockPrice = df['StockPrice'].values[0]
    listStrike = df['Strike'].unique()

    dfStrike = pd.DataFrame(listStrike)
    dfStrike.columns = ['Strike']
    dfStrike = dfStrike.set_index('Strike').sort_index()

    listDFByMiddle = []
    for StrikeMiddle in dfStrike.index[1:-1]:
        dfStrike['Distance'] = dfStrike.index - StrikeMiddle
        # find middle
        #StrikeMiddle = dfStrike['Distance'].abs().idxmin()
        dfStrikeExceptMiddle = dfStrike[dfStrike.index != StrikeMiddle]
    
        # left put
        dfStrikeLeft = dfStrikeExceptMiddle[dfStrikeExceptMiddle['Distance']<0]
        if dfStrikeLeft.index.size < NStrikeStrangle:
            StrikeLeft = None
        else:
            StrikeLeft = dfStrikeLeft.index[-(NStrikeStrangle)]
    
        # right call
        dfStrikeRight = dfStrikeExceptMiddle[dfStrikeExceptMiddle['Distance']>0]
        if dfStrikeRight.index.size < NStrikeStrangle:
            StrikeRight = None
        else:
            StrikeRight = dfStrikeRight.index[NStrikeStrangle-1]
    
        df['LeftRight'] = np.nan
        df['StrikeMiddle'] = np.nan
        if StrikeLeft is None or StrikeRight is None:
            pass
        else:
            df.loc[df['Strike'].isin([StrikeLeft, StrikeMiddle, StrikeRight]), 'StrikeMiddle'] = StrikeMiddle
            df.loc[df['Strike'] == StrikeMiddle, 'LeftRight'] = 'M'
            df.loc[(df['Strike'] == StrikeLeft)&(df['COP']==False), 'LeftRight'] = 'L'
            df.loc[(df['Strike'] == StrikeRight)&(df['COP']), 'LeftRight'] = 'R'
        dfThisMiddle = df[df['LeftRight'].notnull()].copy()
        listDFByMiddle.append(dfThisMiddle)

    dfRet = pd.concat(listDFByMiddle, axis=0)
    return dfRet

def funcFindStrangle(df):
    print df.name
    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'Strike']
    listColumnTradingC = ['call' + x for x in listColumnTrading]
    listColumnTradingP = ['put' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'SettleDate', 'StrikeMiddle', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle','ATM']
    sC = df[df['LeftRight']=='R'].iloc[0]
    sP = df[df['LeftRight']=='L'].iloc[0]
    sM = df[(df['LeftRight']=='M')&(df['COP'])].iloc[0]
    sCRename = sC[listColumnTrading]
    sCRename.index = listColumnTradingC
    sPRename = sP[listColumnTrading]
    sPRename.index = listColumnTradingP
    sConcat = sM[listColumnSingle].append(sCRename).append(sPRename)

    # calculate pair IV difference
    for NDayHist in [5,10,20]:
        sConcat['IVDiff_%d'%NDayHist] = sConcat['callIV'] + sConcat['putIV'] - sConcat['HVSettle_%d'%NDayHist] * 2
        sConcat['IVRatio_%d'%NDayHist] = (sConcat['callIV'] + sConcat['putIV']) / sConcat['HVSettle_%d'%NDayHist] * 2
        sConcat['ReturnExpected_%d'%NDayHist] = sConcat['IVDiff_%d'%NDayHist]/max(1,sConcat['NDayToSettle'])*365

    sConcat = sConcat.drop(['SettleDate', 'StrikeMiddle'])
    sConcat['PairName'] = '%s_%s'%(sConcat['callcode'], sConcat['putcode'])

    # 
    sConcat['volume'] = sConcat[['callvolume', 'putvolume']].min()
    return sConcat


def funcFindCalendar(df):
    #
    print df.name
    if len(df['code'].unique()) <= 1:
        return

    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'SettleDate']
    listColumnTradingC = ['near' + x for x in listColumnTrading]
    listColumnTradingP = ['deferred' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'Strike', 'COP', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle', 'ATM']

    df = df.sort_values(by='NDayToSettle')
    NContract = df.index.size
    listRow = []
    for nNear in range(0, NContract-1):
        for nDeferred in range(nNear+1, NContract):
            ixNear = df.index[nNear]
            ixDeferred = df.index[nDeferred]
            rowNear = df.loc[ixNear]
            rowDeferred = df.loc[ixDeferred]
    
            rowNearRename = rowNear[listColumnTrading]
            rowNearRename.index = listColumnTradingC
            rowDeferredRename = rowDeferred[listColumnTrading]
            rowDeferredRename.index = listColumnTradingP
            sConcat = rowNearRename.append(rowDeferredRename).append(rowNear[listColumnSingle])

            # calculate pair IV difference
            sConcat['IVDiff'] = sConcat['nearIV'] - sConcat['deferredIV']
            sConcat['ReturnExpected'] = sConcat['IVDiff'] / max(1,sConcat['NDayToSettle']) * 365
            for NDayHist in [5,10,20]:
                sConcat['IVRatio_%d'%NDayHist] = sConcat['IVDiff'] / sConcat['HVSettle_%d'%NDayHist]

            sConcat = sConcat.drop(['Strike', 'COP'])
            sConcat['PairName'] = '%s_%s'%(sConcat['nearcode'], sConcat['deferredcode'])
            
            # 
            sConcat['volume'] = sConcat[['nearvolume', 'deferredvolume']].min()
            listRow.append(sConcat)
    dfRet = pd.concat(listRow, axis=1)
    dfRet = dfRet.transpose()

    return dfRet


