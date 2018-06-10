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

    dfStrike = pd.DataFrame(listStrike)
    dfStrike.columns = ['Strike']
    dfStrike['Distance'] = (dfStrike['Strike'] - StockPrice).abs()
    dfStrike = dfStrike.set_index('Strike')
    listStrikeATM = dfStrike['Distance'].nsmallest(NStrikeATM).index.values
    df.loc[df['Strike'].isin(listStrikeATM), 'ATM'] = True
    return df

def findLMR(df, NStrikeStrangle=1):
    print df.name
    StockPrice = df['StockPrice'].values[0]
    listStrike = df['Strike'].unique()

    dfStrike = pd.DataFrame(listStrike)
    dfStrike.columns = ['Strike']
    dfStrike = dfStrike.set_index('Strike').sort_index()

    listDFByMiddle = []
    #for StrikeMiddle in dfStrike.index[1:-1]:
    for StrikeMiddle in dfStrike.index:
        dfStrike['Distance'] = dfStrike.index - StrikeMiddle
        # find middle
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
   
        # assign StrikeMiddle
        df['StrikeMiddle'] = np.nan
        df.loc[df['Strike'].isin([StrikeLeft, StrikeMiddle, StrikeRight]), 'StrikeMiddle'] = StrikeMiddle
        
        # assign LeftRight
        df['LeftRight'] = np.nan
        df.loc[df['Strike'] == StrikeMiddle, 'LeftRight'] = 'M'
        if StrikeLeft is not None:
            df.loc[df['Strike'] == StrikeLeft, 'LeftRight'] = 'L'
        if StrikeRight is not None:
            df.loc[df['Strike'] == StrikeRight, 'LeftRight'] = 'R'

        dfThisMiddle = df[df['LeftRight'].notnull()].copy()
        listDFByMiddle.append(dfThisMiddle)

    dfRet = pd.concat(listDFByMiddle, axis=0)
    dfRet = dfRet.drop(['SettleDate', 'trade_date'], axis=1)
    return dfRet

"""
def funcFindStrangle(df):
    if df['LeftRight'].unique().size < 3:
        return
    print df.name

    # find call and put
    sC = df[(df['LeftRight']=='R')&(df['COP'])].iloc[0]
    sP = df[(df['LeftRight']=='L')&(df['COP']==False)].iloc[0]
    sM = df[(df['LeftRight']=='M')&(df['COP'])].iloc[0]

    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'Strike']
    listColumnTradingC = ['call' + x for x in listColumnTrading]
    listColumnTradingP = ['put' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'SettleDate', 'StrikeMiddle', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle','ATM']
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
    sConcat['volume'] = sConcat[['callvolume', 'putvolume']].min()

    return sConcat
"""

def funcFindStraddle(df, strStrategy):
    if df['LeftRight'].unique().size < 3:
        return
    print df.name

    # find call and put
    if strStrategy == 'OptionStrangle':
        sC = df[(df['LeftRight']=='R')&(df['COP'])].iloc[0]
        sP = df[(df['LeftRight']=='L')&(df['COP']==False)].iloc[0]
        sM = df[(df['LeftRight']=='M')&(df['COP'])].iloc[0]
    elif strStrategy == 'OptionStraddle':
        sC = df[(df['LeftRight']=='M')&(df['COP'])].iloc[0]
        sP = df[(df['LeftRight']=='M')&(df['COP']==False)].iloc[0]
        sM = sC

    # cast
    listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'Strike']
    listColumnTradingC = ['call' + x for x in listColumnTrading]
    listColumnTradingP = ['put' + x for x in listColumnTrading]
    listColumnSingle = ['StockPrice', 'SettleDate', 'StrikeMiddle', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle','ATM']
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
    sConcat['volume'] = sConcat[['callvolume', 'putvolume']].min()

    return sConcat

def funcFindButterfly(df, strStrategy='Butterfly'):
    if df['LeftRight'].unique().size < 3:
        return
    print df.name

    listS = []
    for COP in [True, False]:
        # find call and put
        sL = df[(df['LeftRight']=='L')&(df['COP']==COP)].iloc[0]
        sR = df[(df['LeftRight']=='R')&(df['COP']==COP)].iloc[0]
        sM = df[(df['LeftRight']=='M')&(df['COP']==COP)].iloc[0]

        # cast
        listColumnTrading = ['code', 'open', 'high', 'low', 'close', 'settle', 'volume', 'openInterest', 'turnover', 'presettle', 'IV', 'Strike']
        listColumnTradingL = ['left' + x for x in listColumnTrading]
        listColumnTradingR = ['right' + x for x in listColumnTrading]
        listColumnTradingM = ['middle' + x for x in listColumnTrading]
        listColumnSingle = ['StockPrice', 'SettleDate', 'StrikeMiddle', 'HVSettle_5', 'HVSettle_10', 'HVSettle_20', 'NDayToSettle','ATM']

        sLRename = sL[listColumnTrading]
        sLRename.index = listColumnTradingL
        sRRename = sR[listColumnTrading]
        sRRename.index = listColumnTradingR
        sMRename = sM[listColumnTrading]
        sMRename.index = listColumnTradingM
        sConcat = sM[listColumnSingle].append(sLRename).append(sRRename).append(sMRename)
    
        # calculate pair IV difference
        sConcat['IVDiff'] = sConcat['leftIV'] + sConcat['rightIV'] - sConcat['middleIV'] * 2
        sConcat['ReturnExpected'] = sConcat['IVDiff']/max(1,sConcat['NDayToSettle'])*365
        sConcat = sConcat.drop(['SettleDate', 'StrikeMiddle'])
        sConcat['PairName'] = '%s_%s'%(sConcat['leftcode'], sConcat['rightcode'])
        sConcat['volume'] = sConcat[['leftvolume', 'middle', 'rightvolume']].min()
        sConcat['COP'] = COP
        listS.append(sConcat)

    dfRet = pd.concat(listS, axis=1)
    dfRet = dfRet.transpose()
    return dfRet

