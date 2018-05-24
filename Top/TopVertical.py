# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc, shutil
import sqlalchemy
import logging
import random

import GreenLine.Config.Option.UtilsVertical as Utils
reload(Utils)

import GreenLine.Common.PARAMS as PARAMS
reload(PARAMS)
import OptionDataBase as ODB
reload(ODB)

# param
direction = 'S'
if direction == 'B':
    NDayEnterMin = 40
    NDayExitMax = 20
    ReturnAnnualized = 0.1
    ReturnOnce = 0.25
    ReturnOnceExit = 0.15
elif direction == 'S':
    NDayEnterMin = 10
    NDayExitMax = 5
    ReturnAnnualized = 0.4
    ReturnOnce = 0.15
    ReturnOnceExit = 0.1
VolumeMin = 500
strIVDiff = 'IVDiff_5'
strReturnAnnualized = 'ReturnExpected_20'

boolUpdate = False
strProduct = '50ETF'
strStrategy = 'Straddle'

if boolUpdate:
    strAddressIV = ODB.Utils.strAddressIVTemplateLatest%strProduct
else:
    strAddressIV = ODB.Utils.strAddressIVTemplateHist%strProduct

# read data from ODB
strFileAddressTemp = '%s/%s_%s_%s.pickle'%(PARAMS.dirDataCache, strStrategy, strProduct, str(boolUpdate))
if os.path.exists(strFileAddressTemp):
    dfStraddle = pd.read_pickle(strFileAddressTemp)
else:
    # read data from ODB
    df = pd.read_excel(strAddressIV)
    df = df[df['StockPrice'].notnull()]
    if strProduct == '50ETF':
        df['code'] = df['code'].apply(lambda x: str(int(x)))
    
    # only keep ATM pairs
    df['ATM'] = False
    df = df.groupby(['SettleDate', 'trade_date']).apply(Utils.keepATM)
    
    # straddle or strangle
    df = df.groupby(['SettleDate', 'trade_date']).apply(Utils.findLMR)
    df = df[df['LeftRight'].notnull()]
    gg = df.reset_index().groupby(['SettleDate', 'StrikeMiddle', 'trade_date'])
    dfStraddle = gg.apply(Utils.funcFindStraddle, strStrategy)
    dfStraddle.to_pickle(strFileAddressTemp)


# when to sell straddle
if direction == 'B':
    dfOppo = dfStraddle[(dfStraddle[strIVDiff]<-ReturnOnce)&(dfStraddle[strReturnAnnualized]<-ReturnAnnualized)]
elif direction == 'S':
    dfOppo = dfStraddle[(dfStraddle[strIVDiff]>ReturnOnce)&(dfStraddle[strReturnAnnualized]>ReturnAnnualized)]
dfOppo['ATM'] = dfOppo['ATM'].astype(bool)
if strProduct == '50ETF':
    dfOppo['callcode'] = dfOppo['callcode'].astype(int)
    dfOppo['putcode'] = dfOppo['putcode'].astype(int)
dfOppo = dfOppo[dfOppo['ATM']&(dfOppo['NDayToSettle']>NDayEnterMin)&(dfOppo['volume']>VolumeMin)]
print dfOppo.shape

# generate pseudo secu for port value calculation
listDF = []
for nOppo in range(0, dfOppo.index.size):
    ix = dfOppo.index[nOppo]
    row = dfOppo.iloc[nOppo]
    SettleDate = ix[0]
    Strike = ix[1]
    trade_date = ix[2]
    print trade_date, nOppo
    # read two contracts
    callcode = str(row['callcode'])
    putcode = str(row['putcode'])
    listContractCode = [callcode, putcode]
    dtStart = trade_date
    dfOption = ODB.Utils.UtilsDB.readDB(ODB.Utils.UtilsDB.strDB, ODB.Utils.UtilsDB.DAILY_DB_NAME, dtStart-datetime.timedelta(20, 0), listContractCode)
    dfCall = dfOption[dfOption['code']==callcode].set_index('trade_date').sort_index()
    dfPut = dfOption[dfOption['code']==putcode].set_index('trade_date').sort_index()

    # diff contract
    listColumn = ['open', 'close']
    dfDiff = dfCall[listColumn] + dfPut[listColumn]
    dfDiff['indicator'] = np.nan
    dfDiff['ReturnExpected'] = row[strReturnAnnualized]
    dfDiff.loc[dtStart, 'indicator'] = 1
    
    # when to close short position of straddle
    dfPair = dfStraddle[dfStraddle['PairName']==row['PairName']]
    if direction == 'B':
        dfPairClose = dfPair[(dfPair[strIVDiff]>-ReturnOnceExit)|(dfPair['NDayToSettle']<NDayExitMax)]
    elif direction == 'S':
        dfPairClose = dfPair[(dfPair[strIVDiff]<ReturnOnceExit)|(dfPair['NDayToSettle']<NDayExitMax)]
    dfPairClose = dfPairClose[dfPairClose.index.get_level_values('trade_date')>dtStart]
    dfPairClose = dfPairClose[dfPairClose['volume'] > VolumeMin]

    if dfPairClose.empty:
        dtClosePosition = dfDiff.index[-1]
        rowClose = dfPair.iloc[-1]
    else:
        dtClosePosition = dfPairClose.index[0][-1]
        rowClose = dfPairClose.iloc[0]
    dfDiff.loc[dtClosePosition, 'indicator'] = -1

    # shift 1 day
    dfDiff['indicator'] = dfDiff['indicator'].shift(1)
    dfIndicator1 = dfDiff[dfDiff['indicator']==1]
    if dfIndicator1.empty:
        # enter tomorrow
        continue
    else:
        dtEnter = dfIndicator1.index[0]
    dfDiff['EnterPrice'] = dfDiff.loc[dtEnter, 'open']
    
    if dtClosePosition == dfDiff.index[-1]:
        dtClose = dfDiff.index[-1]
        dfDiff.loc[dtClose, 'indicator'] = -1
    else:
        dtClose = dfDiff[dfDiff['indicator']==-1].index[0]
    dfDiff['ExitPrice'] = dfDiff.loc[dtClose, 'open']
    
    # for port async
    dfDiff['callcode'] = callcode
    dfDiff['putcode'] = putcode
    dfDiff['PairName'] = row['PairName']
    dfDiff['direction'] = direction
    dfDiff['NShareExe'] = PARAMS.TOTAL_MONENY / float(PARAMS.dictNTotalSecurity[strProduct]) / row['StockPrice'] * 10
    dfDiff['StockPrice'] = dfPair.reset_index().set_index('trade_date').loc[dfDiff.index, 'StockPrice']
    dfDiff['Mark'] = row[strReturnAnnualized]
    dfDiff = dfDiff.rename(columns={'PairName':'SecuCode'})
    dfDiff.index.name = 'TradingDay'
    dfDiff['nOppo'] = nOppo
    
    dfDiff['ExeOpen'] = dfDiff['open']
    dfDiff['ExeClose'] = dfDiff['close']
    dfDiff['ExePrevCloseToOpen'] = dfDiff['open'] - dfDiff['close'].shift(1)
    dfDiff['ExePrevCloseToClose'] = dfDiff['close'] - dfDiff['close'].shift(1)
    dfDiff['ExeOpenToClose'] = dfDiff['close'] - dfDiff['open']
    listSign = ['ExeOpen', 'ExeClose', 'ExePrevCloseToOpen', 'ExePrevCloseToClose', 'ExeOpenToClose']
    if direction == 'S':
        dfDiff[listSign] = -1*dfDiff[listSign]

    if row['PairName'] == '10000117_10000118':
        #raise Exception
        pass

    dfDiff = dfDiff[(dfDiff.index >= dtEnter)&(dfDiff.index <= dtClose)]

    # calculate profit
    if direction == 'B':
        dfDiff['Profit'] = dfDiff['close'] - dfDiff['EnterPrice']
    elif direction == 'S':
        dfDiff['Profit'] = -dfDiff['close'] + dfDiff['EnterPrice']

    # calculate margin
    dfPair['EquityShortCall'] = dfPair.apply(lambda row: min(0, Strike - row['StockPrice']), axis=1)
    dfPair['EquityShortPut'] = dfPair.apply(lambda row: min(0, row['StockPrice'] - Strike), axis=1)
    if direction == 'B':
        dfDiff['Margin'] = dfDiff['EnterPrice']
    elif direction == 'S':
        sMargin = dfPair[['EquityShortCall', 'EquityShortPut']].sum(axis=1).abs()
        sMargin.name = 'Margin'
        sMargin = sMargin.reset_index().set_index('trade_date')['Margin']
        sMargin = sMargin.loc[sMargin.index.intersection(dfDiff.index)]
        dfDiff['Margin'] = sMargin
        dfDiff['Margin'] = dfDiff['Margin'] - dfDiff['close']

    if pd.isnull(dfDiff.iloc[-1]['Profit']):
        raise Exception
    print dfDiff.iloc[-1]['Profit'], dfDiff.index.size, row['volume'], rowClose['volume']
    
    # dump result
    listDF.append(dfDiff)

dfPairAll = pd.concat(listDF, axis=0)
dfPairAll.to_pickle('dfPair.pickle')

dirData = PARAMS.strDirDataStrategyTemplate%strStrategy
if PARAMS.boolClear:
    shutil.rmtree(dirData)
    os.mkdir(dirData)
for nOppo in dfPairAll['nOppo'].unique():
    strFileAddress = '%s/%s.pickle'%(dirData, str(nOppo))
    dfPairAll[dfPairAll['nOppo']==nOppo].to_pickle(strFileAddress)


