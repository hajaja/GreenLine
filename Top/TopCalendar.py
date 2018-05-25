# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc, shutil
import sqlalchemy
import logging
import random

import GreenLine.Config.Option.Utils as Utils
reload(Utils)

import GreenLine.Common.PARAMS as PARAMS
reload(PARAMS)
import OptionDataBase as ODB
reload(ODB)

# param
VolumeMin = 500
strIVDiff = 'IVDiff'
NDayEnterMin = 10
NDayExitMax = 5
ReturnOnce = 0.1
ReturnOnceExit = 0.01
strReturnAnnualized = 'ReturnExpected'
boolUpdate = False

strProduct = '50ETF'
#strProduct = 'm.dce'
strStrategy = 'OptionCalendar'

if boolUpdate:
    strAddressIV = ODB.Utils.strAddressIVTemplateLatest%strProduct
else:
    strAddressIV = ODB.Utils.strAddressIVTemplateHist%strProduct

# read data from ODB
strFileAddressTemp = '%s/Cache/%s_%s_%s.pickle'%(PARAMS.dirDataSource, strStrategy, strProduct, str(boolUpdate))
if os.path.exists(strFileAddressTemp):
    dfCalendar = pd.read_pickle(strFileAddressTemp)
else:
    df = pd.read_excel(strAddressIV)
    df = df[df['StockPrice'].notnull()]
    if strProduct == '50ETF':
        df['code'] = df['code'].apply(lambda x: str(int(x)))
    
    # only keep ATM pairs
    df['ATM'] = False
    df = df.groupby(['SettleDate', 'trade_date']).apply(Utils.keepATM, 5)
    
    # calendar
    #gg = df.set_index('trade_date').groupby(['SettleDate', 'Strike'])
    gg = df.groupby(['trade_date', 'Strike', 'COP'])
    dfCalendar = gg.apply(Utils.funcFindCalendar)
    dfCalendar = dfCalendar[dfCalendar['ATM'].notnull()]
    dfCalendar['ATM'] = dfCalendar['ATM'].astype(bool)

    dfCalendar.to_pickle(strFileAddressTemp)

# when to enter
dfOppo = dfCalendar[(dfCalendar[strIVDiff]>ReturnOnce)]
dfOppo = dfOppo[dfOppo['ATM']&(dfOppo['NDayToSettle']>NDayEnterMin)&(dfOppo['volume']>VolumeMin)]
print dfOppo.shape

# generate pseudo secu for port value calculation
listDF = []
for nOppo in range(0, dfOppo.index.size):
    ix = dfOppo.index[nOppo]
    row = dfOppo.iloc[nOppo]
    trade_date = ix[0]
    Strike = ix[1]
    COP = ix[2]
    print trade_date, nOppo

    # read two contracts
    if strProduct == '50ETF':
        nearcode = str(int(row['nearcode']))
        deferredcode = str(int(row['deferredcode']))
    else:
        nearcode = row['nearcode']
        deferredcode = row['deferredcode']

    listContractCode = [nearcode, deferredcode]
    dtStart = trade_date
    dfOption = ODB.Utils.UtilsDB.readDB(ODB.Utils.UtilsDB.strDB, ODB.Utils.UtilsDB.DAILY_DB_NAME, dtStart-datetime.timedelta(20, 0), listContractCode)
    dfNear = dfOption[dfOption['code']==nearcode].set_index('trade_date').sort_index()
    dfDeferred = dfOption[dfOption['code']==deferredcode].set_index('trade_date').sort_index()

    # diff contract
    listColumn = ['open', 'close']
    dfDiff = dfNear[listColumn] - dfDeferred[listColumn]
    dfDiff['indicator'] = np.nan
    dfDiff['ReturnExpected'] = row[strReturnAnnualized]
    dfDiff.loc[dtStart, 'indicator'] = 1
    
    # when to close short position of straddle
    dfPair = dfCalendar[dfCalendar['PairName']==row['PairName']]
    dfPairClose = dfPair[(dfPair[strIVDiff]<ReturnOnceExit)|(dfPair['NDayToSettle']<=NDayExitMax)]
    dfPairClose = dfPairClose[dfPairClose.index.get_level_values('trade_date')>dtStart]
    dfPairClose = dfPairClose[dfPairClose['volume'] > VolumeMin]

    if dfPairClose.empty:
        dtClosePosition = dfNear.index[-NDayExitMax]
        rowClose = dfPair.iloc[-1]
    else:
        dtClosePosition = dfPairClose.index[0][0]
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
    
    if dtClosePosition == dfNear.index[-1]:
        dtClose = dfNear.index[-1]
        dfDiff.loc[dtClose, 'indicator'] = -1
        if dtClose == dtEnter:
            # enter at 20180327, close at 20180328
            print 'enter at %s, close at %s'%(dtEnter, dtClose)
            continue
    else:
        dtClose = dfDiff[dfDiff['indicator']==-1].index[0]
    dfDiff['ExitPrice'] = dfDiff.loc[dtClose, 'open']
    
    # for port async
    dfDiff['nearcode'] = nearcode
    dfDiff['deferredcode'] = deferredcode
    dfDiff['PairName'] = row['PairName']
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
    dfDiff[listSign] = -1*dfDiff[listSign]

    if row['PairName'] == '10000117_10000118':
        #raise Exception
        pass

    dfDiff = dfDiff[(dfDiff.index >= dtEnter)&(dfDiff.index <= dtClose)]

    # calculate profit
    dfDiff['Profit'] = -dfDiff['close'] + dfDiff['EnterPrice']

    # calculate margin
    dfPair['EquityShortCall'] = dfPair.apply(lambda row: min(0, Strike - row['StockPrice']), axis=1)
    dfPair['EquityShortPut'] = dfPair.apply(lambda row: min(0, row['StockPrice'] - Strike), axis=1)

    dfPair = dfPair.reset_index().set_index('trade_date').sort_index()
    dfMargin = dfPair.loc[dfDiff.index.intersection(dfPair.index)]
    if COP:
        dfDiff['Margin'] = dfMargin['EquityShortCall']
    else:
        dfDiff['Margin'] = dfMargin['EquityShortPut']
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
