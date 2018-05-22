# -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import datetime
import numpy as np
import pdb
import scipy.io as sio
import os
import gc
import random
from scipy.signal import butter, lfilter, freqz
from scipy import signal

import UtilsMySQL as UtilsDB
reload(UtilsDB)
import GreenLine.Common.Utils as Utils
reload(Utils)
import GreenLine.Common.PARAMS as PARAMS
reload(PARAMS)
import logging

###################
# wrapper
###################
def generateIndicatorTech(dictDataSpec):
    """
    wrapper for all indicator generator
    """
    if dictDataSpec['strModelName'] == 'TickAB':
        seriesIndicator = generateIndicatorTickAB(dictDataSpec)
    elif dictDataSpec['strModelName'] == 'NewHigh':
        seriesIndicator = generateIndicatorNewHigh(dictDataSpec)
    else:
        raise Exception

    return seriesIndicator

def readHFFactor():
    strDataPath = '/home/csdong/workspace/DATA/StockDataBase/DATA/'
    dfFactorRaw = pd.read_pickle(strDataPath + 'dfQuantileValueS.pickle')
    return dfFactorRaw

def funcCalcFactor1(df, NDayFast):
    NDay = NDayFast
    ## PTQuantileValue_BOA
    #df['Diff'] = df['PTQuantileValue_B_80'] - df['PTQuantileValue_A_80']
    #df['Mean'] = (df['PTQuantileValue_B_80'] + df['PTQuantileValue_A_80']) / 2
    #sFactor = df['Diff'].rolling(NDay).sum() / df['Mean'].rolling(NDay).sum()
    #sFactor.name = 'PTQuantileValue_BOA_80'
    
    # PTQuantileValue_BOA
    df['Diff'] = df['PTQuantileValue_B_90'] - df['PTQuantileValue_A_90']
    df['Mean'] = (df['PTQuantileValue_B_90'] + df['PTQuantileValue_A_90']) / 2
    sFactor = df['Diff'].rolling(NDay).sum() / df['Mean'].rolling(NDay).sum()
    sFactor.name = 'PTQuantileValue_BOA_90'

    return sFactor

#################
#
#################
def postProcess(df, ixL, ixS, TOTAL_MONENY_PER_SEC):
    # prepare dataframe for portfolio calculation
    df.ix[ixL, 'PriceEnter'] = df.ix[ixL, 'ExeOpen']
    df.ix[ixS, 'PriceEnter'] = np.nan
    df.ix[ixS, 'PriceClose'] = df.ix[ixS, 'ExeOpen']
    df.ix[ixL, 'PriceClose'] = np.nan
    df.ix[ixL, 'NShare'] = df.ix[ixL, 'Open'].apply(lambda x: round(TOTAL_MONENY_PER_SEC / x / PARAMS.NSharePerLot) * PARAMS.NSharePerLot)
    df.ix[ixS, 'NShare'] = 0
    df.ix[ixL, 'NShareExe'] = df.ix[ixL].apply(lambda row: row['NShare'] * row['Open'] / row['ExeOpen'], axis=1)
    df.ix[ixS, 'NShareExe'] = 0
    df.ix[ixL, 'Direction'] = 1
    df.ix[ixS, 'Direction'] = -1
    listFactorFFill = ['PriceEnter', 'PriceClose', 'NShare', 'NShareExe',  'Direction']
    df[listFactorFFill] = df[listFactorFFill].ffill()

    # calculate daily change for portfolio value calculation
    df['ExeOpenToClose'] = df['ExeClose'] - df['ExeOpen'] * (1+PARAMS.COMMISSION)
    df['ExePrevCloseToOpen'] = df['ExeOpen'] * (1-PARAMS.COMMISSION) - df['ExeClose'].shift(1)
    df['ExePrevCloseToClose'] = df['ExeClose'] - df['ExeClose'].shift(1)

    # calculate NShare equivalent if using ExeOpen to buy

    # simple calculation of the cumulateed value
    df['PCTHold'] = df['PCT'] * df['indicator'].ffill()
    df['Value'] = (1+df['PCTHold']).cumprod()
    if pd.isnull(df.ix[-1, 'Value']):
        #raise Exception
        pass
    print df.ix[-1, 'Value'], df['Value'].max()
    
    # save result
    dfOut = df[listFactorFFill + ['SecuCode', 'ExeOpen', 'ExeClose', 'ExeOpenToClose', 'ExePrevCloseToOpen', 'ExePrevCloseToClose', 'Mark', 'indicator']]

    return dfOut


#################
# inter day strategy
#################
def generateIndicatorTickAB(dictDataSpec):
    # parameters
    NTotalSecurity = PARAMS.dictNTotalSecurity[dictDataSpec['SecuCodeIndex']]
    TOTAL_MONENY_PER_SEC = PARAMS.TOTAL_MONENY / NTotalSecurity
    NDayRail = dictDataSpec['NDayRail']
    alphaRail = dictDataSpec['alphaRail']

    # get the data from dictDataSpec
    dfTrading = UtilsDB.getTradingData(dictDataSpec).set_index('TradingDay')
    dfHFFactor = dictDataSpec['dfHFFactor']
    if dictDataSpec['SecuCode'] not in dfHFFactor.index.get_level_values('SecuCode'):
        return
    dfHFFactor = dfHFFactor.ix[dictDataSpec['SecuCode'], ['PTQuantileValue_B_80', 'PTQuantileValue_A_80', 'PTQuantileValue_B_90', 'PTQuantileValue_A_90']]
    sFactor = funcCalcFactor1(dfHFFactor, dictDataSpec['NDayFast'])
    ixCommon = dfTrading.index.intersection(sFactor.index)
    dfTrading.ix[ixCommon, 'QuantileValueBOA'] = sFactor.ix[ixCommon]
    dfTrading = dfTrading.sort_index()
    dfTrading['PCT'] = dfTrading['ExeClose'].pct_change()
    dfTrading = dfTrading[dfTrading['QuantileValueBOA'].isnull()==False]
    if dfTrading.empty:
        return
    
    # calculate indicator
    sInfo = dfTrading['QuantileValueBOA']
    sLowerRail = sInfo.rolling(NDayRail).quantile(alphaRail)
    sUpperRail = sInfo.rolling(NDayRail).quantile(1-alphaRail)
    #sLowerRail = sLowerRail.apply(lambda x: min(-0.1, x))
    #sUpperRail = sUpperRail.apply(lambda x: max(0.3, x))
    #sLowerRail = -0.2
    #sUpperRail = 0.4
    ixL = sInfo < sLowerRail
    ixS = sInfo > sUpperRail
    ixL = ixL.shift(1).fillna(False)
    ixS = ixS.shift(1).fillna(False)
    dfTrading.ix[ixL, 'indicator'] = 1
    dfTrading.ix[ixS, 'indicator'] = -1
    dfTrading['indicator'] = Utils.keepOperation(dfTrading['indicator'])
    sIndicatorValueCount = dfTrading['indicator'].value_counts()
    if sIndicatorValueCount.size < 1:
        #print sIndicatorValueCount
        return

    # prepare dataframe for portfolio calculation
    dfTrading.ix[ixL, 'PriceEnter'] = dfTrading.ix[ixL, 'ExeOpen']
    dfTrading.ix[ixS, 'PriceEnter'] = np.nan
    dfTrading.ix[ixS, 'PriceClose'] = dfTrading.ix[ixS, 'ExeOpen']
    dfTrading.ix[ixL, 'PriceClose'] = np.nan
    dfTrading.ix[ixL, 'NShare'] = dfTrading.ix[ixL, 'Open'].apply(lambda x: round(TOTAL_MONENY_PER_SEC / x / PARAMS.NSharePerLot) * PARAMS.NSharePerLot)
    dfTrading.ix[ixS, 'NShare'] = 0
    dfTrading.ix[ixL, 'NShareExe'] = dfTrading.ix[ixL].apply(lambda row: row['NShare'] * row['Open'] / row['ExeOpen'], axis=1)
    dfTrading.ix[ixS, 'NShareExe'] = 0
    dfTrading.ix[ixL, 'Direction'] = 1
    dfTrading.ix[ixS, 'Direction'] = -1
    listFactorFFill = ['PriceEnter', 'PriceClose', 'NShare', 'NShareExe',  'Direction']
    dfTrading[listFactorFFill] = dfTrading[listFactorFFill].ffill()
    dfTrading['Mark'] = -dfTrading['QuantileValueBOA']

    # calculate daily change for portfolio value calculation
    dfTrading['ExeOpenToClose'] = dfTrading['ExeClose'] - dfTrading['ExeOpen'] * (1+PARAMS.COMMISSION)
    dfTrading['ExePrevCloseToOpen'] = dfTrading['ExeOpen'] * (1-PARAMS.COMMISSION) - dfTrading['ExeClose'].shift(1)
    dfTrading['ExePrevCloseToClose'] = dfTrading['ExeClose'] - dfTrading['ExeClose'].shift(1)

    # calculate NShare equivalent if using ExeOpen to buy

    # simple calculation of the cumulateed value
    dfTrading['PCTHold'] = dfTrading['PCT'] * dfTrading['indicator'].ffill()
    dfTrading['Value'] = (1+dfTrading['PCTHold']).cumprod()
    print dfTrading.ix[-1, 'Value'], dfTrading['Value'].max()
    
    # save result
    strFileAddress = '%s/%s.pickle'%(PARAMS.dirDataSingleStock, dictDataSpec['strCase'])
    dfOut = dfTrading[listFactorFFill + ['SecuCode', 'ExeOpen', 'ExeClose', 'ExeOpenToClose', 'ExePrevCloseToOpen', 'ExePrevCloseToClose', 'Mark', 'indicator']]
    dfOut.to_pickle(strFileAddress)

    if dictDataSpec['SecuCode'] in ['601988']:
        pass
        #raise Exception

##################
# NewHigh
#################
def generateIndicatorNewHigh(dictDataSpec):
    # parameters
    NTotalSecurity = PARAMS.dictNTotalSecurity[dictDataSpec['SecuCodeIndex']]
    TOTAL_MONENY_PER_SEC = PARAMS.TOTAL_MONENY / NTotalSecurity
    p = dictDataSpec['p']

    # get the data from dictDataSpec
    df = UtilsDB.getTradingData(dictDataSpec).set_index('TradingDay')
    df['PCT'] = df['ExeClose'].pct_change()
    #p = dictDataSpec['NSigma'] * df['PCT'].std()
    print p
    
    # calculate indicator
    import RoyalMountain.Tech.BTRecognizer as BTRecognizer
    reload(BTRecognizer)
    df = BTRecognizer.funcBT(df, 'ExeClose', p)
    if df['confirm'].dropna().empty:
        return

    # low PE PB
    boolPELow = df['PE'] < df['PE'].expanding().quantile(0.5)
    boolPBLow = df['PB'] < df['PB'].expanding().quantile(0.8)

    NDayRollingSum = 60
    sTurnoverRatioRollingSum = df['TurnoverRatio'].rolling(NDayRollingSum).sum() 
    boolTurnoverRatio = sTurnoverRatioRollingSum < 2. * sTurnoverRatioRollingSum.shift(NDayRollingSum)

    boolPELow.iloc[:250*3] = True
    boolPBLow.iloc[:250*3] = True
    boolPELow = True
    boolPBLow = True
    boolTurnoverRatio = True

    df = df.rename(columns={'indicator':'indicatorBT'})
    df[['top', 'bottom', 'confirm']] = df[['top', 'bottom', 'confirm']].ffill()
    ixL = df[(df['confirm']=='B')&(df['ExeClose']>df['top'])&boolPELow&boolPBLow&boolTurnoverRatio].index
    ixS = df[(df['confirm']=='T')&(df['ExeClose']<df['bottom'])].index
    #ixS = df[df['ExeClose'] < df['ExeClose'].rolling(5).max()*0.8].index

    # assign indicator
    df.ix[ixL, 'indicator'] = 1
    df.ix[ixS, 'indicator'] = -1
    sIndicator = Utils.keepOperation(df['indicator'].ffill())
    sIndicator = sIndicator.shift(1)
    ixL = sIndicator[sIndicator==1].index
    ixS = sIndicator[sIndicator==-1].index
    df['indicator'] = sIndicator
    sIndicatorValueCount = df['indicator'].value_counts()
    if sIndicatorValueCount.size < 1:
        return

    # mark for priority
    df['Mark'] = df['TurnoverRatio'].rolling(20).median()

    # post process & dump result
    df = postProcess(df, ixL, ixS, TOTAL_MONENY_PER_SEC)
    strFileAddress = '%s/%s.pickle'%(PARAMS.dirDataSingleStock, dictDataSpec['strCase'])
    df.to_pickle(strFileAddress)



