#i -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc
import sqlalchemy
import logging
import random

import GreenLine.Common.Utils as Utils
reload(Utils)
import GreenLine.Common.PARAMS as PARAMS
reload(PARAMS)
import RoyalMountain
reload(RoyalMountain)

def calcPortAsyc(dictDataSpec):
    NTotalSecurity = PARAMS.dictNTotalSecurity[dictDataSpec['SecuCodeIndex']]
    # read trading data, enter & close price, direction
    listDF = []
    for root, dirs, files in os.walk(dictDataSpec['dirPerSecuCode']):
        for name in files:
            strFileAddress = '%s/%s'%(root, name)
            df = pd.read_pickle(strFileAddress)
            listDF.append(df)
    dfMiddle = pd.concat(listDF, axis=0)
    dfMiddle = dfMiddle.reset_index().set_index(['TradingDay', 'SecuCode'])

    dfMiddle = dfMiddle.reset_index().drop_duplicates(subset=['TradingDay', 'SecuCode']).set_index(['TradingDay','SecuCode']).sort_index()

    # read TradingDay info
    dfTradingDay = RoyalMountain.TradingDay.getTradingDayEquity()
    listTradingDay = dfTradingDay.index.intersection(dfMiddle.index.get_level_values('TradingDay'))
    listTradingDay = list(set(listTradingDay))
    listTradingDay = np.sort(listTradingDay)

    # initialize
    NSecuInPort = 0
    listDailyReturn = []
    listPerDecision = []    # each buy/sell decision is stored in listPerDecision
    setSecuCodeInPort = set()

    ReturnDollarCum = PARAMS.TOTAL_MONENY
    CumValueRatio = 1.0

    # loop for everyday
    for dtTradingDay in listTradingDay:
        ReturnDollarClose, ReturnDollarEnter, ReturnDollarSame = 0,0,0
        
        # all the stocks (either in the port or not) are stored in dfOneDay
        dfOneDay = dfMiddle.ix[dtTradingDay]
        
        # which stocks to be closed at open today (earn Open - PrevClose) 
        dfClosePosition = dfOneDay[dfOneDay['indicator']==-1]
        if dfClosePosition.empty is False:
            listSecuCodeClose = dfClosePosition.index.values.tolist()
            listSecuCodeClose = list(set(listSecuCodeClose).intersection(setSecuCodeInPort))
            if len(listSecuCodeClose) != 0:
                ReturnDollarClose = dfClosePosition.ix[listSecuCodeClose].apply(lambda row: row['NShareExe'] * CumValueRatio * row['ExePrevCloseToOpen'], axis=1).sum()
                NSecuInPort = NSecuInPort - len(listSecuCodeClose)
                for ix in listSecuCodeClose:
                    row = dfClosePosition.ix[ix]
                    listPerDecision.append({'Direction': 'S', 'SecuCode': ix, 'TradingDay': dtTradingDay, 'ExeOpen': row['ExeOpen']})
                    setSecuCodeInPort.remove(ix)
                    print ix
        
        # which stocks remain the same as yesterday (earn Close - PrevClose)
        if len(setSecuCodeInPort) > 0:
            dfSame = dfOneDay.ix[setSecuCodeInPort]
            ReturnDollarSame = dfSame.apply(lambda row: row['NShareExe'] * CumValueRatio * row['ExePrevCloseToClose'], axis=1).sum()

        # which stocks to be entered at open today (earn Close - Open)
        if NSecuInPort < NTotalSecurity:
            dfEnterPosition = dfOneDay[dfOneDay['indicator']==1]
            if dfEnterPosition.empty is False:
                NSecuAllowed = NTotalSecurity - NSecuInPort
                #listSecuCodeEnter = dfEnterPosition.index.values.tolist()
                listSecuCodeEnter = dfEnterPosition['Mark'].nlargest(NSecuAllowed).index.values.tolist()
                listSecuCodeEnter = set(listSecuCodeEnter).difference(setSecuCodeInPort)
                listSecuCodeEnter = list(listSecuCodeEnter)
                if len(listSecuCodeEnter) > 0:
                    ReturnDollarEnter = dfEnterPosition.ix[listSecuCodeEnter].apply(lambda row: row['NShareExe'] * CumValueRatio * row['ExeOpenToClose'], axis=1).sum()
                    NSecuInPort = NSecuInPort + len(listSecuCodeEnter)
                    for ix in listSecuCodeEnter:
                        row = dfEnterPosition.ix[ix]
                        if type(row['ExeOpen']) is pd.Series:
                            raise Exception
                        if ix == '603993' and dtTradingDay == datetime.datetime(2018,2,27):
                            pass
                            #raise Exception
                        listPerDecision.append({'Direction': 'B', 'SecuCode': ix, 'TradingDay': dtTradingDay, 'ExeOpen': row['ExeOpen']})
                        setSecuCodeInPort.add(ix)
                        print ix

        # store today's log
        dictOne = {}
        dictOne['TradingDay'] = dtTradingDay
        dictOne['NSecuInPort'] = NSecuInPort
        dictOne['ReturnDollar'] = ReturnDollarClose + ReturnDollarEnter + ReturnDollarSame
        if dictDataSpec['SecuCodeIndex'] in ['50ETF', 'sr.czc', 'm.czc']:
            dictOne['Margin'] = dfOneDay.loc[setSecuCodeInPort, 'Margin'].sum()
        listDailyReturn.append(dictOne)
        
        # debug
        if dtTradingDay >= datetime.datetime(2015,6,1):
            pass
            #raise Exception
        print dtTradingDay, NSecuInPort, dfClosePosition.index.size
        if type(dictOne['ReturnDollar']) not in [np.float64, int]:
            raise Exception
        if dictOne['ReturnDollar'] > PARAMS.TOTAL_MONENY * 0.9:
            pass
            #raise Exception

        # CumValueRatio
        ReturnDollarCum = ReturnDollarCum + dictOne['ReturnDollar']
        CumValueRatio = min(1, ReturnDollarCum / PARAMS.TOTAL_MONENY * 0.8)

    # daily return
    dfDailyReturn = pd.DataFrame(listDailyReturn).set_index('TradingDay')
    sPCTPort = (dfDailyReturn['ReturnDollar'].cumsum() + PARAMS.TOTAL_MONENY).pct_change()
    dictMetric = RoyalMountain.PortPerf.Eval.Utils.funcMetric(sPCTPort)
    print '\nPort Performance %s-%s'%(sPCTPort.index.min(), sPCTPort.index.max())
    print pd.Series(dictMetric)

    # per decision, append the decision for current position
    for SecuCode in setSecuCodeInPort:
        if SecuCode in dfOneDay.index:
            row = dfOneDay.ix[SecuCode]
            listPerDecision.append({'Direction': 'S', 'SecuCode': SecuCode, 'TradingDay': dtTradingDay, 'ExeOpen': row['ExeClose']})
        else:
            ExeClose = dfMiddle.xs(SecuCode, level='SecuCode')['ExeClose'].iloc[-1] # for stocks not trading today, there is no data in dfOneDay
            listPerDecision.append({'Direction': 'S', 'SecuCode': SecuCode, 'TradingDay': dtTradingDay, 'ExeOpen': ExeClose})
    dfPerDecision = pd.DataFrame(listPerDecision).set_index(['SecuCode', 'TradingDay']).sort_index()
    NDecision = dfPerDecision.index.size
    PCTPerDecision = dfPerDecision.ix[range(1,NDecision,2), 'ExeOpen'].values / dfPerDecision.ix[range(0,NDecision,2), 'ExeOpen'].values - 1
    sPCTPerDecision = pd.Series(PCTPerDecision)
    
    #raise Exception

if __name__ == '__main__':
    # sample : run Weight/UtilsPortAsync 50ETF OptionStraddle
    SecuCodeIndex = sys.argv[1]
    Strategy = sys.argv[2]
    dictDataSpec = {}

    dictDataSpec['dirPerSecuCode'] = PARAMS.strDirDataStrategyTemplate%Strategy
    dictDataSpec['SecuCodeIndex'] = SecuCodeIndex
    calcPortAsyc(dictDataSpec)


