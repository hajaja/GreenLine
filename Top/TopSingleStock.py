#i -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas as pd
import numpy as np
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc, shutil
import sqlalchemy
import logging
import random

import GreenLine.Config.Stock.API as APITech
reload(APITech)
import GreenLine.Config.Stock.UtilsTech as UtilsTech
reload(UtilsTech)
import GreenLine.Config.Stock.UtilsMySQL as UtilsDB
reload(UtilsDB)
import GreenLine.Common.Utils as Utils
reload(Utils)
import GreenLine.Common.PARAMS as PARAMS
reload(PARAMS)

# 
strModelName = 'NewHigh'
#strModelName = 'TickAB'
if __name__ == '__main__':
    SecuCodeIndex = sys.argv[1]

# clear middle result
if PARAMS.boolClear:
    shutil.rmtree(PARAMS.dirDataSingleStock)

# generate dictDataSpec template
dictDataSpec = {}
dictDataSpec['strModelName'] = strModelName
dictDataSpec['SecuCodeIndex'] = SecuCodeIndex
dictDataSpec['SecuCode'] = '603993'
dictDataSpec['dtStart'] = datetime.datetime(2007,1,1)
dictDataSpec['strCase'] = 'TestCase'

if strModelName == 'TickAB':
    dictDataSpec['NDayRail'] = 240
    dictDataSpec['alphaRail'] = 0.2
    dictDataSpec['NDayFast'] = 3
    dictDataSpec['dfHFFactor'] = UtilsTech.readHFFactor()
else:
    dictDataSpec['NSigma'] = 2.
    dictDataSpec['p'] = 0.1

APITech.singleStock(dictDataSpec)

# iterate each SecuCode
import StockDataBase as SDB
reload(SDB)
dfIndexConstituent = UtilsDB.readIndexConstituent(SecuCodeIndex)
listSecuCode = np.sort(dfIndexConstituent['SecuCode'].unique()).tolist()
for SecuCode in listSecuCode:
    print SecuCode
    dictDataSpec['SecuCode'] = SecuCode
    dictDataSpec['strCase'] = SecuCode
    APITech.singleStock(dictDataSpec)


