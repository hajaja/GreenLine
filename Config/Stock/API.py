#i -*- coding:utf-8 -*-
__author__ = 'csdong'
import pandas
import numpy
from dateutil.parser import parse
import os, re, pdb, datetime, sys, gc
import sqlalchemy

import logging
import random

import GreenLine.Common.Utils as Utils
reload(Utils)
import UtilsTech
reload(UtilsTech)

###################################
# wrapper
###################################
def singleStock(dictDataSpec):
    import UtilsTech
    reload(UtilsTech)
    UtilsTech.generateIndicatorTech(dictDataSpec)

###################################
# function
###################################
# calculate total score of a stock

if __name__ == '__main__':
    SecuCode = sys.argv[1]
    dictDataSpec = {}
    dictDataSpec['NDayRail'] = 240
    dictDataSpec['alphaRail'] = 0.05
    dictDataSpec['SecuCode'] = SecuCode
    dictDataSpec['dtStart'] = datetime.datetime(2013,1,1)
    dictDataSpec['strModelName'] = 'TickAB'
    dictDataSpec['dfHFFactor'] = UtilsTech.readHFFactor()
    dictDataSpec['strCase'] = 'TestCase'
    singleStock(dictDataSpec)
