import pandas as pd
import numpy as np
import io, zipfile
import urllib2
from cStringIO import StringIO
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from pandas.tools.plotting import scatter_matrix
import copy
import time
import sys

"""Load/re-load class definitions"""
from dataCAISO import dataCAISO
from dataCAISO_LmpSolarValue import dataCAISO_LmpSolarValue




def main():
    """------------------INPUTS---------------------"""
    filename = 'data/LmpSolar - CA - Jun1-Jun7.h5'
    #bbox = [37.2,38.1,-122.5,-121.7]
    startdate='20150601T00:00-0000'
    enddate='20150608T00:00-0000' 
    thread_days = 1
    debug = True
    methodAll = False
    reload = False
    print_level = 2 #level to print to screen  -  0: none, 1: errors, 2: info, 3: debug
    """---------------------------------------------"""


    """LOGGING"""
    logfile = ( 'logs/' + filename.split('/')[1].split('.')[0] + '.log' )
    from logging_setup import logging_setup
    logging_setup(print_level, logfile)

    """CREATE LMPSOLARVALUE INSTANCE"""
    test = dataCAISO_LmpSolarValue(startdate=startdate, enddate=enddate, debug=debug) #,bbox = bbox

    """RUN BATCH"""
    test.saveNodesLmpSolarThreaded(filename ,thread_days, methodAll, reload = reload)

    """PRINT STUFF"""
    dfNodeSolarValue = pd.read_hdf(filename,'dfNodeSolarValue')
    print dfNodeSolarValue.info()
    print dfNodeSolarValue.head()



if __name__ == '__main__':
    main()