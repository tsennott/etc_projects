#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 16:53:18 2016

@author: timothysennott
"""
import pandas as pd
from dataCAISO import dataCAISO
import threading
import time
import urllib2
import os.path

class dataCAISO_LmpSolarValue(dataCAISO):
    """Subclass of dataCAISO that combines LMP and radiation data to estimate solar value"""
    def __init__(self, *args, **kwargs):
        
        # Call superclass init
        dataCAISO.__init__(self, *args, **kwargs)
        
            
    def readFile(self,filename):
        """Reads back in a saved file from self.saveNodesLmpSolarThreaded"""
        self.dfNodeSolarValue = pd.read_hdf('data/test - saveNodesLmpSolarThreaded.h5','dfNodeSolarValue')
        self.dfLmpSolar = pd.read_hdf('data/test - saveNodesLmpSolarThreaded.h5','dfLmpSolar')
        
        
    def saveNodesLmpSolarThreaded(self, filename, thread_days, methodAll, reload = False):
        """
        Method to grab weather and lmp data, combine, and save to hdf5
            (threaded on nodes only within OASIS node fetch method for now)
        TODO: Allow calling to "ALL" method to improve OASIS fetching
        """
        self.log.info('method saveNodesLmpSolarThreaded called with filename: %s and thread_days: %s' %(filename,thread_days))
        
        # Create temp stores for data
        dfLmp = pd.DataFrame()
        dfSolar = pd.DataFrame()
        num_nodes = len(self.nodelist)
        print reload
        if reload: 
            # open existing file (non-overwrite) and read in data
            if not(os.path.isfile(filename)): raise IOError('File not found for reloading')
            store = pd.HDFStore(filename)
            dfLmp = store['dfLmp']
            dfSolar = store['dfSolar']  
        else:
            # overwrite and open
            store = pd.HDFStore(filename, mode = 'w') 
        
        # Depending on choice of mode, use fetch single node or all nodes
        if methodAll:
            self.log.error('ALL METHOD NOT IMPLEMENTED, but will get weather')
            # Loop over nodes to build weather, saving as we go if in debug
            for num,node in enumerate(self.nodelist):
                tstart = time.time()
                dfSolar = dfSolar.append( self.getNodeWeather(node,'ghi') )
                if self.debug: store.put('dfSolar', dfSolar, format='table', data_columns=True)
                self.log.debug('Weather fetched for %s, %s of %s nodes in %s sec' % (node,num+1,num_nodes,
                                                                                     (time.time()-tstart)))
            # TODO -- NEED TO IMPLEMENT SECOND LOOP, IDEALLY THREADED, TO BUILD UP LMP DATA
                    # ...CURRENTLY STILL HAVING FUCKING PROXY ERRORS...
            # TODO -- much later, need to look at chunking the data while handling large fetches       
            
            return 
           
        else:
            
            # Loop over nodes, saving as we go if in debug
            for num,node in enumerate(self.nodelist):
                # If reloading, check if node is present and continue if so
                if reload:
                    if node in dfSolar.index:
                        self.log.info('Node %s found in store, continuing' % node)
                        continue
                tstart = time.time()
                dfLmp = dfLmp.append( self.getNodeLmpThreaded(node, thread_days) )
                
                try:
                    dfSolar = dfSolar.append( self.getNodeWeather(node,'ghi') )
                except urllib2.HTTPError as e:
                    self.log.error('can''t get weather data node %s, error %s, continuing...' %(node,str(e)))               
                if self.debug: store.put('dfLmp', dfLmp, format='table', data_columns=True)
                if self.debug: store.put('dfSolar', dfSolar, format='table', data_columns=True)
                self.log.info('Node %s done, finished %s of %s nodes in %s sec' % (node,num+1,num_nodes,(time.time()-tstart)))

        # Merge data
        self.dfLmpSolar = pd.merge(dfLmp, dfSolar, how='inner', on=None, left_on=None, right_on=None,left_index=True, 
                 right_index=True, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False)

        # Calculate solar value approximation as LMP*GHI
        self.dfLmpSolar['Solar_Value'] = self.dfLmpSolar['LMP_PRC'] * self.dfLmpSolar['GHI'] * 8760/1000/1000

        # Grab solar value, group on index value and average
        self.dfNodeSolarValue = self.dfLmpSolar[['LMP_PRC','GHI','Solar_Value']].groupby(level=0).mean()
        # Join with node locations and sort 
        self.dfNodeSolarValue = (self.dfNodeLocations.join(self.dfNodeSolarValue)
                             .sort_values(by='Solar_Value',ascending=False))
        # Save out tables
        store.put('dfLmpSolar', self.dfLmpSolar, format='table', data_columns=True)
        store.put('dfNodeSolarValue', self.dfNodeSolarValue, format='table', data_columns=True)
        store.close()
        self.log.info('Finished merging tables and saved to %s' % filename)
    
    def zdeprecated_buildAllNodes(self):
        """Deprecated function to build solar value table using the single-zone all-node fetch from CAISO"""
                          
        # TODO -- NEED TO CHANGE OLDER METHODS NAMES, AND THEN FIX WEATHER-ALL PARENT METHOD AND INCLUDE NODE LIST
                          
        # Construct main data frame
        dfLmp = self.getLmpAll(self.startdate)
        dfSolar = self.getAllNodeWeather('ghi')
        self.dfLmpSolar = pd.merge(dfLmp, dfSolar, how='inner', on=None, left_on=None, right_on=None,left_index=True, 
                 right_index=True, sort=True, suffixes=('_x', '_y'), copy=True, indicator=False)

        # Calculate solar value approximation as LMP*GHI
        self.dfLmpSolar['Solar_Value'] = self.dfLmpSolar['LMP_DAM_LMP'] * self.dfLmpSolar['GHI']

        # Grab solar value, group on index value and sum
        self.dfNodeSolarValue = self.dfLmpSolar[['LMP_DAM_LMP','GHI','Solar_Value']].groupby(level=0).sum()
        # Join with node locaitons and sort 
        self.dfNodeSolarValue = (self.dfNodeLocations.join(self.dfNodeSolarValue)
                             .sort_values(by='Solar_Value',ascending=False))
        
    