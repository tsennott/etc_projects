#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 16:52:11 2016

@author: timothysennott
"""
import pandas as pd
import io, zipfile
import urllib2
from cStringIO import StringIO
import copy
import time
import threading
import Queue
import logging

class dataCAISO:
    """
    Wrapper class for tools built to use the CAISO OASIS Data 
    Inputs (optional):
        startdate - beginning time string for all methods, i.e. 20150831T13:00-0000
        enddate - ending time string for all methods, i.e. 20150831T15:00-0000
        debug - boolean determines debugging behavior
    
    """
    
    def __init__(self, *args, **kwargs):
        """Sets default values of inputs (dates, debug, ) and loads node locations from file"""
        
        # Set start and end to be used for all methods and data
        startdate = kwargs.get('startdate','20150831T13:00-0000')
        enddate = kwargs.get('enddate','20150831T15:00-0000')
        # Convert to datetime
        self.startdate = pd.to_datetime(startdate)
        self.enddate = pd.to_datetime(enddate)
        # Get debug state, used for internally saving redundant data, etc
        self.debug = kwargs.get('debug',False)
        # Get node locations
        self.dfNodeLocations = self.getNodeLocationsFromFile('data/CAISO LMP Node Locations.csv')
        # Get bounding (if included) and drop nodes
        self.bbox = kwargs.get('bbox',[-180,180,-180,180])
        self.dropNodesOutside(self.bbox)
        # Make list of nodes
        self.nodelist = list(self.dfNodeLocations.index.get_level_values('NODE').unique())
        # Set up logging ## TODO add elsewhere and remove the debug print things
        self.log=logging.getLogger('main')
        self.log.info('New %s created, %s nodes, args: %s and kwargs: %s' % (self.__class__.__name__, 
                                                                             len(self.nodelist), args, kwargs))
        
        
    def dropNodesOutside(self, bbox):
        """Drops node locations from dfNodeLocations, takes list of la_min, la_max, ln_min, ln_max """
        
        self.dfNodeLocations = self.dfNodeLocations[(
                (self.dfNodeLocations['latitude'] > bbox[0] ) &
                (self.dfNodeLocations['latitude'] < bbox[1] )&
                (self.dfNodeLocations['longitude'] > bbox[2] ) &
                (self.dfNodeLocations['longitude'] < bbox[3] ) )]
        
        
    def getNodeWeather(self,node,properties):
        """
        Returns multiindex dataframe of requested weather properties for each node,
        for the time period [self.startdate:self.enddate]
        """
        
        # Get lat,lon of node
        lat,lon = self.dfNodeLocations.loc[node,['latitude','longitude']].tolist()
        # Fetch data for each year required
        years = range(self.startdate.year,self.enddate.year+1,1)
        dfWeatherNode=pd.DataFrame()
        for year in years:
            if year > 2015: raise TypeError('Cannot get NSRDB for years past 2015')
            dfWeatherYearNode = self.getNSRDB(lat,lon,year,properties)
            # Only keep range we are interested in
            dfWeatherYearNode = dfWeatherYearNode[self.startdate:self.enddate]
            # Add column name of node
            dfWeatherYearNode.loc[:,'NODE'] = node 
            # Append and keep going
            dfWeatherNode = dfWeatherNode.append(dfWeatherYearNode)
        # Convert to multi-index in proper order
        dfWeatherNode.set_index(['NODE'],append=True, inplace=True)
        dfWeatherNode = dfWeatherNode.reorder_levels(['NODE','Time_UTC'])
        return dfWeatherNode
    
    def getNodeLmpThreaded(self,node, thread_days):
        """Returns DAM LMP data for a node over the whole date range"""
        
        # Define thread worker function
        def thread_worker(node,start,end,q_out):
            try:
                dfLmp31 = self.getNodeLmp31(node, start, end)
            except urllib2.HTTPError as e:
                self.log.error('can''t get caiso, node %s, error %s, continuing...' %(node,str(e)))
                return
            except TypeError as e:
                self.log.error(e)
                return
                
            q_out.put(dfLmp31)
            self.log.debug('got %s to %s' %(start,end))
            
        # Initialize dates and queue and loop until we've gotten enough data
        start = self.startdate
        end = start
        q_out = Queue.Queue()
        threads = []
        while True:
            end=min([self.enddate , end + pd.datetools.timedelta(days=thread_days)])
            self.log.debug('fetching %s to %s' %(start,end))
            t = threading.Thread(target=thread_worker, args=(node,start,end,q_out))
            threads.append(t)
            t.start()
            time.sleep(6)
            start=end
            if end==self.enddate: break
        
        # Wait for queues, then build up and return dataframe
        self.log.debug('NODE %s ALL THREADS DISPATCHED' % node)
        for t in threads:
            t.join()
        self.log.debug('NODE %s ALL DONE' % node)
        dfLmp = pd.DataFrame()
        while not(q_out.empty()):
            dfLmp = dfLmp.append(q_out.get())
            
        return dfLmp
    
    def getNodeLmp31(self, node, startdate, enddate):
        """Returns up to 31 days of DAM LMP data for specified node"""
        
        # Convert datetime to string
        startdate_string = startdate.strftime('%Y%m%dT%H:%M-0000')
        enddate_string = enddate.strftime('%Y%m%dT%H:%M-0000')
        # Build url
        url=('http://oasis.caiso.com/oasisapi/SingleZip?'+
             'queryname=PRC_LMP&'+
             'startdatetime='+startdate_string+'&'+ 
             'enddatetime='+enddate_string+'&'+  
             'market_run_id=DAM&resultformat=6&version=1&node='+node) 
            
        # Get data 
        names, dfOasisLmp_list = self.getOasisUrl(url)
        if self.debug: self.debug_oasis_data,self.debug_oasis_names = copy.copy(dfOasisLmp_list),names
        # Convert date and drop unneeded data
        dfOasisLmp_list[0]['Time_UTC']=pd.to_datetime(dfOasisLmp_list[0]['INTERVALSTARTTIME_GMT'])
        dfOasisLmp_list[0] = dfOasisLmp_list[0].loc[:,['Time_UTC','NODE','XML_DATA_ITEM','MW']]
        # Can use pd.pivot_table to both extract columns and set to proper multi-index
        dfOasisLmp = dfOasisLmp_list[0].pivot_table(index=['NODE','Time_UTC'],
                                    columns='XML_DATA_ITEM',values='MW')
            
        return dfOasisLmp
 

    def getOasisUrl(self,url):
        """ Returns unprocessed OASIS data. 
            Returns:
                filenames: list of returned filenames
                dfOasis_list: list of dataframes, one from each file, unprocessed"""
        
        # Get data
        self.log.debug('url: %s' %url)
        try:
            response=urllib2.urlopen(url)
        except urllib2.HTTPError as e:
            self.log.error(e)
            self.log.debug(url)
            raise 
        data=response.read()
        
        # Unzip unto list of strings, one for each file
        filecontent=io.BytesIO(data)
        z=zipfile.ZipFile(filecontent)
        unzipped = [z.read(thisfile) for thisfile in z.namelist()]
        filenames = [thisfile for thisfile in z.namelist()]
        z.close()
        
        # Make sure we haven't returned an error
        if 'xml' in filenames[0]:
            print url
            raise TypeError('oasis request returned invalid, url = ' + url)
            
        # For each string create a file-like stream and read stream into dataframe
        streams = [StringIO(unzipped[n]) for n in range(0,len(unzipped))]
        dfOasis_list = [pd.read_csv(thisstream) for thisstream in streams]

        return filenames, dfOasis_list 
    
    
    def getNodeLocationsFromFile(self, node_location_file):
        """Returns CAISO node locations from local csv file """
        
        # Read csv, rename node column and index on it
        dfNodeLocations = pd.read_csv(node_location_file)
        dfNodeLocations.rename(columns={'name':'NODE'},inplace=True)
        dfNodeLocations.set_index(['NODE'], inplace=True)
        
        return dfNodeLocations

    
    def getNSRDB(self,lat,lon,year,properties):
        """Returns NSRDB info, just GHI for now, for the requested year and location"""
        
        # Build up url
        api_key = 'cB1c77oc4kcas2uzZ6LDNGlu47RreNqCRdqt15FH'
        attributes = properties
        leap_year = 'true'
        interval = '60'
        utc = 'true'
        your_name = 'Tim+Sennott'
        reason_for_use = 'beta+testing'
        your_affiliation = ''
        your_email = 'timothy.sennott@gmail.com'
        mailing_list = 'false'
        url = ('http://developer.nrel.gov/api/solar/nsrdb_0512_download.csv?wkt=POINT'+
            '({lon}%20{lat})&names={year}&leap_day={leap}&interval={interval}&utc={utc}&'+
            'full_name={name}&email={email}&affiliation={affiliation}&mailing_list={mailing_list}&'+
            'reason={reason}&api_key={api}&attributes={attr}').format(year=year, lat=lat, lon=lon, 
                leap=leap_year, interval=interval, utc=utc, name=your_name, email=your_email, 
                mailing_list=mailing_list, affiliation=your_affiliation, reason=reason_for_use, 
                api=api_key, attr=attributes)
        
        # Return just the first 2 lines to get metadata:
        # info = pd.read_csv(url, nrows=1)
        # See metadata for specified properties, e.g., timezone and elevation
        # timezone, elevation = info['Local Time Zone'], info['Elevation']
        # Return all but first 2 lines of csv to get data:
        try:
            dfNSRDB = pd.read_csv(url, skiprows=2) 
        except urllib2.HTTPError as e:
            self.log.error(e)
            self.log.error(url)
            raise 
            
        # Set the time index in the pandas dataframe:
        dfNSRDB = dfNSRDB.set_index(pd.date_range('1/1/{yr}'.format(yr=year), freq=interval+'Min', periods=525600/int(interval)))
        dfNSRDB = dfNSRDB.reindex(dfNSRDB.index.rename('Time_UTC'))
        # Then drop the unnecessary columns
        dfNSRDB.drop(['Year','Month','Day','Hour','Minute'], axis=1, inplace=True)
        
        return dfNSRDB
    
                 
        
    def getLmpAll(self, startdate):
        """Returns 24 hours of DAM LMP data for all nodes in a multi-index dataframe"""
        
        # Convert datetime to string
        startdate_string = startdate.strftime('%Y%m%dT%H:%m-0000')
        # Build url
        url=('http://oasis.caiso.com/oasisapi/SingleZip?'+
             'queryname=PRC_LMP&'+
             'startdatetime='+startdate_string+'&'+ 
             'enddatetime=20150919T09:00-0000&'+ 
             'market_run_id=DAM&resultformat=6&version=1') 
            
        # Get data
        try:
            names, dfOasisLmp_list = self.getOasisUrl(url)
        except urllib2.HTTPError as e:
            self.log.error(e)
            return
        except TypeError as e:
            self.log.error(e)
            return
        
        # Loop through files (one each for each type of response)
        if self.debug: self.debug_oasis_data,self.debug_oasis_names = dfOasisLmp_list,names
        # TODO LATER there is a more elegant solution here that uses pd.pivot_table instead, maybe do later
        for index,content in enumerate(dfOasisLmp_list):
            # Get name of LMP component, rename file column
            value_name = '_'.join(names[index].split('_')[3:6])
            content.rename(columns={'MW':value_name},inplace=True)
            # Convert starttime to datetime, index on that and NODE
            content['Time_UTC']=pd.to_datetime(content['INTERVALSTARTTIME_GMT'])
            content = content.loc[:,['Time_UTC','NODE',value_name]]
            content.set_index(['NODE','Time_UTC'], inplace=True)
            # Assign back to original list
            dfOasisLmp_list[index] = content
        
        # Build final dataframe by joining each frame from list   
        dfLmpAll = dfOasisLmp_list[0].join(dfOasisLmp_list[1:])

        return dfLmpAll
        
        