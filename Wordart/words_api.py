#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains classes that read from the NYT and twitter APIs 
and perform POS and sentiment analysis
"""

import requests
import json
import math
import time
import nltk
from matplotlib import pyplot as plt
import pandas as pd


class WordsAPI:
    """Base class for methods of investigating text from API sources"""

    def __init__(self):

        # define part of speech lists 
        self.pos = {}
        self.pos['all'] = ['CC','CD','DT','EX','FW','IN','JJ','JJR','JJS','LS','MD',
                        'NN','NNS','NNP','NNPS','PDT','POS','PRP','PRP$','RB','RBR',
                        'RBS','RP','TO','UH','VB','VBD','VBG','VBN','VBP','VBZ','WDT',
                        'WP','WP$','WRB',]
        self.pos['exclude'] = ['IN','TO','DT','CC',]
        self.pos['include'] = ['NNP','NNPS',]

        # define empty list where text sources will go
        self.text_sources = []

    def build_list_from_sources(self, source_list):
        """strings together all entities from source list"""
            
        text_list = []
        for source in source_list:
            try:
                text_list.extend(self.text_sources[source])
            except:
                print('Source not found: %s' % source)

        return text_list

    def df_from_text(self, source_list):
        """Creates df from a text source"""

        text_list = self.build_list_from_sources(source_list)
        corpus = ' '.join(text_list)    
        text = nltk.word_tokenize(corpus)
        poses = nltk.pos_tag(text)
        df = pd.DataFrame({'word': text, 'tuple': poses, 'pos': [x[1] for x in poses], 'count':1})
        return df[df['word'].str.isalpha()]


    def plot_top_words(self, df, title, pos_list=None, exclude=False, print_tuple=False, n=25):
        """Plots top words from df"""

        if print_tuple:
            to_group='tuple'
        else:
            to_group='word'
        
        fig,ax = plt.subplots(figsize=(15,3))    
        
        if pos_list is None:
            df.groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
        elif exclude==False:
            df[df['pos'].isin(pos_list)].groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
        elif exclude==True:
            df[~df['pos'].isin(pos_list)].groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
        
        # hide x label
        x_axis = ax.axes.get_xaxis()
        x_label = x_axis.get_label()
        x_label.set_visible(False)

        # show
        plt.title(title)
        plt.show()


    def get_words(self, source_list, pos_list=None, exclude=False):
        """Returns filtered list of words from df"""

        df = self.df_from_text(source_list)

        if pos_list is None:
            words = df['word'].tolist()
        elif exclude==False:
            words = df[df['pos'].isin(pos_list)]['word'].tolist()
        elif exclude==True:
            words = df[~df['pos'].isin(pos_list)]['word'].tolist()

        return words

    def search_source(self, source, search):
        try:
            text_list = self.text_sources[source]
        except:
            print('Source not found')

        for text in text_list:
            if search in text:
                print('Match: %s' % text)


class NYTReader(WordsAPI):
    """Reads NYT article API"""

    def __init__(self, key=None):

        # set auth
        if key is None: raise(Exception("must define API key"))
        
        #set url
        self.url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        
        #set initial params
        fields = 'web_url, snippet, lead_paragraph, headline, keywords, pub_date'
        self.params = {
            'api-key': key,
            'page': 0,
            'fl': fields,
        }

        super().__init__()


    def _get_request(self, params, limit, verbose):

        # store raw results and docs
        self.docs = []
        self.content_list = []

        # loop to read in aricles up to limit
        n = 0
        while n < limit:
            
            # fetch url and load result
            r = requests.get(self.url, params=params)
            try:
                content = json.loads(r.content)
                self.content_list.append(content)
                response = content['response']
                self.docs.extend(response['docs'])
            except:
                if verbose: print('Request failed, only got %d docs' % len(self.docs))
                break

            # get current n and increment the page number
            n = response['meta']['offset']
            params['page'] += 1

            # get reset limit if less than hits
            hits = int(math.ceil(response['meta']['hits']/10))
            limit = min(limit, hits)

            # sleep for a moment to stay within api limits
            time.sleep(0.5)


    def _buildText(self):
        """Gets different parts of the request list"""

        keywords = []
        headlines = []
        snippets = []
        
        for doc in self.docs:

            # get headlines and snippets
            headlines.append(doc['headline']['main'])
            snippets.append(doc['snippet']) 

            # get all keywords, if they exist and if they are not material type
            if len(doc['keywords']) > 0: 
                for keyword in  doc['keywords']:
                    if keyword['name'] not in ('type_of_material'):
                        keywords.append(keyword['value'])

        self.text_sources = {
            'headlines': headlines,
            'snippets': snippets,
            'keywords': keywords,
        }

    def get_search_term(self, search=None, article_limit=50, begin_date=None, end_date=None, verbose=False):
        """ Fetches results for a search string and/or a date range of string format YYYMMDD"""

        # setup params
        params = self.params
        if search is not None: params['fq'] = search
        if begin_date is not None: params['begin_date'] = begin_date
        if end_date is not None: params['end_date'] = end_date

        # fetch data
        self._get_request(params, article_limit, verbose)
        self._buildText()

        # print if verbose
        if verbose: print('Got %d docs, from %s to %s' % (len(self.docs), 
                                                            min([x['pub_date'] for x in self.docs]), 
                                                            max([x['pub_date'] for x in self.docs]))
                                                        )



class TwitterReader(WordsAPI):
    """Reads NYT article API"""

    def __init__(self, auth=None):

        # set auth
        if auth is None: raise(Exception("must pass authorization in form of OAuth1 object"))
        self.auth = auth

        #set urls
        self.url = 'https://api.twitter.com/1.1/search/tweets.json'
        self.recent_url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        self.rate_url = 'https://api.twitter.com/1.1/application/rate_limit_status.json'

        #set initial params
        self.params = {
            'count': 100,
        }
        super().__init__()


    def _get_request(self, url, params, tweet_limit, verbose):

        # store raw results in content
        self.content_list = []

        # temp containers for contents
        tweets = []
        names = []
        descriptions = []

        # loop to read in content up to limit
        n = 0
        next_link = ''
        limit = tweet_limit/100
        while n < limit:
            
            # fetch url and load result
            r = requests.get(url+next_link, auth=self.auth, params=params)
            try:
                content = json.loads(r.content)
                self.content_list.append(content)

                # parse results
                for tweet in content['statuses']:
                    tweets.append(tweet['text'])
                    names.append(tweet['user']['name'])
                    descriptions.append(tweet['user']['description']) 
            except:
                if verbose: print('Request failed, only got %d tweets' % len(tweets))
                break

            # get link to next results
            try:
                next_link = content['search_metadata']['next_results']
            except:
                if verbose: print('Only got %d results' % len(tweets))
                break
            
            n+=1

        self.text_sources = {
            'tweets': tweets,
            'names': names,
            'descriptions': descriptions,
        }


    def _get_streaming_request(self, url, params, tweet_limit, verbose):

        # store raw results in content
        self.content_list = []

        # temp containers for contents
        tweets = []
        names = []
        descriptions = []

        # open stream
        session = requests.Session()
        r = session.post(url, auth=self.auth, params=params, stream=True)

        # loop to read in content up to limit
        n = 0
        for line in r.iter_lines():

            # increment and break if reached limit
            n+=1
            if n > tweet_limit: break

            try:    
                response = json.loads(line)
                self.content_list.append(response)
                tweets.append(response['text'])
                names.append(response['user']['name'])
                descriptions.append(response['user']['description'])
            except:
                print('Stream exhausted, got %d tweets' % len(tweets))
                break

        self.text_sources = {
            'tweets': tweets,
            'names': names,
            'descriptions': descriptions,
        }



    def _get_api_limits(self):
        content = json.loads(requests.get(self.rate_url, auth=self.auth).content)
        remaining = content['resources']['search']['/search/tweets']['remaining']
        return remaining


    def _get_dates(self, streaming):
        dates = []
        if streaming:
            for tweet in self.content_list:
                dates.append(pd.to_datetime(tweet['created_at']))
        else:
            for content in self.content_list:
                for tweet in content['statuses']:
                    dates.append(pd.to_datetime(tweet['created_at']))
        return dates


    def _print_response(self, streaming=False):
        if len(self.text_sources['tweets']) > 0: 
            dates = self._get_dates(streaming)
            print('Got %d tweets from %s to %s, allowance now %d requests' % (
                                            len(self.text_sources['tweets']),
                                            str(min(dates)),
                                            str(max(dates)),
                                            self._get_api_limits())
                                            )
        else:
            print('No tweets')


    def get_search_term(self, search='', tweet_limit=200, geo=None, verbose=False, recent=False):
        """ Fetches results for a search string and optional geocode
            of string format LAT,LON,RADIUSmi """

        # setup params
        params = self.params
        params['q'] = search
        if geo is not None: params['geocode'] = geo
        if recent: params['result_type'] = 'recent'

        # fetch data
        self._get_request(self.url, params, tweet_limit, verbose)

        # print if verbose
        if verbose: self._print_response()

    def get_recent_tweets(self, tweet_limit=200, location_box=None, verbose=False):
        """Fetches recent tweets, with optional location defined as LON,LAT,LON,LAT 
        with SW corner coming first """

        # setup params
        params = self.params
        params.pop('count')
        if location_box is not None: params['locations'] = location_box
        
        # fetch data
        self._get_streaming_request(self.recent_url, params, tweet_limit, verbose)

        # print if verbose
        if verbose: self._print_response(streaming=True)
        
        
