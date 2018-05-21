#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains classes that read from the NYT API and visualize data
"""

import requests
import json
import math
import time
import nltk
from matplotlib import pyplot as plt
import pandas as pd

class NYTReader:
    """Reads NYT article API"""

    def __init__(self, key=None):

        # set initial params
        if key is None: raise(Exception("must define API key"))
        self.url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        fields = 'web_url, snippet, lead_paragraph, headline, keywords, pub_date'
        
        self.params = {
            'api-key': key,
            'page': 0,
            'fl': fields,
        }

        #define part of speech lists 
        self.pos = {}
        self.pos['all'] = ['CC','CD','DT','EX','FW','IN','JJ','JJR','JJS','LS','MD',
                        'NN','NNS','NNP','NNPS','PDT','POS','PRP','PRP$','RB','RBR',
                        'RBS','RP','TO','UH','VB','VBD','VBG','VBN','VBP','VBZ','WDT',
                        'WP','WP$','WRB',]
        self.pos['exclude'] = ['IN','TO','DT','CC',]
        self.pos['include'] = ['NNP','NNPS',]


    def _get_request(self, params, limit):

        # store raw results in docs
        self.docs = []

        # loop to read in aricles up to limit
        n = 0
        while n < limit:
            
            # fetch url and load result
            r = requests.get(self.url, params=params)
            response = json.loads(r.content)['response']
            self.docs.extend(response['docs'])

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

        self.keywords = []
        self.headlines = []
        self.snippets = []
        
        for doc in self.docs:

            # get headlines and snippets
            self.headlines.append(doc['headline']['main'])
            self.snippets.append(doc['snippet']) 

            # get all keywords, if they exist and if they are not material type
            if len(doc['keywords']) > 0: 
                for keyword in  doc['keywords']:
                    if keyword['name'] not in ('type_of_material'):
                        self.keywords.append(keyword['value'])


    def get_search_term(self, search, article_limit=50, begin_date=None, end_date=None):
        """ Fetches results for a search string"""

        # setup params
        params = self.params
        params['fq'] = search
        if begin_date is not None: params['begin_date'] = begin_date
        if end_date is not None: params['end_date'] = end_date

        # fetch data
        self._get_request(params, article_limit)
        self._buildText()


    def get_dates(self, begin_date=None, end_date=None, article_limit=50):
        """ Fetches results for a specific date, format YYYYMMDD"""

        # setup params
        params = self.params
        if begin_date is not None: params['begin_date'] = begin_date
        if end_date is not None: params['end_date'] = end_date

        # fetch data
        self._get_request(params, article_limit)  
        self._buildText()


    def df_from_text(self, text):
        """Creates df from a text source"""

        corpus = ' '.join(text)
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
        
        fig,ax = plt.subplots(figsize=(15,5))    
        
        if pos_list is None:
            df.groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
        elif exclude==False:
            df[df['pos'].isin(pos_list)].groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
        elif exclude==True:
            df[~df['pos'].isin(pos_list)].groupby(to_group).count()['count']\
                .sort_values(ascending=False)[0:n].plot.bar(ax=ax, color='C0')
            
        plt.title(title)
        plt.show()


    def get_words(self, df, pos_list=None, exclude=False):
        """Returns filtered string of text from df"""

        if pos_list is None:
            words = df['word'].tolist()
        elif exclude==False:
            words = df[df['pos'].isin(pos_list)]['word'].tolist()
        elif exclude==True:
            words = df[~df['pos'].isin(pos_list)]['word'].tolist()

        return words
