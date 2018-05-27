#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script can be run periodically to create background 
images based on a search and a masking image
"""

import json
import sys
import os
from requests_oauthlib import OAuth1
from words_visualization import WordArt
from words_api import TwitterReader

if __name__ == '__main__':
    
    # grab settings file 
    settings_file = 'example_twitter_desktop_background_settings.json'

    # try to parse all settings
    try:
        settings = json.loads(open(settings_file).read())
        twitter_auth_dict = settings['twitter-auth']
        auth = OAuth1(client_key=twitter_auth_dict['client_key'],
                client_secret=twitter_auth_dict['client_secret'],
                resource_owner_key=twitter_auth_dict['resource_owner_key'],
                resource_owner_secret=twitter_auth_dict['resource_owner_secret'])
        directory = settings['directory_for_images']
        source_image = settings['image']['source']
        invert = settings['image']['invert']
        transparency = settings['image']['transparency']
        geo = settings['geo_to_use']
        tweet_limit = settings['tweet_limit']
        images_to_keep = settings['images_to_keep']

    except:
        print('Invalid settings file')
        sys.exit()


    # reader
    reader = TwitterReader(auth)
    reader.get_search_term(geo=geo, tweet_limit=tweet_limit)
    text_list = reader.build_list_from_sources(source_list=['tweets'])

    # get highest number file in list
    file_list = os.listdir(directory)
    file = sorted(file_list)[-1]
    try:
        i = int(file.split('.')[0].split('_desktop')[0])
    except:
        i = 0

    # increment i
    i += 1

    # make image and wordcloud
    wa= WordArt()
    image = wa.create_mask_image(source_image, transparency=transparency, invert=invert)

    wa.make_wordcloud(text=text_list, image=image,
                   filename=directory + '/' + '{:06d}'.format(i) + ".png", 
                   colormap='gist_earth', background='lightblue',
                   additional_stopwords=['https', 'co', 'amp', 'RT'], 
                   reset_stopwords=False, desktop=True)

    try:
        os.remove(directory + '/' + '{:06d}'.format(i-images_to_keep) + '_desktop.png')
    except:
        pass
