#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains classes that visualize text data
"""

from PIL import Image, ImageOps, ImageEnhance
from wordcloud import WordCloud, STOPWORDS

import numpy as np
import os


class WordArt:
    """Class for creating word art images"""

    def create_mask_image(self, image_location, invert=False, rotate=0, 
                            transparency=False, max_size=None):
        """Creates a proper image with white areas for masking"""

        # open image
        image = Image.open(image_location)
        
        # resize if required
        if max_size is not None:
            resize_factor = min([1, max_size[0]/image.size[0], max_size[1]/image.size[1]])
            image = image.resize((int(image.size[0]*resize_factor), 
                                    int(image.size[1]*resize_factor)))

        # rotate if required
        image = image.rotate(rotate, expand=True)
        
        # paste onto white background 
        if transparency:
            background = Image.new(size=image.size, color='white', mode='RGBA')
            image = image.convert(mode='RGBA')
            image = Image.alpha_composite(background, image)

        # convert and invert if necessary    
        image = image.convert(mode='L')
        if invert: image = ImageOps.invert(image)
        image = ImageEnhance.Contrast(image).enhance(10)

        return image

    def make_wordcloud(self, text, filename, image=None, colormap='viridis', 
                       background='white', max_words=4000, additional_stopwords=None,
                      desktop=False, max_size=(1920, 1080), reset_stopwords=False,):
        """
        Makes a wordcloud, with added features like background fill and desktop modes.
        Can optionally add additional stopwords (words to keep out of image), or 
        reset the standard stopwords and define them all yourself.
        """

        # import and add stopwords
        if not reset_stopwords:    
            stopwords = set(STOPWORDS)
            stopwords_to_add = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                                'Friday', 'Saturday', 'Sunday']
        else:
            stopwords=set([])
            stopwords_to_add = []
        if additional_stopwords is not None:
            stopwords_to_add.extend(additional_stopwords)
            for word in stopwords_to_add: 
                stopwords.add(word)

        # join to single string if necessary
        if isinstance(text, list):
            text = ' '.join(text)

        if image is not None:
            # resize
            resize_factor = min([1, max_size[0]/image.size[0], max_size[1]/image.size[1]])
            image = image.resize((int(image.size[0]*resize_factor), 
                                    int(image.size[1]*resize_factor)))
            
            #define mask as np array
            mask = np.array(image)
        
            # initialize wordcloud
            wc = WordCloud(background_color="white", max_words=max_words, mask=mask,
                           stopwords=stopwords, colormap=colormap)

            # generate word cloud
            wc.generate(text)

            # store to file
            wc.to_file(filename)
            wc_image = Image.open(filename)
            
            # apply background 
            background_image = Image.new(size=image.size, color=background, mode='RGB')
            mask = ImageOps.invert(image.convert(mode='L'))
            wc_w_background = Image.composite(wc_image, background_image, mask)
            background_filename = filename.split('.')[0] + '_backgound' + '.' + filename.split('.')[1]
            wc_w_background.save(background_filename)
                
            # make desktop friendly version
            if desktop:
                
                # resize for desktop (in case previous image was too large)
                resize_factor = min([1, 1920/image.size[0], 1080/image.size[1]])
                image = image.resize((int(image.size[0]*resize_factor), 
                                        int(image.size[1]*resize_factor)))
                
                # make image
                desktop = Image.new(size=(1920, 1080), color=background, mode='RGB')
                offset = ((1920 - wc_w_background.size[0]) // 2, (1080 - wc_w_background.size[1]) // 2)
                desktop.paste(wc_w_background, offset)
                desktop_filename = filename.split('.')[0] + '_desktop' + '.' + filename.split('.')[1]
                desktop.save(desktop_filename)   
                
                # clean up 
                os.remove(background_filename)
                wc_image.close()
                os.remove(filename)

                return desktop
            
            # clean up files
            wc_image.close()
            os.remove(filename)
                    
            return wc_w_background

        else:
            # initialize wordcloud
            wc = WordCloud(background_color=background, max_words=max_words,
                           stopwords=stopwords, colormap=colormap)

            # generate word cloud
            wc.generate(text)

            # store to file
            filename = filename.split('.')[0] + '_' + colormap + '.' + filename.split('.')[1]
            wc.to_file(filename)
            wc_image = Image.open(filename)

            return wc_image

