# Wordart project

## Description
Beginning stages of project to read various data source API's and use *amueller's* excellent [WordCloud project](https://github.com/amueller/word_cloud) to visualize them

## Example
See [example notebook](https://github.com/tsennott/etc_projects/blob/master/Wordart/wordart_example.ipynb) 


## Status
* Classes for reading NYT API and Twitter API implemented in `words_api.py`
* Class for plotting, image handling and wordcloud with improvements in `words_visualization.py`
* Basic example implementation in `wordart_example.ipynb`
* Script for refreshed twitter wallpaper included `example_twitter_desktop_background.py`

## Notes
* This is using an older version of `WordCloud` than currently referenced in their docs, and so mask images must be RBG with white areas definining areas *not* to draw to
