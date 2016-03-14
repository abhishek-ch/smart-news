# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class PywebcrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    crawled = scrapy.Field()
    source = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    desc = scrapy.Field()
    category = scrapy.Field()
    data = scrapy.Field()
    date = scrapy.Field()
    time = scrapy.Field()
    meta = scrapy.Field()
