# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BaiduDoctorsItem(scrapy.Item):
    scholar_id = scrapy.Field()
    baidu_id = scrapy.Field()
    scholar_name = scrapy.Field()
    institution = scrapy.Field()
    discipline = scrapy.Field()
    stats = scrapy.Field()
    journal = scrapy.Field()
    cited_trend = scrapy.Field()
    ach_trend = scrapy.Field()


class BaiduEssaysItem(scrapy.Item):
    scholar_id = scrapy.Field()
    baidu_cited_num = scrapy.Field()
    source = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    authors = scrapy.Field()
    institutions = scrapy.Field()
    journal = scrapy.Field()
    abstract = scrapy.Field()
    keywords = scrapy.Field()
    DOI = scrapy.Field()
    publish_time = scrapy.Field()
