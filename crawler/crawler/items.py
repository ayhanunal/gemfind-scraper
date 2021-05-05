# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    UPC = scrapy.Field()
    Brand = scrapy.Field()
    Model = scrapy.Field()
    Price = scrapy.Field()
    ProductName = scrapy.Field()
    RetailerSKU = scrapy.Field()
    RatingsNumber = scrapy.Field()
    RatingsAverage = scrapy.Field()
    Availability = scrapy.Field()
    Quantity = scrapy.Field()
    ProductWeight = scrapy.Field()
    ProductDimensions = scrapy.Field()
    ScrapedDescription = scrapy.Field()
    Offer = scrapy.Field()
    DateFirstAvailable = scrapy.Field()
    Warranty = scrapy.Field()
    Vendor = scrapy.Field()
    Images = scrapy.Field()
    URL = scrapy.Field()
    AmazonSalesRanking = scrapy.Field()
    


