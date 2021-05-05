# -*- coding: utf-8 -*-

from scrapy.exceptions import DropItem
# from scrapy.pipelines.images import ImagesPipeline
from scrapy import Request
from datetime import datetime
from nameparser import HumanName
import string
import re, os
import scrapy
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
        
from scrapy.exporters import JsonLinesItemExporter
from scrapy.exporters import CsvItemExporter
#from scrapy_xlsx import XlsxItemExporter
from scrapy import signals
#from scrapy.xlib.pydispatch import dispatcher


class CrawlerPipeline(object):

    def __init__(self):
        self.ids_seen = set()
    
    def process_item(self, item, spider):
        
        for field in item.keys():
            if item[field]:
                if isinstance(item[field], str):
                    item[field] = item[field].strip()
                
        if "Price" in item and item["Price"]:
            item["Price"] = item["Price"].replace("$", "").strip()
            
        if 'obit_name' in item:
            item["obit_name"] = item["obit_name"].strip()
            name = HumanName(item["obit_name"])
            
            item["title"] = name.title
            item["first"] = name.first
            item["middle"] = name.middle
            item["last"] = name.last
            item["suffix"] = name.suffix
            item["nickname"] = name.nickname
        
        return item

        
# class MyImagesPipeline(ImagesPipeline):
    
#     def file_path(self, request, response=None, info=None):
#         return request.meta.get('filename','')

#     def get_media_requests(self, item, info):
#         if os.path.exists("images/" + item["ImageName"]):
#             print "Already have image: " + item["ImageName"]
        
#         if not os.path.exists("images/" + item["ImageName"]) and "ImageUrl" in item and item["ImageUrl"]:
#             yield Request(item["ImageUrl"], meta={"filename": item["ImageName"]})

class HumanEmailPipeline(object):

    def __init__(self):
        self.ids_seen = set()
        
    def process_item(self, item, spider):
        
        for k in item.keys():
            if not item[k]:
                item[k] = ""
            item[k] = item[k].strip()
        
        if "FullName" in item:
                    
            item["FullName"] = item["FullName"].strip()
            name = HumanName(item["FullName"])
            
            item["First"] = name.first
            item["Middle"] = name.middle
            item["Last"] = name.last
        
        if "Email" in item and len(item["Email"].strip()) > 0:
            if item['Email'] in self.ids_seen:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.ids_seen.add(item['Email'])
        
        return item
        
class CustomImagePipeLine(ImagesPipeline):
    DEFAULT_IMAGES_URLS_FIELD = "image_url"
    @classmethod
    def from_crawler(cls, crawler):
        try:
            pipe = cls.from_settings(crawler)
        except AttributeError:
            pipe = cls()
        pipe.crawler = crawler
        return pipe

    @classmethod
    def from_settings(cls, crawler):
        settings = crawler.settings
        s3store = cls.STORE_SCHEMES['s3']
        s3store.AWS_ACCESS_KEY_ID = settings['AWS_ACCESS_KEY_ID']
        s3store.AWS_SECRET_ACCESS_KEY = settings['AWS_SECRET_ACCESS_KEY']
        s3store.POLICY = "public-read" # settings['IMAGES_STORE_S3_ACL']

        store_uri = settings.get("IMAGES_STORE")
        spider_name = crawler.spider.name
        return cls(store_uri, settings=settings, spider_name=spider_name)

    def __init__(self, *args, **kwargs):
        self.spider_name = kwargs.pop('spider_name', None)
        super(CustomImagePipeLine, self).__init__(*args, **kwargs)

    def get_media_requests(self, item, info):
        image_urls = item.get(self.images_urls_field, [])
        requests_list = []
        for idx, image_url in enumerate(image_urls.split(" | "), 0):
            request = Request(image_url, meta={
                "file_name": image_url.strip("/").split("/")[-1],
            })
            requests_list.append(request)
        return requests_list

    def file_path(self, request, response=None, info=None):
        path = "{}/{}".format(
            self.spider_name,
            request.meta['file_name']
        )
        return path

class MultiCSVItemPipeline(object):

    def __init__(self):
        dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        self.files = dict() # dict([ (name, open(CSVDir + name + '.csv','w+b')) for name in self.SaveTypes ])
        self.exporters = dict() # dict([ (name,CsvItemExporter(self.files[name])) for name in self.SaveTypes])
        # [e.start_exporting() for e in self.exporters.values()]

    def spider_closed(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]

    def process_item(self, item, spider):
        file_key = item.get("file_key", "default").lower().replace(" ", "_").replace("!", "").replace("-", "_").replace(",", "")
        
        if file_key not in self.files:
            self.files[file_key] = open('output/' + file_key + '.json','w+b')
            # self.files[file_key] = open("output/" + file_key + ".xlsx", "w+b")
        if file_key not in self.exporters:
            self.exporters[file_key] = JsonLinesItemExporter(self.files[file_key])
            self.exporters[file_key].start_exporting()
        
        self.exporters[file_key].export_item(item)
        
        return item
