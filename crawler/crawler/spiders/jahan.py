from __future__ import absolute_import
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy import Request, FormRequest
from lxml import html as lxhtml
from scrapy.utils.log import configure_logging
from datetime import datetime, timedelta
from crawler.loader import ScrapyLoader
from scrapy.loader.processors import MapCompose
import json
import sys
import re
import logging
import time
import traceback
from collections import OrderedDict
import xlsxwriter
import csv
import requests 
import shutil

class My_Spider(Spider):
    name = "jahan"
    download_timeout = 120
    start_urls = ["https://www.jahandiamondimports.com/"]
    custom_settings = {
        #"PROXY_ON": True,
        #"PROXY_PR_ON": True,
        #"PASSWORD": "1yocxe3k3sr3",
        "HTTPCACHE_ENABLED": True,
        "LOG_LEVEL":"INFO",
        #"FEED_EXPORT_ENCODING" : 'utf-8',
    }

    def parse(self, response):

        # yield Request(
        #     url="https://www.jahandiamondimports.com/audemars-piguet/audemars-piguet-volcano.html",
        #     callback=self.detail_page,
        #     meta={
        #         "cat_name":"AAA",
        #     },
        # )

        for item in response.xpath("//nav[@id='nav']//li/a[contains(@class,'level1 ')]"):
            main_cat = "".join(item.xpath("./../../../a//text()").getall())
            cat_name = "".join(item.xpath(".//text()").getall())
            if not cat_name:
                cat_name = ""

            cat_name = main_cat + ", " + cat_name

            state = True
            for sub_cat in item.xpath("./following-sibling::ul/li/a"):
                state = False
                sub_category = sub_cat.xpath("./text()").get()

                yield Request(
                    url=response.urljoin(sub_cat.xpath("./@href").get()),
                    callback=self.listing_page,
                    meta={
                        "cat_name":cat_name + ", " + sub_category,
                    }
                )
            
            if state:
                yield Request(
                    url=response.urljoin(item.xpath("./@href").get()),
                    callback=self.listing_page,
                    meta={
                        "cat_name":cat_name,
                    }
                )

        yield Request(
            url="https://www.jahandiamondimports.com/estate-jewelry.html",
            callback=self.listing_page,
            meta={
                "cat_name":"Estate Jewelry",
            }
        )


    product_count = 0
    def listing_page(self, response):
        cat_name = response.meta["cat_name"]
        if response.xpath("//li[contains(@class,'item last')]/a/@href").get():
            for item in response.xpath("//li[contains(@class,'item last')]/a/@href").getall():
                self.product_count += 1
                yield Request(
                    url=response.urljoin(item),
                    callback=self.detail_page,
                    meta={
                        "cat_name":cat_name,
                    },
                )

            next_page = response.xpath("//a[contains(@class,'next i-next')]/@href").get()
            if next_page:
                yield Request(
                    url=response.urljoin(next_page),
                    callback=self.listing_page,
                    meta={
                        "cat_name":cat_name,
                    },
                )
            else:
                print(f"TOTAL_COUNT :{self.product_count}")
    
    def detail_page(self, response):
        jahan_dict = {}

        sku = response.xpath("//div[@class='product-sku']/span/text()").get()
        if not sku:
            sku = ""

        title = response.xpath("//div[@class='product-name']/span/text()").get()
        description = "".join(response.xpath("//div[@class='short-description']//text()").getall())
        price = response.xpath("//span[@class='price']/text()").get()
        if price:
            price = price.replace("$", "").replace(",", "").strip()
        else:
            price = ""

        image_list = [x.replace("thumbnail", "image").replace("/90x", "") for x in response.xpath("//ul[@class='product-image-thumbs']/li/a/img/@src").getall()]
        if not image_list:
            image_list = [response.xpath("//a[contains(@class,'MagicZoomPlus desktop')]/img/@src").get()]


        jahan_dict["Handle"] = sku.strip().split("&")[0].replace(".", "-").replace(" ", "-").replace(",", "-").replace("/", "-").replace("_", "-").strip("-").replace("#", "")
        jahan_dict["Title"] = title if title else ""
        jahan_dict["Body (HTML)"] = "<p> " + description.replace("\n", "").replace("\xa0", "").replace("\t", "").replace("\r", "").strip() if description else "" + " </p>"
        jahan_dict["Vendor"] = "Jahan Diamonds"
        jahan_dict["Type"] = ""
        jahan_dict["Tags"] = response.meta["cat_name"]
        jahan_dict["Published"] = "TRUE"

        
        jahan_dict["Option1 Name"] = "Title"
        jahan_dict["Option1 Value"] = "Default Title"

        jahan_dict["Option2 Name"] = ""
        jahan_dict["Option2 Value"] = ""

        jahan_dict["Option3 Name"] = ""
        jahan_dict["Option3 Value"] = ""
        

        jahan_dict["Variant SKU"] = sku
        jahan_dict["Variant Grams"] = "0" #####
        jahan_dict["Variant Inventory Tracker"] = "shopify" 
        jahan_dict["Variant Inventory Qty"] = ""
        jahan_dict["Variant Inventory Policy"] = "continue" 
        jahan_dict["Variant Fulfillment Service"] = "manual"
        jahan_dict["Variant Price"] = price
        jahan_dict["Variant Compare at Price"] = ""
        jahan_dict["Variant Requires Shipping"] = "TRUE"
        jahan_dict["Variant Taxable"] = "TRUE"
        jahan_dict["Variant Barcode"] = ""

        
        for i in range(0, len(image_list)):
            if i == 0:
                jahan_dict["Image Src"] = image_list[i]
                jahan_dict["Image Position"] = str(i+1)
                jahan_dict["Image Alt Text"] = ""
                jahan_dict["Gift Card"] = "FALSE"
                jahan_dict["SEO Title"] = ""
                jahan_dict["SEO Description"] = ""
                jahan_dict["Google Shopping / Google Product Category"] = ""
                jahan_dict["Google Shopping / Gender"] = ""
                jahan_dict["Google Shopping / Age Group"] = ""
                jahan_dict["Google Shopping / MPN"] = ""
                jahan_dict["Google Shopping / AdWords Grouping"] = ""
                jahan_dict["Google Shopping / AdWords Labels"] = ""
                jahan_dict["Google Shopping / Condition"] = ""
                jahan_dict["Google Shopping / Custom Product"] = ""
                jahan_dict["Google Shopping / Custom Label 0"] = ""
                jahan_dict["Google Shopping / Custom Label 1"] = ""
                jahan_dict["Google Shopping / Custom Label 2"] = ""
                jahan_dict["Google Shopping / Custom Label 3"] = ""
                jahan_dict["Google Shopping / Custom Label 4"] = ""
                jahan_dict["Variant Image"] = ""
                jahan_dict["Variant Weight Unit"] = "g"
                jahan_dict["Variant Tax Code"] = ""
                jahan_dict["Cost per item"] = ""
                jahan_dict["Status"] = "active"

                yield jahan_dict

            elif image_list[i]:
                multiple_image = {}
                multiple_image["Handle"] = jahan_dict["Handle"]
                multiple_image["Title"] = ""
                multiple_image["Body (HTML)"] = ""
                multiple_image["Vendor"] = ""
                multiple_image["Type"] = ""
                multiple_image["Tags"] = ""
                multiple_image["Published"] = ""
                multiple_image["Option1 Name"] = ""
                multiple_image["Option1 Value"] = ""
                multiple_image["Option2 Name"] = ""
                multiple_image["Option2 Value"] = ""
                multiple_image["Option3 Name"] = ""
                multiple_image["Option3 Value"] = ""
                multiple_image["Variant SKU"] = ""
                multiple_image["Variant Grams"] = ""
                multiple_image["Variant Inventory Tracker"] = ""
                multiple_image["Variant Inventory Qty"] = ""
                multiple_image["Variant Inventory Policy"] = ""
                multiple_image["Variant Fulfillment Service"] = ""
                multiple_image["Variant Price"] = ""
                multiple_image["Variant Compare at Price"] = ""
                multiple_image["Variant Requires Shipping"] = ""
                multiple_image["Variant Taxable"] = ""
                multiple_image["Variant Barcode"] = ""
                multiple_image["Image Src"] = image_list[i]
                multiple_image["Image Position"] = str(i+1)
                multiple_image["Image Alt Text"] = ""
                multiple_image["Gift Card"] = ""
                multiple_image["SEO Title"] = ""
                multiple_image["SEO Description"] = ""
                multiple_image["Google Shopping / Google Product Category"] = ""
                multiple_image["Google Shopping / Gender"] = ""
                multiple_image["Google Shopping / Age Group"] = ""
                multiple_image["Google Shopping / MPN"] = ""
                multiple_image["Google Shopping / AdWords Grouping"] = ""
                multiple_image["Google Shopping / AdWords Labels"] = ""
                multiple_image["Google Shopping / Condition"] = ""
                multiple_image["Google Shopping / Custom Product"] = ""
                multiple_image["Google Shopping / Custom Label 0"] = ""
                multiple_image["Google Shopping / Custom Label 1"] = ""
                multiple_image["Google Shopping / Custom Label 2"] = ""
                multiple_image["Google Shopping / Custom Label 3"] = ""
                multiple_image["Google Shopping / Custom Label 4"] = ""
                multiple_image["Variant Image"] = ""
                multiple_image["Variant Weight Unit"] = ""
                multiple_image["Variant Tax Code"] = ""
                multiple_image["Cost per item"] = ""
                multiple_image["Status"] = ""

                yield multiple_image
            
            else:
                break
