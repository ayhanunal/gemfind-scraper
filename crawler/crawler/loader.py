from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from .items import CrawlerItem

def strip_newlines(x):
    return x.replace("\n"," ").strip().strip("\n").strip()
    
class ScrapyLoader(ItemLoader):
    default_item_class = CrawlerItem
    #default_input_processor = MapCompose(strip_newlines)
    default_output_processor = TakeFirst()
    
    UPC = MapCompose()
    Brand = MapCompose()
    Model = MapCompose()
    Price = MapCompose()
    ProductName = MapCompose()
    RetailerSKU = MapCompose()
    RatingsNumber = MapCompose()
    RatingsAverage = MapCompose()
    Availability = MapCompose()
    Quantity = MapCompose()
    ProductWeight = MapCompose()
    ProductDimensions = MapCompose()
    ScrapedDescription = MapCompose()
    Offer = MapCompose()
    DateFirstAvailable = MapCompose()
    Warranty = MapCompose()
    Vendor = MapCompose()
    Images = MapCompose()
    URL = MapCompose()
    AmazonSalesRanking = MapCompose()