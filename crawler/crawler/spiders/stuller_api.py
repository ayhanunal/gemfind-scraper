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
# from botocore.exceptions import ClientError
#import boto3



class My_Spider(Spider):
    name = "stuller_api"
    download_timeout = 120
    start_urls = ["https://www.stuller.com/browse/3c-collection/ever-and-ever-collection"]
    
    custom_settings = {
        #"PROXY_ON": True,
        #"PROXY_PR_ON": True,
        #"PASSWORD": "1yocxe3k3sr3",
        "HTTPCACHE_ENABLED": True,
        "LOG_LEVEL":"INFO",
        #"FEED_EXPORT_ENCODING" : 'utf-8',
    }

    
    def parse(self, response):

        for item in response.xpath("//div[@class='caption']//a/@href").getall()[1:]:
            f_url = response.urljoin(item)
            yield Request(
                f_url,
                callback=self.get_stuller_API_parent,
            )
            
        
        next_page = response.xpath("//td[@class='nextPage']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )



    def get_stuller_API_parent(self, response):
        """
        bu kisim tum 3c kategorisine istek atar.
        url = "https://api.stuller.com/v2/products"
        payload="{\"CategoryIds\":[23618], \"Include\": [\"All\"], \"Filter\":[\"Orderable\",\"OnPriceList\"]}"
        """

        page_info = response.meta.get("page_info", "parent_product")
        stuller_dict = response.meta.get("stuller_dict", {})

        # parent_base_url = ""
        # if "ProductType" in stuller_dict and stuller_dict["ProductType"] == "1":
        #     parent_base_url = response.url

        script_data = response.xpath("//script[contains(.,'FlexibleDesign')]/text()").get()
        if script_data:
            data = json.loads(script_data.split("FlexibleDesign(")[1].split("var productDetailsViewModel")[0].strip().strip(");"))
            product_details = data["ProductDetails"]


            sku = product_details["Product"]["ItemNumber"]

            stuller_dict["IsActive"] = "A" if product_details["Product"]["IsActive"] else "I"


            stuller_dict["ProductName"] = product_details["Product"]["Title"]

            stuller_dict["ProductDescription"] = product_details["Product"]["Description"] if "Description" in product_details["Product"] else ""
            
            stuller_dict["BaseMetalType"] = ""
            stuller_dict["MetalType"] = ""
            stuller_dict["MetalColor"] = ""
            
            if "ProductType" in stuller_dict and stuller_dict["ProductType"] != "1":
                base_metal = product_details["ProductCustomizationViewModel"]["QualityDialogViewModel"]["SelectedMetalQuality"]["BaseMetal"]
                if base_metal:
                    stuller_dict["BaseMetalType"] = base_metal
                else:
                    base_metal = ""
                
                metal_type = product_details["ProductCustomizationViewModel"]["QualityDialogViewModel"]["SelectedQuality"]
                if metal_type:
                    metal_type = metal_type + " " + base_metal
                    stuller_dict["MetalType"] = metal_type.strip()
                
                metal_color = product_details["ProductCustomizationViewModel"]["QualityDialogViewModel"]["SelectedMetalQuality"]["ColorName"]
                if metal_color:
                    stuller_dict["MetalColor"] = metal_color
                
                centerstone_shape = product_details["Product"]["CenterStoneShape"]
                if centerstone_shape:
                    stuller_dict["GemstoneShape1"] = centerstone_shape
                
                stuller_dict["CustomAttribute"] = str(product_details["Product"]["CenterStoneSize"]) if "CenterStoneSize" in product_details["Product"] else ""
                stuller_dict["CustomAttributeLabel"] = ""
        



            stuller_dict["StyleNumber"] = product_details["Product"]["StyleNumber"] if "StyleNumber" in product_details["Product"] and product_details["Product"]["StyleNumber"] else ""


            series = product_details["Product"]["Series"]

            if page_info == "parent_product":

                selected_attributes = ""

                # for catalog_values in product_details["CatalogValues"]:
                #     if len(catalog_values["CatalogValueChoices"]) > 1 and catalog_values["CatalogLabel"] != "Quality":
                #         selected_attributes = selected_attributes + catalog_values["CatalogLabel"] + ", "
    
                metal_type = product_details["ProductCustomizationViewModel"]["QualityDialogViewModel"]["SelectedQuality"]
                if metal_type:
                    selected_attributes = selected_attributes + "Metal Type, "
                
                metal_color = product_details["ProductCustomizationViewModel"]["QualityDialogViewModel"]["SelectedMetalQuality"]["ColorName"]
                if metal_color:
                    selected_attributes = selected_attributes + "Metal Color, "
                
                centerstone_shape = product_details["Product"]["CenterStoneShape"]
                if centerstone_shape:
                    selected_attributes = selected_attributes + "Center Stone Shape, "

                centerstone_size = product_details["Product"]["CenterStoneSize"]
                if centerstone_size:
                    selected_attributes = selected_attributes + "Custom Attribute, "
                
                selected_attributes = selected_attributes + "Gemstone Shape"
                    
                

                #get api series
                url = "https://api.stuller.com/v2/products"
                headers = {
                    'Content-Type':'application/json',
                    'Accept':'application/json',
                    'Host':'api.stuller.com',
                    'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
                }
                payload="{\r\n    \"Series\": [\"" + series + "\"],\r\n    \"Include\": [\"ExcludeAll\"],\r\n    \"AdvancedProductFilters\": [{\r\n            \"Type\": \"Collection\",\r\n            \"Values\": [{\r\n                    \"Value\": \"EVER&EVER\"\r\n                }\r\n            ]\r\n        }\r\n    ]\r\n}"
                
                yield Request(
                    url = url,
                    callback = self.get_stuller_API_child,
                    body = payload,
                    method="POST",
                    headers = headers,
                    meta={"parent_SKU":sku, "series":series, "selected_attributes":selected_attributes}
                )
                return
        else:
            stuller_dict["IsActive"] = "I"
            sku = stuller_dict["DealerStockNumber"] if "DealerStockNumber" in stuller_dict else None
        
        url = f"https://api.stuller.com/v2/products?SKU={sku}"
        headers = {
            'Content-Type':'application/json',
            'Accept':'application/json',
            'Host':'api.stuller.com',
            'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
        }
        yield Request(
            url = url,
            callback = self.populate_item_for_API,
            #body = payload,
            method="GET",
            headers = headers,
            meta = {"stuller_dict":stuller_dict}
        )


    def get_stuller_API_child(self, response):


        parent_SKU = response.meta.get("parent_SKU")
        series = response.meta.get("series")
        selected_attributes = response.meta.get("selected_attributes")

        data = json.loads(response.body)

        for item in data["Products"]:
            stuller_dict = {}
            
            sku = item["SKU"] if "SKU" in item else None
            if sku and sku != parent_SKU:
                stuller_dict["DealerStockNumber"] = sku
                stuller_dict["ParentSKU"] = parent_SKU
                stuller_dict["ProductType"] = "0"
                stuller_dict["SelectedAttributes"] = ""
            elif sku:
                stuller_dict["DealerStockNumber"] = sku
                stuller_dict["ProductType"] = "1"
                stuller_dict["ParentSKU"] = ""
                stuller_dict["SelectedAttributes"] = selected_attributes
                stuller_dict["CustomAttributeLabel"] = "Center Stone Size" if "Custom Attribute" in selected_attributes else ""
                stuller_dict["CustomAttribute"] = ""
            else:
                stuller_dict["ProductType"] = ""
                stuller_dict["ParentSKU"] = ""
                stuller_dict["DealerStockNumber"] = None
                stuller_dict["SelectedAttributes"] = ""

            stuller_dict["ProductName"] = item["Description"] if "Description" in item else None
            stuller_dict["WholesaleBasePrice"] = str(item["Price"]["Value"]).replace(",","").strip() if "Price" in item else None,
            stuller_dict["PriceType"] = "2"
            
            product_id = str(item["Id"]) if "Id" in item else None

            group_id = str(item["DefaultProductGroupId"]) if "DefaultProductGroupId" in item else None
            if group_id and product_id:
                url = f"https://www.stuller.com/products/build/{series}/{product_id}/?groupId={group_id}"

                yield Request(
                    url = url,
                    callback=self.get_stuller_API_parent,
                    meta={"page_info":"child_product", "stuller_dict":stuller_dict}
                )
        
        if "NextPage" in data and data["NextPage"]:
            url = "https://api.stuller.com/v2/products"
            payload="{\"NextPage\":\"" + data["NextPage"] + "\"}"
            headers = {
                'Content-Type':'application/json',
                'Accept':'application/json',
                'Host':'api.stuller.com',
                'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
            }
            yield Request(
                url = url,
                callback = self.get_stuller_API_child,
                body = payload,
                method = "POST",
                headers = headers,
                meta={"parent_SKU":parent_SKU, "series":series, "selected_attributes":selected_attributes}
            )





    #all_sku = []
    def populate_item_for_API(self, response):
        stuller_dict = response.meta.get("stuller_dict",{})

        data = json.loads(response.body)
        if "Products" in data:
            for item in data["Products"]:
                if item:   #"Collection" in item and item["Collection"] == "EVER&EVER":

                    # if item["SKU"] in self.all_sku:
                    #     continue
                    
                    # self.all_sku.append(item["SKU"]) #to choose the unique ones

                    stuller_dict["RetailerStockNumber"] = ""

                    if "DealerStockNumber" not in stuller_dict:
                        stuller_dict["DealerStockNumber"] = item["SKU"]

                    #stuller_dict["StyleNumber"] = ""
                    
                    if "ProductName" not in stuller_dict:
                        stuller_dict["ProductName"] = item["Description"] #detayda title olarak alindi.

                    stuller_dict["WholesaleDescription"] = ""
                    #stuller_dict["ProductDescription"] = item["Description"]
                    categorie = ""
                    if "MerchandisingCategory1" in item:
                        categorie = categorie + item["MerchandisingCategory1"] + " - "
                        if "MerchandisingCategory2" in item:
                            categorie = categorie + item["MerchandisingCategory2"] + " - "
                            if "MerchandisingCategory3" in item:
                                categorie = categorie + item["MerchandisingCategory3"] + " - "
                                if "MerchandisingCategory4" in item:
                                    categorie = categorie + item["MerchandisingCategory4"]

                    stuller_dict["Categories"] = categorie
                    stuller_dict["Collections"] = item["Collection"] if "Collection" in item else ""
                    #stuller_dict["PriceType"] = "" #

                    if "WholesaleBasePrice" not in stuller_dict:
                        stuller_dict["WholesaleBasePrice"] = str(item["Price"]["Value"]).replace(",","").strip() if "Price" in item else None,
                        stuller_dict["PriceType"] = "2"

                    stuller_dict["MSRP"] = ""
                    stuller_dict["DisplayOrder"] = ""

                    """
                    #isActive detay page de alindi.
                    if "Orderable" in item and item["Orderable"]:
                        Orderable = "A"
                    elif "Orderable" in item and item["Orderable"] == False:
                        Orderable = "I"
                    else:
                        Orderable = ""
                    stuller_dict["IsActive"] = Orderable
                    """
                    
                    """
                    if "DescriptiveElementGroup" in item and "DescriptiveElements" in item["DescriptiveElementGroup"]:
                        selected_attributes = ""
                        for element in item["DescriptiveElementGroup"]["DescriptiveElements"]:
                            if element["Name"] not in ["Description", "Product", "Series"]:
                                selected_attributes = selected_attributes + element["Name"] + ", " 
                            if element["Name"] == "Quality":
                                metal_info = element["DisplayValue"]
                                if len(metal_info.split(" ")) > 1:
                                    stuller_dict["MetalType"] = " ".join(metal_info.split(" ")[0:-1]).strip()
                                    stuller_dict["MetalColor"] = metal_info.split(" ")[-1].strip()
                                else:
                                    if metal_info.isalpha():
                                        stuller_dict["MetalType"] = metal_info
                                    else:
                                        stuller_dict["MetalColor"] = metal_info
                    
                        
                        stuller_dict["SelectedAttributes"] = selected_attributes.strip()
                    """

                    if "CustomAttribute" not in stuller_dict or stuller_dict["CustomAttribute"] == "None":
                        stuller_dict["CustomAttribute"] = item["CenterStoneSize"] if "CenterStoneSize" in item else "None"

                            
                    stuller_dict["Status"] = item["Status"]
                    stuller_dict["DiscountLevelType2"] = "" 
                    stuller_dict["DiscountLevelValue2"] = "" 
                    stuller_dict["RetailPrice"] = "" 
                    stuller_dict["IsCalculatedRetailPrice"] = "" #
                    stuller_dict["MSRP2"] = "" 
                    stuller_dict["MSRPTypeID"] = "" #
                    stuller_dict["IsCalculatedMSRP"] = "" #
                    stuller_dict["DeliveryTime"] = "" 

                    if "Images" in item:
                        for i in range(len(item["Images"])):
                            if i == 0:
                                stuller_dict["ImagePath"] = item["Images"][i]["FullUrl"]
                                stuller_dict["ImageLabel"] = "" 
                                continue
                            
                            stuller_dict[f"ImagePath{i+1}"] = item["Images"][i]["FullUrl"]
                            stuller_dict["ImageLabel2"] = "" 
                        stuller_dict["IsImageFromURL"] = "1"
                    
                    # stuller_dict["ImagePath2"] = "" 
                    # stuller_dict["ImageLabel2"] = "" 
                    # stuller_dict["ImagePath3"] = "" 
                    # stuller_dict["ImageLabel3"] = "" 
                    # stuller_dict["ImagePath4"] = "" 
                    # stuller_dict["ImageLabel4"] = "" 
                    # stuller_dict["ImagePath5"] = "" 
                    # stuller_dict["ImageLabel5"] = "" 
                    # stuller_dict["ImagePath6"] = "" 
                    # stuller_dict["ImageLabel6"] = "" 
                    # stuller_dict["ImagePath7"] = "" 
                    # stuller_dict["ImageLabel7"] = "" 
                    # stuller_dict["ImagePath8"] = "" 
                    # stuller_dict["ImageLabel8"] = "" 
                    # stuller_dict["ImagePath9"] = "" 
                    # stuller_dict["ImageLabel9"] = "" 
                    # stuller_dict["ImagePath10"] = "" 
                    # stuller_dict["ImageLabel10"] = "" 
                    # stuller_dict["ImagePath11"] = "" 
                    # stuller_dict["ImageLabel11"] = "" 
                    # stuller_dict["ImagePath12"] = "" 
                    # stuller_dict["ImageLabel12"] = "" 
                    # stuller_dict["ImagePath13"] = "" 
                    # stuller_dict["ImageLabel13"] = "" 
                    # stuller_dict["ImagePath14"] = "" 
                    # stuller_dict["ImageLabel14"] = "" 
                    # stuller_dict["ImagePath15"] = "" 
                    # stuller_dict["ImageLabel15"] = "" 
                    # stuller_dict["ImagePath16"] = "" 
                    # stuller_dict["ImageLabel16"] = "" 
                    # stuller_dict["ImagePath17"] = "" 
                    # stuller_dict["ImageLabel17"] = "" 
                    stuller_dict["FlashFileCode"] = "" 
                    if "Videos" in item and len(item["Videos"]) > 0: 
                        stuller_dict["VideoURL"] = item["Videos"][0]["Url"]
                    stuller_dict["VideoType"] = ""
                    stuller_dict["AdditionalInformation"] = ""
                    stuller_dict["Style"] = ""
                    stuller_dict["FinishingTechnique"] = ""
                    stuller_dict["Setting"] = ""
                    stuller_dict["Gender"] = ""
                    stuller_dict["Width_mm"] = ""
                    stuller_dict["Thickness_mm"] = ""
                    stuller_dict["Length_in"] = ""
                    stuller_dict["Weight_gm"] = item["GramWeight"] if "GramWeight" in item else ""
                    stuller_dict["FingerSize"] = "" #str(item["RingSize"]) if "RingSize" in item and stuller_dict["ProductType"] != "1" else ""
                    stuller_dict["FingerSizeMinRange"] = ""
                    stuller_dict["FingerSizeMaxRange"] = ""
                    stuller_dict["MatchingSKUs"] = ""
                    stuller_dict["UpSellSKUs"] = ""
                    stuller_dict["GroupedProductSKUs"] = ""
                    stuller_dict["MetalMarket"] = ""
                    stuller_dict["Quantity"] = ""
                    stuller_dict["Period"] = ""
                    stuller_dict["HallMark"] = ""
                    stuller_dict["Condition"] = ""
                    stuller_dict["RoundMinimumCarat"] = "" #
                    stuller_dict["RoundMaximumCarat"] = "" #
                    stuller_dict["AsscherMinimumCarat"] = ""
                    stuller_dict["AsscherMaximumCarat"] = ""
                    stuller_dict["MarquiseMinimumCarat"] = ""
                    stuller_dict["MarquiseMaximumCarat"] = ""
                    stuller_dict["EmeraldMinimumCarat"] = ""
                    stuller_dict["EmeraldMaximumCarat"] = ""
                    stuller_dict["CushionMinimumCarat"] = ""
                    stuller_dict["CushionMaximumCarat"] = ""
                    stuller_dict["PearMinimumCarat"] = ""
                    stuller_dict["PearMaximumCarat"] = ""
                    stuller_dict["HeartMinimumCarat"] = ""
                    stuller_dict["HeartMaximumCarat"] = ""
                    stuller_dict["PrincessMinimumCarat"] = ""
                    stuller_dict["PrincessMaximumCarat"] = ""
                    stuller_dict["OvalMinimumCarat"] = ""
                    stuller_dict["OvalMaximumCarat"] = ""
                    stuller_dict["RadiantMinimumCarat"] = ""
                    stuller_dict["RadiantMaximumCarat"] = ""
                    # if "SKU" in item and "P" in item["SKU"].split(":"):
                    #     product_type = "1"
                    # elif "SKU" in item:
                    #     product_type = "0"
                    # else:
                    #     product_type = ""
                    # stuller_dict["ProductType"] = product_type
                    #stuller_dict["ParentSKU"] = item["SKU"][item["SKU"].find(":"):].lstrip(":")
                    stuller_dict["WatchBandMaterial"] = ""
                    stuller_dict["WatchBandType"] = ""
                    stuller_dict["WatchCaseMaterial"] = ""
                    stuller_dict["WatchCaseShape"] = ""
                    stuller_dict["WatchCrystalType"] = ""
                    stuller_dict["WatchBezel"] = ""
                    stuller_dict["WatchDialColor"] = ""
                    stuller_dict["WatchDisplayType"] = ""
                    stuller_dict["WatchMovement"] = ""
                    stuller_dict["WatchNumberType"] = ""
                    stuller_dict["WatchSize"] = ""
                    stuller_dict["WatchType"] = ""
                    stuller_dict["ComesPackagedIn"] = ""
                    stuller_dict["Warranty"] = ""
                    stuller_dict["WatchCondition"] = ""
                    stuller_dict["WatchManufactureDate"] = ""
                    stuller_dict["WatchCertification"] = ""
                    stuller_dict["WatchEnergy"] = ""
                    stuller_dict["WholesalePriceFactor"] = ""
                    stuller_dict["BaseMetalMarket"] = ""
                    stuller_dict["RelationalWholesalePrice"] = ""
                    stuller_dict["RelationalMarketBase"] = ""
                    stuller_dict["IsCalcualtedWholesale"] = "" #
                    stuller_dict["MetalLaborCode"] = ""
                    stuller_dict["OtherLaborCode"] = ""
                    stuller_dict["TotalDiamondWeight"] = "" #
                    stuller_dict["TotalGemstoneWeight"] = "" #
                    stuller_dict["VisibleAs"] = "" #
                    #stuller_dict["BaseMetalType"] = "" #
                    stuller_dict["BrouchureDate"] = ""
                    stuller_dict["StockFingerSize"] = ""
                    stuller_dict["StockLength"] = ""
                    # stuller_dict["CustomAttributeLabel"] = ""
                    # stuller_dict["CustomAttribute"] = "" #
                    stuller_dict["Dimensions"] = ""
                    stuller_dict["Terms"] = ""
                    stuller_dict["IsStockBalancing"] = "" #
                    stuller_dict["DiscountLevelType1"] = ""
                    stuller_dict["DiscountLevelValue1"] = ""
                    stuller_dict["RetailDiscountType"] = ""
                    stuller_dict["RetailDiscountValue"] = ""
                    stuller_dict["GemstoneType1"] = "" #
                    stuller_dict["NoOfGemstones1"] = "" #
                    #stuller_dict["GemstoneShape1"] = "" #
                    stuller_dict["GemstoneCaratWeight1"] = "" #
                    stuller_dict["GemstoneLotNo1"] = ""
                    stuller_dict["GemstoneLotCode1"] = ""
                    stuller_dict["GemstoneSettingLaborCode1"] = ""
                    stuller_dict["GemstoneDimensions1"] = ""
                    stuller_dict["GemstoneQuality1"] = "" #
                    stuller_dict["DiamondClarity1"] = ""
                    stuller_dict["DiamondColor1"] = ""
                    stuller_dict["FancyGemstoneColor1"] = ""
                    stuller_dict["FancyDiamondIntensity1"] = ""
                    stuller_dict["FancyDiamondOvertone1"] = ""
                    stuller_dict["CertificateType1"] = ""
                    stuller_dict["CertificateNumber1"] = ""
                    stuller_dict["Origin1"] = ""
                    stuller_dict["PearlType1"] = ""
                    stuller_dict["PearlShape1"] = ""
                    stuller_dict["PearlBodyColor1"] = ""
                    stuller_dict["PearlQuality1"] = ""
                    stuller_dict["PearlWidth1"] = ""
                    stuller_dict["AdditionalInfo1"] = "" #
                    stuller_dict["PearlSurfaceMarkingsAndBlemishes1"] = ""
                    stuller_dict["PearlLustre1"] = ""
                    stuller_dict["PearlUniformity1"] = ""
                    stuller_dict["DimensionUnitOfMeasure1"] = ""
                    stuller_dict["StoneCreationMethod1"] = ""
                    stuller_dict["StoneTreatmentMethod1"] = ""
                    stuller_dict["GemstoneType2"] = ""
                    stuller_dict["NoOfGemstones2"] = ""
                    stuller_dict["GemstoneShape2"] = ""
                    stuller_dict["GemstoneCaratWeight2"] = ""
                    stuller_dict["GemstoneLotNo2"] = ""
                    stuller_dict["GemstoneLotCode2"] = ""
                    stuller_dict["GemstoneSettingLaborCode2"] = ""
                    stuller_dict["GemstoneDimensions2"] = ""
                    stuller_dict["GemstoneQuality2"] = ""
                    stuller_dict["DiamondClarity2"] = ""
                    stuller_dict["DiamondColor2"] = ""
                    stuller_dict["FancyGemstoneColor2"] = ""
                    stuller_dict["FancyDiamondIntensity2"] = ""
                    stuller_dict["FancyDiamondOvertone2"] = ""
                    stuller_dict["CertificateType2"] = ""
                    stuller_dict["CertificateNumber2"] = ""
                    stuller_dict["Origin2"] = ""
                    stuller_dict["PearlType2"] = ""
                    stuller_dict["PearlShape2"] = ""
                    stuller_dict["PearlBodyColor2"] = ""
                    stuller_dict["PearlQuality2"] = ""
                    stuller_dict["PearlWidth2"] = ""
                    stuller_dict["AdditionalInfo2"] = ""
                    stuller_dict["PearlSurfaceMarkingsAndBlemishes2"] = ""
                    stuller_dict["PearlLustre2"] = ""
                    stuller_dict["PearlUniformity2"] = ""
                    stuller_dict["DimensionUnitOfMeasure2"] = ""
                    stuller_dict["StoneCreationMethod2"] = ""
                    stuller_dict["StoneTreatmentMethod2"] = ""
                    stuller_dict["GemstoneType3"] = ""
                    stuller_dict["NoOfGemstones3"] = ""
                    stuller_dict["GemstoneShape3"] = ""
                    stuller_dict["GemstoneCaratWeight3"] = ""
                    stuller_dict["GemstoneLotNo3"] = ""
                    stuller_dict["GemstoneLotCode3"] = ""
                    stuller_dict["GemstoneSettingLaborCode3"] = ""
                    stuller_dict["GemstoneDimensions3"] = ""
                    stuller_dict["GemstoneQuality3"] = ""
                    stuller_dict["DiamondClarity3"] = ""
                    stuller_dict["DiamondColor3"] = ""
                    stuller_dict["FancyGemstoneColor3"] = ""
                    stuller_dict["FancyDiamondIntensity3"] = ""
                    stuller_dict["FancyDiamondOvertone3"] = ""
                    stuller_dict["CertificateType3"] = ""
                    stuller_dict["CertificateNumber3"] = ""
                    stuller_dict["Origin3"] = ""
                    stuller_dict["PearlType3"] = ""
                    stuller_dict["PearlShape3"] = ""
                    stuller_dict["PearlBodyColor3"] = ""
                    stuller_dict["PearlQuality3"] = ""
                    stuller_dict["PearlWidth3"] = ""
                    stuller_dict["AdditionalInfo3"] = ""
                    stuller_dict["PearlSurfaceMarkingsAndBlemishes3"] = ""
                    stuller_dict["PearlLustre3"] = ""
                    stuller_dict["PearlUniformity3"] = ""
                    stuller_dict["DimensionUnitOfMeasure3"] = ""
                    stuller_dict["StoneCreationMethod3"] = ""
                    stuller_dict["StoneTreatmentMethod3"] = ""
                    stuller_dict["GemstoneType4"] = ""
                    stuller_dict["NoOfGemstones4"] = ""
                    stuller_dict["GemstoneShape4"] = ""
                    stuller_dict["GemstoneCaratWeight4"] = ""
                    stuller_dict["GemstoneLotNo4"] = ""
                    stuller_dict["GemstoneLotCode4"] = ""
                    stuller_dict["GemstoneSettingLaborCode4"] = ""
                    stuller_dict["GemstoneDimensions4"] = ""
                    stuller_dict["GemstoneQuality4"] = ""
                    stuller_dict["DiamondClarity4"] = ""
                    stuller_dict["DiamondColor4"] = ""
                    stuller_dict["FancyGemstoneColor4"] = ""
                    stuller_dict["FancyDiamondIntensity4"] = ""
                    stuller_dict["FancyDiamondOvertone4"] = ""
                    stuller_dict["CertificateType4"] = ""
                    stuller_dict["CertificateNumber4"] = ""
                    stuller_dict["Origin4"] = ""
                    stuller_dict["PearlType4"] = ""
                    stuller_dict["PearlShape4"] = ""
                    stuller_dict["PearlBodyColor4"] = ""
                    stuller_dict["PearlQuality4"] = ""
                    stuller_dict["PearlWidth4"] = ""
                    stuller_dict["AdditionalInfo4"] = ""
                    stuller_dict["PearlSurfaceMarkingsAndBlemishes4"] = ""
                    stuller_dict["PearlLustre4"] = ""
                    stuller_dict["PearlUniformity4"] = ""
                    stuller_dict["DimensionUnitOfMeasure4"] = ""
                    stuller_dict["StoneCreationMethod4"] = ""
                    stuller_dict["StoneTreatmentMethod4"] = ""
                    stuller_dict["GemstoneType5"] = ""
                    stuller_dict["NoOfGemstones5"] = ""
                    stuller_dict["GemstoneShape5"] = ""
                    stuller_dict["GemstoneCaratWeight5"] = ""
                    stuller_dict["GemstoneLotNo5"] = ""
                    stuller_dict["GemstoneLotCode5"] = ""
                    stuller_dict["GemstoneSettingLaborCode5"] = ""
                    stuller_dict["GemstoneDimensions5"] = ""
                    stuller_dict["GemstoneQuality5"] = ""
                    stuller_dict["DiamondClarity5"] = ""
                    stuller_dict["DiamondColor5"] = ""
                    stuller_dict["FancyGemstoneColor5"] = ""
                    stuller_dict["FancyDiamondIntensity5"] = ""
                    stuller_dict["FancyDiamondOvertone5"] = ""
                    stuller_dict["CertificateType5"] = ""
                    stuller_dict["CertificateNumber5"] = ""
                    stuller_dict["Origin5"] = ""
                    stuller_dict["PearlType5"] = ""
                    stuller_dict["PearlShape5"] = ""
                    stuller_dict["PearlBodyColor5"] = ""
                    stuller_dict["PearlQuality5"] = ""
                    stuller_dict["PearlWidth5"] = ""
                    stuller_dict["AdditionalInfo5"] = ""
                    stuller_dict["GroupParentSKU"] = ""
                    stuller_dict["ConfigurableControlType"] = ""
                    stuller_dict["ControlDisplayOrder"] = ""
                    stuller_dict["PearlSurfaceMarkingsAndBlemishes5"] = ""
                    stuller_dict["PearlLustre5"] = ""
                    stuller_dict["PearlUniformity5"] = ""
                    stuller_dict["DimensionUnitOfMeasure5"] = ""
                    stuller_dict["StoneCreationMethod5"] = ""
                    stuller_dict["StoneTreatmentMethod5"] = ""
                    stuller_dict["WeightUnit"] = item["WeightUnitOfMeasure"] if "WeightUnitOfMeasure" in item else ""
                    stuller_dict["MetalFactorCode"] = ""
                    stuller_dict["GPMCode"] = ""
                    stuller_dict["DiscountA"] = ""
                    stuller_dict["Qty1"] = ""
                    stuller_dict["Qty2"] = ""
                    stuller_dict["Qty3"] = ""
                    stuller_dict["Qty4"] = ""
                    stuller_dict["Qty5"] = ""
                    stuller_dict["FingerSizeIncrement"] = ""
                    stuller_dict["VendorName"] = ""
                    stuller_dict["RetailerBrandName"] = ""
                    stuller_dict["DimensionUnitOfMeasure"] = ""
                    stuller_dict["ClaspType"] = ""
                    stuller_dict["ChainType"] = ""
                    stuller_dict["BackFinding"] = ""
                    stuller_dict["AdditionalInformation2"] = ""
                    stuller_dict["ModifiedDate"] = "" #
                    stuller_dict["HasValidImage"] = "" #
                    stuller_dict["HasSideStones"] = ""
                    stuller_dict["ProngMetal"] = ""
                    stuller_dict["Rhodum"] = ""
                    stuller_dict["IsRingBuilder"] = "1"
                    stuller_dict["AmazonProduct"] = ""
                    stuller_dict["BulletPoint1"] = ""
                    stuller_dict["BulletPoint2"] = ""
                    stuller_dict["BulletPoint3"] = ""
                    stuller_dict["BulletPoint4"] = ""
                    stuller_dict["BulletPoint5"] = ""
                    stuller_dict["SecondaryMetalType"] = "" #

                    # if stuller_dict["ProductType"] == "1" and response.meta.get("parent_base_url") != "":
                    #     stuller_dict_parent = stuller_dict
                    #     stuller_dict_parent["ParentSKU"] = stuller_dict_parent["DealerStockNumber"]
                    #     stuller_dict_parent["ProductType"] = "0"
                    #     stuller_dict_parent["SelectedAttributes"] = ""
                    #     stuller_dict_parent["CustomAttributeLabel"] = ""
                    #     yield Request(
                    #         url=response.meta.get("parent_base_url"),
                    #         dont_filter=True,
                    #         callback=self.get_stuller_API_parent,
                    #         meta={
                    #             "parent_base_url" : "",
                    #             "page_info" : "child_product",
                    #             "stuller_dict" : stuller_dict_parent,
                    #         }
                    #     )


                    center_stone_shape = stuller_dict["GemstoneShape1"] if "GemstoneShape1" in stuller_dict else None
                    center_stone_size = stuller_dict["CustomAttribute"] if "CustomAttribute" in stuller_dict else None
                    if center_stone_size and center_stone_size != "None" and center_stone_shape:
                        center_stone_size = center_stone_size.split(" ")[0].strip()
                        min_max_carat = mm_carat_conversion(center_stone_size, center_stone_shape)

                        if min_max_carat:
                            stuller_dict["ToBeDeleted"] = ""
                            if "round" in center_stone_shape.lower():
                                stuller_dict["RoundMinimumCarat"] = min_max_carat
                                stuller_dict["RoundMaximumCarat"] = min_max_carat
                            elif "pear" in center_stone_shape.lower():
                                stuller_dict["PearMinimumCarat"] = min_max_carat
                                stuller_dict["PearMaximumCarat"] = min_max_carat
                            elif "marquise" in center_stone_shape.lower():
                                stuller_dict["MarquiseMinimumCarat"] = min_max_carat
                                stuller_dict["MarquiseMaximumCarat"] = min_max_carat
                            elif "oval" in center_stone_shape.lower():
                                stuller_dict["OvalMinimumCarat"] = min_max_carat
                                stuller_dict["OvalMaximumCarat"] = min_max_carat
                            elif "emerald" in center_stone_shape.lower():
                                stuller_dict["EmeraldMinimumCarat"] = min_max_carat
                                stuller_dict["EmeraldMaximumCarat"] = min_max_carat
                            elif "heart" in center_stone_shape.lower():
                                stuller_dict["HeartMinimumCarat"] = min_max_carat
                                stuller_dict["HeartMaximumCarat"] = min_max_carat
                            elif "princess" in center_stone_shape.lower():
                                stuller_dict["PrincessMinimumCarat"] = min_max_carat
                                stuller_dict["PrincessMaximumCarat"] = min_max_carat
                            elif "asscher" in center_stone_shape.lower():
                                stuller_dict["AsscherMinimumCarat"] = min_max_carat
                                stuller_dict["AsscherMaximumCarat"] = min_max_carat
                            elif "cushion" in center_stone_shape.lower():
                                stuller_dict["CushionMinimumCarat"] = min_max_carat
                                stuller_dict["CushionMaximumCarat"] = min_max_carat
                        else:
                            stuller_dict["ToBeDeleted"] = stuller_dict["CustomAttribute"] if "CustomAttribute" in stuller_dict else ""

                    yield stuller_dict 


        
        # if "NextPage" in data and (data["NextPage"] != "" or data["NextPage"] != None):
        #     url = "https://api.stuller.com/v2/products"
        #     payload="{\"NextPage\":\"" + data["NextPage"] + "\"}"
        #     headers = {
        #         'Content-Type':'application/json',
        #         'Accept':'application/json',
        #         'Host':'api.stuller.com',
        #         'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
        #     }
        #     yield Request(
        #         url = url,
        #         callback = self.parse,
        #         body = payload,
        #         method="POST",
        #         headers = headers,
        #     )
        



# def other_request(self, response):
    #     data = json.loads(response.body)
    #     count_col = 0
    #     count_top = 0
    #     if "Products" in data:
    #         for item in data["Products"]:
    #             count_top += 1
    #             if "Collection" in item:
    #                 count_col += 1
        
    #     if "NextPage" in data and (data["NextPage"] != "" or data["NextPage"] != None):
    #         url = "https://api.stuller.com/v2/products"
    #         payload="{\"NextPage\":\"" + data["NextPage"] + "\"}"
    #         headers = {
    #             'Content-Type':'application/json',
    #             'Accept':'application/json',
    #             'Host':'api.stuller.com',
    #             'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
    #         }
    #         yield Request(
    #             url = url,
    #             callback = self.deneme,
    #             body = payload,
    #             method="POST",
    #             headers = headers,
    #         )
        
    #     print("Collection",count_col)
    #     print("Toplam",count_top)
    
    
    # def jump(self, response):

    #     data = json.loads(response.body)
    #     if "Products" in data:
    #         for item in data["Products"]:
    #             sku = item["SKU"]
    #             url = f"https://api.stuller.com/v2/products?SKU={sku}"
    #             #payload="{\"SKU\":[\"" + sku + "\"],\"Include\":[\"All\"]}"
    #             headers = {
    #                 'Authorization':'Basic Z2VtZmluZDpQaHJlYWs0NCE=',
    #             }
    #             yield Request(
    #                 url = url,
    #                 callback = self.parse,
    #                 headers = headers,
    #             )
    #             break


def mm_carat_conversion(center_stone_size, center_stone_shape):

    center_stone_size = center_stone_size.replace("mm", "").lower().strip()

    round_con = { '1.0': '0.005', '1.15': '0.0067', '1.12': '0.0075', '1.3': '0.01', '1.5': '0.015', '1.7': '0.02', '1.8': '0.025', '2.0': '0.03', '2.1': '0.035', '2.2': '0.04', '2.4': '0.05', '2.5': '0.06', '2.7': '0.07', '2.8': '0.08', '2.9': '0.09', '3.0': '0.10', '3.1': '0.11', '3.2': '0.12', '3.3': '0.14', '3.4': '0.15', '3.5': '0.16', '3.6': '0.17', '3.7': '0.18', '3.8': '0.20', '3.9': '0.22', '4.0': '0.23', '4.1': '0.25', '4.2': '0.30', '4.4': '0.33', '4.6': '0.38', '4.8': '0.40', '5.0': '0.47', '5.2': '0.50', '5.4': '0.60', '5.6': '0.65', '5.8': '0.75', '6.0': '0.80', '6.4': '0.95', '6.5': '1.00', '6.6': '1.10', '6.8': '1.17', '7.0': '1.25', '7.2': '1.33', '7.4': '1.50', '7.6': '1.60', '7.8': '1.75', '8.0': '1.90', '8.2': '2.00', '8.4': '2.15', '8.6': '2.25', '8.8': '2.50', '9.0': '2.75', '9.2': '2.85', '9.4': '3.00', '9.6': '3.15', '9.8': '3.35', '10.0': '3.50', '10.2': '3.75', '10.4': '4.00', '10.6': '4.25', '10.8': '4.50', '11.0': '4.75', '12.0': '6.84', '11.2': '5.00'}
    pear_con = { '3.0 x 2.5': '0.09', '4.0 x 2.5': '0.12', '4.0 x 3.0': '0.14', '4.5 x 3.0': '0.16', '5.0 x 3.0': '0.20', '5.5 x 3.5': '0.30', '6.0 x 4.0': '0.50', '7.0 x 5.0': '0.75', '8.0 x 5.0': '1.00', '9.0 x 6.0': '1.50', '10.0 x 7.0': '2.00', '12.0 x 7.0': '2.50', '12.0 x 8.0': '3.00', '13.0 x 8.0': '3.50', '14.0 x 8.0': '4.00', '14.5 x 9.0': '4.50', '15.0 x 9.0': '5.00'}
    marquise_con = { '2.5 x 1.25': '0.035', '3.0 x 1.5': '0.04', '3.5 x 1.75': '0.06', '3.5 x 2.0': '0.07', '4.0 x 2.0': '0.09', '4.25 x 2.25': '0.11', '4.5 x 2.5': '0.14', '5.0 x 2.5': '0.15', '5.0 x 3.0': '0.20', '6.0 x 3.0': '0.25', '6.0 x 3.5': '0.30', '7.0 x 3.5': '0.38', '8.0 x 4.0': '0.50', '9.0 x 4.5': '0.75', '10.0 x 5.0': '1.00', '11.0 x 5.5': '1.50', '12.0 x 6.0': '2.00', '13.0 x 6.5': '2.50', '14.0 x 7.0': '3.00', '14.0 x 7.5': '3.50', '16.0 x 8.0': '4.00', '16.0 x 8.5': '5.00'}
    oval_con = { '4.0 x 3.0': '0.20', '5.0 x 3.0': '0.25', '5.0 x 4.0': '0.38', '6.0 x 4.0': '0.50', '7.0 x 5.0': '1.00', '7.5 x 5.5': '1.25', '8.0 x 6.0': '1.50', '8.5 x 6.5': '2.00', '9.0 x 7.0': '2.50', '10.0 x 8.0': '3.00', '10.0 x 8.5': '3.50', '11.0 x 9.0': '4.00', '11.0 x 9.5': '4.50', '12.0 x 10.0': '5.00'}
    emarald_con = { '5.0 x 3.0': '0.25', '6.0 x 4.0': '0.50', '6.5 x 4.5': '0.75', '7.0 x 5.0': '1.00', '8.0 x 6.0': '1.50', '8.5 x 6.5': '2.00', '9.0 x 7.0': '2.50', '10.0 x 8.0': '3.00', '11.0 x 9.0': '4.00', '12.0 x 10.0': '5.00'}
    heart_con = { '3.0 x 3.0': '0.12', '3.5 x 3.5': '0.20', '4.0 x 4.0': '0.25', '4.5 x 4.5': '0.38', '5.0 x 5.0': '0.50', '5.5 x 5.5': '0.65', '6.0 x 6.0': '0.75', '6.5 x 6.5': '1.00', '7.0 x 7.0': '1.50', '8.0 x 8.0': '2.00', '8.5 x 8.5': '2.50', '9.0 x 9.0': '3.00', '9.5 x 9.5': '3.50', '10.0 x 10.0': '4.00', '10.5 x 10.5': '4.50', '11.0 x 11.0': '5.00'}
    trillion_con = { '3.0 x 3.0 x 3.0': '0.10', '3.5 x 3.5 x 3.5': '0.14', '4.0 x 4.0 x 4.0': '0.18', '4.5 x 4.5 x 4.5': '0.25', '5.0 x 5.0 x 5.0': '0.33', '5.5 x 5.5 x 5.5': '0.40', '6.0 x 6.0 x 6.0': '0.50', '7.0 x 7.0 x 7.0': '1.50', '8.0 x 8.0 x 8.0': '2.00', '9.0 x 9.0 x 9.0': '2.50', '10.0 x 10.0 x 10.0': '3.00'}
    princess_con = {'1.5 x 1.5': '0.03', '1.75 x 1.75': '0.04', '2.0 x 2.0': '0.06', '2.25 x 2.25': '0.07', '2.5 x 2.5': '0.10', '2.75 x 2.75': '0.14', '3.0 x 3.0': '0.17', '3.25 x 3.25': '0.20', '3.5 x 3.5': '0.23', '3.75 x 3.75': '0.30', '4.0 x 4.0': '0.38'}


    result_value = ""
    if "round" in center_stone_shape.lower():
        if "x" not in center_stone_size:
            if str(float(center_stone_size)) in round_con:
                result_value = round_con[str(float(center_stone_size))]
        else:
            formatted_round = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_round = f"{v1} x {v2}"
            if formatted_round in marquise_con:
                result_value = marquise_con[formatted_round]
    
    elif "pear" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_pear = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_pear = f"{v1} x {v2}"
            if formatted_pear in pear_con:
                result_value = pear_con[formatted_pear]
        
    elif "marquise" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_marquise = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_marquise = f"{v1} x {v2}"
            if formatted_marquise in marquise_con:
                result_value = marquise_con[formatted_marquise]
    
    elif "oval" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_oval = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_oval = f"{v1} x {v2}"
            if formatted_oval in oval_con:
                result_value = oval_con[formatted_oval]
        else:
            if str(float(center_stone_size)) in round_con:
                result_value = round_con[str(float(center_stone_size))]
    
    elif "emerald" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_emerald = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_emerald = f"{v1} x {v2}"
            if formatted_emerald in emarald_con:
                result_value = emarald_con[formatted_emerald]

    
    elif "heart" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_heart = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_heart = f"{v1} x {v2}"
            if formatted_heart in heart_con:
                result_value = heart_con[formatted_heart]
    
    elif "princess" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_princess = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_princess = f"{v1} x {v2}"
            if formatted_princess in princess_con:
                result_value = princess_con[formatted_princess]

    elif "trillion" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_trillion = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            v3 = float(center_stone_size.lower().split("x")[2])
            formatted_trillion = f"{v1} x {v2} x {v3}"
            if formatted_trillion in trillion_con:
                result_value = trillion_con[formatted_trillion]
    
    elif "cushion" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_cushion = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1]) if center_stone_size.lower().split("x")[1] else "0.0"
            formatted_cushion = f"{v1} x {v2}"
            if formatted_cushion in princess_con:
                result_value = princess_con[formatted_cushion]
            elif formatted_cushion in marquise_con:
                result_value = marquise_con[formatted_cushion]
        else:
            if str(float(center_stone_size)) in round_con:
                result_value = round_con[str(float(center_stone_size))]
    
    elif "asscher" in center_stone_shape.lower():
        if "x" in center_stone_size:
            formatted_asscher = ""
            v1 = float(center_stone_size.lower().split("x")[0])
            v2 = float(center_stone_size.lower().split("x")[1])
            formatted_asscher = f"{v1} x {v2}"
            if formatted_asscher in princess_con:
                result_value = princess_con[formatted_asscher]
            elif formatted_asscher in marquise_con:
                result_value = marquise_con[formatted_asscher]
        else:
            if str(float(center_stone_size)) in round_con:
                result_value = round_con[str(float(center_stone_size))]

    
    return result_value
        

                

    


        


