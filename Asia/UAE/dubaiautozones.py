import os
import re
import json
import scrapy
import datetime
from scrapy import Selector
import apify


class DubuySpider(scrapy.Spider):
    name = 'dubuy'
    start_urls = ['https://www.dubuy.com/egetFilteredProducts']

    def __init__(self):
        self.form_data = {"categoryId": "7517", "manufacturerId": "0", "vendorId": "0", "priceRangeFilterModel7Spikes": {"CategoryId": "7517", "ManufacturerId": "0", "VendorId": "0", "SelectedPriceRange": {}, "MinPrice": "1685", "MaxPrice": "492548"}, "attributeFiltersModel7Spikes": {"CategoryId": "7517", "ManufacturerId": "0", "VendorId": "0", "AttributeFilterGroups": [{"Id": 13, "FilterItems": [{"ValueId": "994210", "ProductVariantAttributeIds": ["17116"], "FilterItemState": "Unchecked"}, { "ValueId": "1042331", "ProductVariantAttributeIds": ["17116"], "FilterItemState":"Unchecked"}, {"ValueId": "1090452", "ProductVariantAttributeIds": ["17116"], "FilterItemState":"Unchecked"}, {"ValueId": "220100", "ProductVariantAttributeIds": ["89107"], "FilterItemState":"Unchecked"}, {"ValueId": "97116", "ProductVariantAttributeIds": ["89107"], "FilterItemState":"Unchecked"}, {"ValueId": "591778", "ProductVariantAttributeIds": ["17116"], "FilterItemState":"Unchecked"}, {"ValueId": "97117", "ProductVariantAttributeIds": ["89107"],"FilterItemState":"Unchecked"}]}]},"manufacturerFiltersModel7Spikes":{"CategoryId":"7517","ManufacturerFilterItems":[{"Id":"7267","FilterItemState":"Unchecked"},{"Id":"6232","FilterItemState":"Unchecked"},{"Id":"11887","FilterItemState":"Unchecked"},{"Id":"6010","FilterItemState":"Unchecked"},{"Id":"6890","FilterItemState":"Unchecked"},{"Id":"7870","FilterItemState":"Unchecked"},{"Id":"7906","FilterItemState":"Unchecked"},{"Id":"7357","FilterItemState":"Unchecked"},{"Id":"21486","FilterItemState":"Unchecked"},{"Id":"7447","FilterItemState":"Unchecked"}]},"vendorFiltersModel7Spikes":{"CategoryId":"7517","VendorFilterItems":[{"Id":"4353","FilterItemState":"Unchecked"},{"Id":"3599","FilterItemState":"Unchecked"},{"Id":"3576","FilterItemState":"Unchecked"},{"Id":"2214","FilterItemState":"Unchecked"},{"Id":"4228","FilterItemState": "Unchecked"}, {"Id": "3903", "FilterItemState": "Unchecked"}, {"Id": "3642", "FilterItemState": "Unchecked"}, {"Id": "3712", "FilterItemState": "Unchecked"}, {"Id": "3602", "FilterItemState": "Unchecked"}, {"Id": "4332", "FilterItemState": "Unchecked"}]}, "pageNumber": 1, "orderby": "15", "viewmode": "grid", "pagesize": 0, "queryString": "", "shouldNotStartFromFirstPage": True, "keyword":"","searchCategoryId": "0", "searchManufacturerId": "0", "searchVendorId": "0", "priceFrom": "", "priceTo": "", "includeSubcategories": "False", "searchInProductDescriptions": "False", "advancedSearch": "False", "isOnSearchPage": "False", "inStockFilterModel": None}
        self.headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
            }

    def start_requests(self):  # Post request
        yield scrapy.Request(self.start_urls[0], body=json.dumps(self.form_data), method='POST', headers=self.headers)

    def parse(self, response):
        tree = Selector(response)

        link_list = tree.xpath("//div[@class='item-grid']/div//a[@itemprop='url']/@href").extract()
        if link_list:
            link_list = ["https://www.dubuy.com" + i for i in link_list]
            yield from response.follow_all(link_list, self.product_detail)

            self.form_data["pageNumber"] = int(self.form_data["pageNumber"]) + 1  # Modify request parameters
            yield scrapy.Request(self.start_urls[0], body=json.dumps(self.form_data),
                                 method='POST', headers=self.headers, callback=self.parse)

    def product_detail(self, response):
        output = {}
        tree = Selector(response)
        try:
            make = tree.xpath("//div[@class='manufacturers']/span/a/text()").extract_first().capitalize()
            output['make'] = " ".join([i.capitalize() for i in make.split(" ")])
        except AttributeError:
            pass
        # from title get "model"
        try:
            output['model'] = tree.xpath("//div[@class='product-name']/h1/text()").extract_first().split(",")[0].replace(output['make'], '').strip()
        except AttributeError:
            pass

        output["make"] = response.xpath("//div[@class='font-normal text-sm tracking-wide leading-singleLine text-primary-500 cursor-pointer']/text()").get()
        url_split = response.url.split("/")[-2].split("-")
        for i, element in enumerate(url_split):
            if re.match("\d\dl", element):
                del url_split[i:]
                break


        url_split.remove(output["make"].lower())
        output["model"] = " ".join(url_split)


        output['year'] = response.url.split("/")[-2].split("-")[-2]
        form_data = tree.xpath("//div[@class='grid grid-cols-2 gap-x-4']/div/div")
        for data in form_data:
            key = data.xpath("./div[@class='text-gray-100 font-normal text-sm tracking-wide leading-singleLine flex-1']/text()").extract_first()
            value = data.xpath("./div[@class='text-gray-100 font-medium text-sm tracking-wide leading-singleLine flex-1 text-end']/text()").extract_first()
            if "VIN" in key:
                output['vin'] = value
            elif "Trim" in key:
                output['trim'] = value
            elif "Transmission" in key:
                output['transmission'] = value
            elif "Interior Trim" in key:
                output['upholstery'] = value
            elif "Engine" in key:
                output['engine_displacement_value'] = "".join([i for i in list(value) if i.isdigit() or i == "."])
                if output.get('engine_displacement_value'):
                    output['engine_displacement_units'] = "".join([i for i in list(value) if i.isalpha()])
            elif "Fuel Type" in key:
                output['fuel'] = value
            elif "Colour" == key:  ## by NT
                output['exterior_color'] = value  ## by NT
            elif "Interior Colour" in key:  ## by NT
                output['interior_color'] = value  ## by NT
            elif "Cylinders" in key:  ## by NT
                output['engine_cylinders'] = int(value.replace('V', ''))  ## by NT
            elif "Doors" in key:  ## by NT
                if value.isdigit():
                    output['doors'] = int(value)  ## by NT
                else:
                    output['doors'] = int(value.replace("+", "").replace("Doors", "").strip())  ## by NT
            elif "Steering " in key:  ## by NT
                output['steering_position'] = value  ## by NT
            elif "Specification" in key:  ## by NT
                output['vehicle_specification'] = value  ## by NT
            elif "Body Type" in key:  ## by NT
                output['body_type'] = value  ## by NT
            elif "Vehicle Condition" in key:  ## by NT
                if value == "Used":
                    output["is_used"] = 1
                else:
                    output["is_used"] = 0
            elif "Mileage" in key:
                if value:
                    output['odometer_value'] = int("".join([i for i in list(value) if i.isdigit()]))
                    output['odometer_unit'] = re.findall("\((.*?)\)", key, re.S)[0]


        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "Dubai Auto Zone’s"
        output['scraped_listing_id'] = response.url.split("/")[-2].split("-")[-1]
        output['vehicle_url'] = response.url
        output['city'] = 'Dubai'
        output['country'] = 'AE'
        output['price_retail'] = float(tree.xpath("//div[@class='text-gray-100 tracking-wide leading-singleLine text-[20px] min-w-max font-[500]']/text()").get().split(" ")[-1])
        output['currency'] = tree.xpath("//div[@class='text-gray-100 tracking-wide leading-singleLine text-[20px] min-w-max font-[500]']/text()").get().split(" ")[0]
        picture_list = tree.xpath("//a[@class='cloudzoom-gallery thumb-item']/@data-full-image-url").extract()
        if picture_list:
            output['picture_list'] = json.dumps(picture_list)

        apify.pushData(output)
