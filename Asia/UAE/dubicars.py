import json
import datetime
import scrapy
from scrapy import Request, Selector
import apify


class DubicarsSpider(scrapy.Spider):
    name = 'dubicars'
    download_timeout = 120
    start_urls = ['https://www.dubicars.com/uae/used']

    def parse(self, response):
        url_list = response.xpath('//a[@class="image-container d-block"]/@href').getall()
        for url in url_list:
            yield Request(url=url, callback=self.get_data)

        # next page
        next_url = response.xpath('//a[@class="next"]/@href').get()
        if next_url is not None:
            yield Request(url=next_url, callback=self.parse)

    def get_data(self, response):
        sel = Selector(response)

        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('-')[-1].split('.')[0]
        output['scraped_from'] = 'Dubicars'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        output['country'] = 'AE'

        value = response.xpath('//span[@class="icon-location location"]/text()').get()
        if value is not None:
            output["city"] = value

        # get price
        price = response.xpath("//strong[@class='price text-primary']/text()").get()
        if price is not None and price.strip() != "Ask for price":
            price = price.strip()
            price_numeric = price.split(" ")[-1].strip()
            output['price_retail'] = float(int(price_numeric.replace(",", "")))
            output['currency'] = price.split(" ")[0]

        # get picture_list
        pictures = response.xpath('//ul[@class="slide-thumbnails"]/li/img/@src').getall()
        pictures_formatted = ['https:' + img for img in pictures]
        if pictures_formatted is not None and len(pictures_formatted) > 0:
            output['picture_list'] = json.dumps(pictures_formatted)

        # get details
        basic_ul = response.xpath('//ul[@class="faq__data"]')[0].get()
        li_list = Selector(text=basic_ul).xpath("//li").getall()

        for k in range(len(li_list)):
            key = Selector(text=li_list[k]).xpath('//span[1]/text()').get().strip()
            value = Selector(text=li_list[k]).xpath('//span[2]/text()').get()
            if value is None:
                value = Selector(text=li_list[k]).xpath('//a/span/text()').get()

            value = value.strip()

            if key == 'Make':
                output['make'] = value

            elif key == 'Model':
                output['model'] = value

            elif key == 'Color':
                output['exterior_color'] = value

            elif key == 'Vehicle type' and value:
                output['body_type'] = value

            elif key == 'Cylinders:' and value:
                output['engine_cylinders'] = int(value)

            elif key == 'Interior' and value:
                output['interior_color'] = value

        ul_list = response.xpath('//ul[@class="faq__data"]').getall()

        if len(ul_list) > 1:
            vehicle_options = []
            basic_ul = response.xpath('//ul[@class="faq__data"]')[1].get()
            interior_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
            for option in interior_options:
                if option == "Air conditioning":
                    output['ac_installed'] = 1
                elif option == "Leather seats":
                    output["upholstery"] = "leather"
                else:
                    vehicle_options.append(option)

            if len(ul_list) > 2:
                basic_ul = response.xpath('//ul[@class="faq__data"]')[2].get()
                exterior_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
                vehicle_options = vehicle_options + exterior_options

                if len(ul_list) > 3:
                    basic_ul = response.xpath('//ul[@class="faq__data"]')[3].get()
                    security_env_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
                    vehicle_options = vehicle_options + security_env_options

            if len(vehicle_options) > 0:
                output['vehicle_options'] = json.dumps(vehicle_options)

        highlights_ul = response.xpath('//ul[@class="dc-highlight-list"]//li').getall()
        for k in range(len(highlights_ul)):
            key = Selector(text=highlights_ul[k]).xpath('//span[1]/text()').get().strip()
            value = Selector(text=highlights_ul[k]).xpath('//span[2]/text()').get()
            if value is None:
                value = Selector(text=highlights_ul[k]).xpath('//a/span/text()').get()
            value = value.strip()
            if "Model Year" == key:
                output["year"] = int(value)
            elif key == "Kilometers":
                output["odometer_value"] = int(value.split(" ")[0].replace(",", ""))
                output['odometer_unit'] = value.split(" ")[-1]
            elif key == "Fuel Type":
                output["fuel"] = value

        apify.pushData(output)
import json
import datetime
import scrapy
from scrapy import Request, Selector
import apify


class DubicarsSpider(scrapy.Spider):
    name = 'dubicars'
    download_timeout = 120
    start_urls = ['https://www.dubicars.com/uae/used']

    def parse(self, response):
        url_list = response.xpath('//a[@class="image-container d-block"]/@href').getall()
        for url in url_list:
            yield Request(url=url, callback=self.get_data)

        # next page
        next_url = response.xpath('//a[@class="next"]/@href').get()
        if next_url is not None:
            yield Request(url=next_url, callback=self.parse)

    def get_data(self, response):
        sel = Selector(response)

        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('-')[-1].split('.')[0]
        output['scraped_from'] = 'Dubicars'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        output['country'] = 'AE'

        value = response.xpath('//span[@class="icon-location location"]/text()').get()
        if value is not None:
            output["city"] = value

        # get price
        price = response.xpath("//strong[@class='price text-primary']/text()").get()
        if price is not None and price.strip() != "Ask for price":
            price = price.strip()
            price_numeric = price.split(" ")[-1].strip()
            output['price_retail'] = float(int(price_numeric.replace(",", "")))
            output['currency'] = price.split(" ")[0]

        # get picture_list
        pictures = response.xpath('//ul[@class="slide-thumbnails"]/li/img/@src').getall()
        pictures_formatted = ['https:' + img for img in pictures]
        if pictures_formatted is not None and len(pictures_formatted) > 0:
            output['picture_list'] = json.dumps(pictures_formatted)

        # get details
        basic_ul = response.xpath('//ul[@class="faq__data"]')[0].get()
        li_list = Selector(text=basic_ul).xpath("//li").getall()

        for k in range(len(li_list)):
            key = Selector(text=li_list[k]).xpath('//span[1]/text()').get().strip()
            value = Selector(text=li_list[k]).xpath('//span[2]/text()').get()
            if value is None:
                value = Selector(text=li_list[k]).xpath('//a/span/text()').get()

            value = value.strip()

            if key == 'Make':
                output['make'] = value

            elif key == 'Model':
                output['model'] = value

            elif key == 'Color':
                output['exterior_color'] = value

            elif key == 'Vehicle type' and value:
                output['body_type'] = value

            elif key == 'Cylinders:' and value:
                output['engine_cylinders'] = int(value)

            elif key == 'Interior' and value:
                output['interior_color'] = value

        ul_list = response.xpath('//ul[@class="faq__data"]').getall()

        if len(ul_list) > 1:
            vehicle_options = []
            basic_ul = response.xpath('//ul[@class="faq__data"]')[1].get()
            interior_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
            for option in interior_options:
                if option == "Air conditioning":
                    output['ac_installed'] = 1
                elif option == "Leather seats":
                    output["upholstery"] = "leather"
                else:
                    vehicle_options.append(option)

            if len(ul_list) > 2:
                basic_ul = response.xpath('//ul[@class="faq__data"]')[2].get()
                exterior_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
                vehicle_options = vehicle_options + exterior_options

                if len(ul_list) > 3:
                    basic_ul = response.xpath('//ul[@class="faq__data"]')[3].get()
                    security_env_options = Selector(text=basic_ul).xpath("//li/span/text()").getall()
                    vehicle_options = vehicle_options + security_env_options

            if len(vehicle_options) > 0:
                output['vehicle_options'] = json.dumps(vehicle_options)

        highlights_ul = response.xpath('//ul[@class="dc-highlight-list"]//li').getall()
        for k in range(len(highlights_ul)):
            key = Selector(text=highlights_ul[k]).xpath('//span[1]/text()').get().strip()
            value = Selector(text=highlights_ul[k]).xpath('//span[2]/text()').get()
            if value is None:
                value = Selector(text=highlights_ul[k]).xpath('//a/span/text()').get()
            value = value.strip()
            if "Model Year" == key:
                output["year"] = int(value)
            elif key == "Kilometers":
                output["odometer_value"] = int(value.split(" ")[0].replace(",", ""))
                output['odometer_unit'] = value.split(" ")[-1]
            elif key == "Fuel Type":
                output["fuel"] = value

        apify.pushData(output)
