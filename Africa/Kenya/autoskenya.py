import scrapy
import os
import json
import datetime
from scrapy import Selector

import apify


class AutoskenyaSpider(scrapy.Spider):
    name = 'autoskenya'
    download_timeout = 120
    start_urls = ['https://www.autoskenya.com/buy-cars?page=1']

    def parse(self, response):
        tree = Selector(response)  # create Selector
        ads_lists = tree.xpath('//div[@class="ads-lists"]/a')
        link_list = ["https://www.autoskenya.com" + i.xpath('./@href').extract_first() for i in
                     ads_lists]  # detail url list
        yield from response.follow_all(link_list, self.product_detail)

        number_list = tree.xpath('//div[@class="pagination"]/a/text()').extract()  # page number list
        last_number = max([int(i) for i in number_list if i.isdigit()])  # get last page number
        current_page = int(str(response.url).split("page=")[1])
        pagination_links = [response.url.split('page=')[0] + f'page={i}' for i in
                            range(int(current_page) + 1, int(last_number + 1))]
        if int(current_page) + 1 < int(last_number + 1):
            yield from response.follow_all(pagination_links, self.parse)

    def product_detail(self, response):
        tree = Selector(response)  # create Selector

        car_data = tree.xpath('//div[@class="ad-title"]/h2/text()').extract_first()  # car brand and vehicle type
        vehicle_properties = tree.xpath(
            '//div[@class="vehicle-properties"]/div')  # a form containing multiple required data
        year = None
        transmission = None
        engine_displacement_value = None
        engine_displacement_units = None
        fuel = None
        odometer_value = None
        odometer_unit = None
        for i in vehicle_properties:
            if i.xpath('./div/span[1]/@title').extract_first() == "Year":
                year = i.xpath('./div/span[2]/text()').extract_first()  # the year may be empty and needs to be judged
                if year.isdigit():
                    year = int(year)
                if year == "N/A":
                    year = None
            elif i.xpath('./div/span[1]/@title').extract_first() == "Gearbox":  # transmission
                transmission = i.xpath('./div/span[2]/text()').extract_first()
            elif i.xpath("./div/span[1]/@title").extract_first() == "Engine":
                engine_displacement_value = i.xpath('./div/span[2]/text()').extract_first()
                if engine_displacement_value == "N/A":
                    engine_displacement_value = None
                else:  # if the data is not empty,two fields need to be processed
                    engine_displacement_units = engine_displacement_value.split(" ")[1]
                    engine_displacement_value = engine_displacement_value.split(" ")[0]
            elif i.xpath("./div/span[1]/@title").extract_first() == "Fuel Type":  # fuel
                fuel = i.xpath('./div/span[2]/text()').extract_first()
            elif i.xpath("./div/span[1]/@title").extract_first() == "Drive Type":  # by NT
                steering_position = i.xpath('./div/span[2]/text()').extract_first()  # by NT
            elif i.xpath("./div/span[1]/@title").extract_first() == "Condition":  # by NT
                is_used = i.xpath('./div/span[2]/text()').extract_first()  # by NT
            elif i.xpath("./div/span[1]/@title").extract_first() == "Color":  # by NT
                color = i.xpath('./div/span[2]/text()').extract_first()  # by NT
            elif i.xpath("./div/span[1]/@title").extract_first() == "Body Type":  # by NT
                body = i.xpath('./div/span[2]/text()').extract_first()  # by NT
            elif i.xpath("./div/span[1]/@title").extract_first() == "Air Con":  # by NT
                air_con = i.xpath('./div/span[2]/text()').extract_first()  # by NT
                if air_con == 'Yes':  # by NT
                    air_condis = 1  # by NT
                if air_con != 'Yes':  # by NT
                    air_condis = 0  # by NT
            elif i.xpath("./div/span[1]/@title").extract_first() == "Mileage":
                mileage = i.xpath('./div/span[2]/text()').extract_first()
                if "km" in mileage:  # Processing mileage value
                    odometer_value = int(mileage.split(" ")[0].replace(",", ""))
                    odometer_unit = mileage.split(" ")[1]

        picture_list = tree.xpath('//div[@class="carousel-wrapper"]//img/@src').extract()
        picture_list = [pic for pic in picture_list if pic.split(".")[-1] != "svg"]  # cars picture list
        city = tree.xpath("//div[@class='ad-title']/a//text()").extract_first()
        price_retail = tree.xpath(
            '//div[@class="back-wrapper"]//div[@class="ad-price"]/span/span/text()').extract_first()
        if price_retail:  # the price may be empty and needs to be judged
            price_retail = float(price_retail.replace(",", ""))
        currency = tree.xpath('//div[@class="back-wrapper"]//div[@class="ad-price"]/span/text()').extract_first()
        if currency and currency.strip().upper() == "KSH":  # the currency may be empty and needs to be judged
            currency = "KES"

        output = {
            "make": car_data.split(" ")[0],
            "model": car_data.split(" ")[1],
            "year": year,
            "transmission": transmission,
            "engine_displacement_value": engine_displacement_value,
            "engine_displacement_units": engine_displacement_units,
            "fuel": fuel,
            "exterior_color": color,  # by NT
            "body_type": body,  # by NT
            "ac_installed": air_condis,  # by NT
            "steering_position": steering_position,  # by NT
            "is_used": is_used,  # by NT
            "tpms_installed": 0,
            "scraped_date": datetime.datetime.isoformat(datetime.datetime.today()),
            "scraped_from": "Autoskenya",
            "scraped_listing_id": str(response.url).split("-")[-1],
            'odometer_value': odometer_value,
            'odometer_unit': odometer_unit,
            "vehicle_url": response.url,
            "picture_list": json.dumps(list(set(picture_list))),
            "city": city,
            "country": "KE",
            "price_retail": price_retail,
            "currency": currency,
        }
        list1 = []
        list2 = []
        for k, v in output.items():
            if v is not None:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))

        # yield output
        apify.pushData(output)