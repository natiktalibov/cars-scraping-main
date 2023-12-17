import os
import re
import json
import math
import scrapy
import datetime
from scrapy import Selector

import apify


class UnaaSpider(scrapy.Spider):
    name = 'unaa'
    start_urls = ['https://unaa.mn/suudlyin/?page=1']

    def __init__(self):
        # Some English translations corresponding to Mongolian
        self.fuel_dict = {"Хайбрид": "hybrid", "Бензин": "petrol", "Дизель": "diesel", "Цахилгаан": "electric",
                          "Газ": "gas"}
        self.transmission_dict = {"Автомат": "automatic", "Механик": "manual"}

    def parse(self, response):
        tree = Selector(response)  # create Selector
        data_list = tree.xpath("//div[@class='listing']/a/@href").extract()
        link_list = ["https://unaa.mn" + i for i in data_list]  # detail url list

        yield from response.follow_all(link_list, self.product_detail)

        # get last page
        total_data = tree.xpath("//div[@class='search__details-count']/span/text()").extract_first()
        last_page = math.ceil(int(total_data) / 31)

        if "page=" not in response.url:  # The URL of the first page needs special processing
            current_page = 1
            next_page = response.url + "?page=" + str(current_page + 1)
        else:
            current_page = int(str(response.url).split("page=")[1])
            next_page = response.url.split('page=')[0] + "page=" + str(current_page + 1)

        if int(current_page) + 1 < int(last_page) + 1:
            yield response.follow(next_page, self.parse)

    def product_detail(self, response):
        output = {}
        tree = Selector(response)  # create Selector

        output["make"] = tree.xpath("//div[@class='breadcrumbs__item']/@data-name").extract()[1]
        output["model"] = tree.xpath("//div[@class='breadcrumbs__item']/@data-name").extract()[2]

        form_data = tree.xpath("//div[@class='card__info']/div[@class='card__info-item']")
        for data in form_data:
            key = data.xpath('./div[@class="card__info-label"]/text()').extract_first().strip()
            value = data.xpath('./div[@class="card__info-value"]/text()').extract()
            value = [l.strip() for l in value if l.strip() != '']
            if "Үйлдвэрлэсэн он" in key:
                if value[0]:  ## by NT
                    output["registration_year"] = int(value[0])  ## by NT
            elif "Орж ирсэн он" in key:  ## by NT
                if value[0]:  ## by NT
                    output["year"] = int(value[0].split('-')[0])  ## by NT
            elif "Нөхцөл" in key:  ## by NT
                if value[0] == "Дугаартай":  ## by NT
                    output["is_used"] = "Yes"  ## by NT
            elif "Хөтлөгч" in key:  ## by NT
                if value[0] == "Бүх дугуйн":  ## by NT
                    output["drive_train"] = '4x4'  ## by NT
            elif "Хроп" in key:
                output["transmission"] = self.transmission_dict.get(value[0])
            elif "Хөд. багтаамж" in key:
                output["engine_displacement_value"] = value[0]
            elif "Моторын төрөл" in key:
                output["fuel"] = self.fuel_dict.get(value[0])
            elif "Гүйлт" in key:
                value = ' '.join(value[0].split())
                odometer_value = value.split(" ")[0]
                odometer_unit = value.split(" ")[1]
                output["odometer_value"] = int(odometer_value)
                output["odometer_unit"] = odometer_unit

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = 'Unaa'
        output["scraped_listing_id"] = re.findall("adv/(.*?)_", response.url, re.S)[0]
        output["vehicle_url"] = response.url
        output["country"] = "MN"
        city_url = tree.xpath('//div[@class="card__details"]/a/@href').extract_first()
        output["city"] = [i for i in city_url.split("/") if i != ""][-1]
        price = tree.xpath('//div[@class="card__price"]/text()').extract_first().replace(",", ".")
        if "сая" in price:  # "сая" represents millions
            price = float(price.replace("сая", "").strip()) * 1000000
        else:
            price = float(price.strip())
        output["price_retail"] = price
        output["price_wholesale"] = price
        currency_symbol = tree.xpath('//div[@class="card__price"]/b/text()').extract_first()
        if "₮" in currency_symbol:
            output["currency"] = "MNT"

        picture_list = tree.xpath('//div[@class="card__content-wrapper"]//img/@src').extract()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        # process empty fields
        list1 = []
        list2 = []
        for k, v in output.items():
            if v or v == 0:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))

        # yield output
        apify.pushData(output)