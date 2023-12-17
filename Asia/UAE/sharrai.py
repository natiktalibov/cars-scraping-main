import re
import time
import scrapy
import os
import json
import datetime
import html

import apify


class SharraiSpider(scrapy.Spider):
    name = 'sharrai'
    page = 1

    def __init__(self):
        self.form_data = {
            "isFeatured": False, "limit": 4,
        }

    def start_requests(self):
        urls = [
            'https://api.sharrai.ae/home?page=1&secret_key=dtdab0YPxDZLj9eDJa2IGuzwOKclmKGt'
        ]
        for url in urls:
            yield scrapy.Request(url=url, body=json.dumps(self.form_data), method='POST', callback=self.parse)

    def parse(self, response):
        jsn = response.json()
        data = jsn["data"]
        for i in data:
            picture_list = []
            for f in i["images"]:
                picture_list.append(f["image"])

            output = {}

            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Sharrai Classified Ads'
            output['scraped_listing_id'] = i['uuid']
            output['vehicle_url'] = i["url"]
            output['picture_list'] = json.dumps(picture_list)
            output['country'] = 'AE'
            output['city'] = i['location']

            output['price_retail'] = float(i["price"])
            output['currency'] = 'AED'

            output['make'] = i["make"]["value"]
            output['model'] = i["model"]["value"]
            output['year'] = int(i["year"])
            output['transmission'] = i["transmission"]
            output['fuel'] = i["fuel"]
            output['odometer_value'] = int(i["mileage"])
            output['odometer_unit'] = 'km'
            output['exterior_color'] = i["color"]["value"]  ##by NT
            output["body_type"] = i["type"]["value"]  ##by NT
            output["doors"] = int(i["doors"])  ##by NT
            output["engine_cylinders"] = int(i["cylinders"])
            output['vehicle_disclosure'] = i["description"].replace('\n', '')  ##by NT

            # process empty fields
            list1 = []
            list2 = []
            for k, v in output.items():
                if v or v == 0:
                    list1.append(k)
                    list2.append(v)
            output = dict(zip(list1, list2))

            apify.pushData(output)
            # yield output

        # pagination
        if jsn["data"] != []:
            self.page += 1
            page_link = f'https://api.sharrai.ae/home?page={self.page}&secret_key=dtdab0YPxDZLj9eDJa2IGuzwOKclmKGt'
            time.sleep(1)

            yield response.follow(url=page_link, body=json.dumps(self.form_data), method='POST', callback=self.parse)
