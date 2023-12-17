import json
import os
import re
import scrapy
import datetime

import apify


class AutozarSpider(scrapy.Spider):
    name = 'autozar'
    start_urls = ['http://autozar.mn/zar/1/p/1', "http://autozar.mn/zar/2/p/1", "http://autozar.mn/zar/3/p/1"]

    def parse(self, response):
        detail_link = response.xpath(
            '//div[@class="inventory margin-bottom-20 clearfix"]/a[@class="inventory"]/@href').getall()
        detail_link = ["http://autozar.mn" + i for i in detail_link]

        yield from response.follow_all(detail_link, self.product_detail)

        current_page = response.url.split("/")[-1]
        last_page = response.xpath("//form[@id='auto-paginate-form']/ul//strong/text()").get().split("/")[-1]

        car_module = re.findall("/zar/(.*?)/p/", response.url, re.S)[0]
        next_page = f'http://autozar.mn/zar/{car_module}/p/' + str(int(current_page) + 1)

        if int(current_page) + 1 < int(last_page) + 1:
            yield response.follow(next_page, self.parse)

    def product_detail(self, response):
        output = {}

        form_data = response.xpath("//table[@class='table']//tr")
        for data in form_data:
            key = data.xpath("./td[1]/text()").get()
            value = data.xpath("./td[2]/text()").get()
            if "Үйлдвэрлэгч:" in key:
                output["make"] = value
            elif "Загвар:" in key:
                output["model"] = value
            elif "Үйлдвэрлэсэн он:" in key:
                output["year"] = int(value)
            elif "Орж ирсэн он:" in key:  ## by NT
                output["registration_year"] = int(value)  ## by NT
            elif "Хөтлөгч:" in key:  ## by NT
                if value == "Бүх дугуй":  ## by NT
                    output["drive_train"] = '4x4'  ## by NT
            elif "Жолооны хүрд:" in key: ## by NT
                output["steering_position"] = value ## by NT
            elif "Гадна өнгө:" in key: ## by NT
                output["exterior_color"] = value ## by NT
            elif "Салоны өнгө:" in key: ## by NT
                output["interior_color"] = value ## by NT
            elif "Төлөв:" in key: ## by NT
                if value == "Явж байсан":  ## by NT
                    output["is_used"] ="Yes"  ## by NT
            elif "Хроп:" in key:
                output["transmission"] = value
            elif "Хөдөлгүүр:" in key:
                output["engine_displacement_value"] = value.split(" ")[0]
                output["engine_displacement_units"] = value.split(" ")[1]
            elif "Шатахуун:" in key:
                output["fuel"] = value
            elif "Явсан км:" in key:
                output["odometer_value"] = int(value.replace(",", ""))
                output["odometer_unit"] = key.split(" ")[-1].replace(":", "")

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = 'Autozar'
        output["scraped_listing_id"] = response.url.split("/")[-1]
        output["vehicle_url"] = response.url
        output["country"] = "MN"

        price = response.xpath("//div[@class='col-lg-2 col-md-2 col-sm-2 text-right']/h2/text()").get()
        if "сая" in price:
            output["price_retail"] = float("".join([i for i in list(price) if i.isdigit() or i == "."])) * 1000000
        else:
            output["price_retail"] = float("".join([i for i in list(price) if i.isdigit() or i == "."]))
        if "₮" in price:
            output["currency"] = "MNT"

        picture_list = response.xpath("//div[@id='home-slider-canvas']//ul[@class='slides']/li/img/@src").getall()
        if picture_list:
            output["picture_list"] = json.dumps(["http://autozar.mn" + i for i in picture_list])

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
