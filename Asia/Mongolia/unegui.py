import re
import json
import scrapy
import datetime

import apify


class UneguiSpider(scrapy.Spider):
    name = 'unegui'
    download_timeout = 120
    start_urls = ['https://www.unegui.mn/avto-mashin/?page=1']

    def parse(self, response):
        # The product information on the web page is divided into three parts
        vip_link = response.xpath('//div[@class="list-title__top-container "]/div//a[@class="mask"]/@href').getall()
        special_link = response.xpath('//div[@class="list-title__top-container "]/li/a/@href').getall()
        simple_link = response.xpath(
            "//ul[@class='list-simple__output js-list-simple__output ']/li/a/@href").getall()

        yield from response.follow_all(
            ["https://www.unegui.mn" + i for i in vip_link+special_link+simple_link], self.product_detail)

        # Page links on the first page do not display page numbers
        if "page=" not in response.url:
            current_page = 1
        else:
            current_page = response.url.split("page=")[1]

        number_list = response.xpath('//ul[@class="number-list"]/li/a/@data-page').getall()
        last_page = max([int(i) for i in number_list])
        if int(current_page)+1 < last_page + 1:
            next_link = "https://www.unegui.mn/avto-mashin/?page=" + str(int(current_page)+1)
            yield response.follow(next_link, self.parse)

    def product_detail(self, response):
        output = {}
        car_type = response.xpath('//ul[@class="breadcrumbs"]/li[3]/a/span/text()').get()
        if car_type == "Автомашин зарна":  # "Автомашин зарна" is the classification of the vehicle
            output["make"] = response.xpath('//ul[@class="breadcrumbs"]/li[4]/a/span/text()').get()
            model = response.xpath('//ul[@class="breadcrumbs"]/li[5]/a/span/text()').get()
            if model:
                output["model"] = model.replace(",", "")

            form_data = response.xpath("//ul[@class='chars-column']/li")
            for data in form_data:
                key = data.xpath('./span[@class="key-chars"]/text()').get()
                value = data.xpath('./span[@class="value-chars"]/text() | ./a[@class="value-chars"]/text()').get()
                if "Үйлдвэрлэсэн он" in key:
                    output["year"] = int(value)
                elif "Хурдны хайрцаг" in key:
                    output["transmission"] = value
                elif "Мотор багтаамж" in key:
                    output["engine_displacement_value"] = value.split(" ")[0]
                    output["engine_displacement_units"] = value.split(" ")[1].strip()
                elif "Хөдөлгүүр" in key:
                    output["fuel"] = value.strip()
                elif "Явсан" in key:
                    output["odometer_value"] = int(value.split(" ")[0])
                    output["odometer_unit"] = value.split(" ")[1].replace(".", "")
                elif "Төрөл" in key:  ## by NT
                    output["body_type"] = value.strip() ## by NT
                elif "Нөхцөл" in key: ## by NT
                    if "Дугаар аваагүй" in value.strip(): ## by NT
                        output["is_used"] = 'Yes' ## by NT
                elif "Өнгө" in key: ## by NT
                    output["exterior_color"] = value.strip() ## by NT
                elif "Орж ирсэн он" in key:  ## by NT
                    output["registration_year"] = value.strip() ## by NT
                elif "Дотор өнгө" in key: ## by NT
                    output["interior_color"] = value.strip() ## by NT
                elif "Хөтлөгч" in key: ## by NT
                    if "Бүх дугуй 4WD" in value.strip(): ## by NT
                        output["drive_train"] = '4x4' ## by NT
                elif "Хаалга" in key: ## by NT
                    output["doors"] = int(value.strip()) ## by NT

            output["ac_installed"] = 0
            output["tpms_installed"] = 0
            output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
            output["scraped_from"] = 'unegui'
            output["scraped_listing_id"] = re.findall("adv/(.*?)_", response.url, re.S)[0]
            output["vehicle_url"] = response.url
            output["country"] = "MN"
            output["city"] = response.xpath('//span[@itemprop="address"]/text()').get()

            price = response.xpath('//div[@class="announcement-price "]//meta[@itemprop="price"]/@content').get()
            output["price_retail"] = float(price)
            output["currency"] = response.xpath(
                '//div[@class="announcement-price "]//meta[@itemprop="priceCurrency"]/@content').get()

            picture_list = response.xpath('//div[@class="announcement__images"]/img/@src').getall()
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
