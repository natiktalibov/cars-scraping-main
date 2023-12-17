import re
import json
import apify
import scrapy
import requests
import datetime


class AnnbodpaginaSpider(scrapy.Spider):
    name = 'annbodpagina'
    start_urls = ['https://auto.aanbodpagina.nl/']

    def parse(self, response):
        brand_link = response.xpath("//ul[@class='list list__nav']/li/a/@href").getall()

        yield from response.follow_all(urls=brand_link, callback=self.list_car)

    def list_car(self, response):
        link_list = response.xpath("//form[@id='form1']/article/a/@href").getall()
        link_list = ["https://auto.aanbodpagina.nl" + i for i in link_list]

        yield from response.follow_all(urls=link_list, callback=self.detail_car)

        pagination = response.xpath("//div[@class='pagination']//tr[2]//li/a/text()").getall()
        current_page = response.xpath("//div[@class='pagination']//tr[2]//li[@class='active']/a/text()").get()
        if current_page:
            last_page = max([int(i) for i in pagination])
            if int(current_page) == 1 and int(current_page) + 1 < int(last_page) + 1:
                next_link = response.url + "/2"
                yield response.follow(url=next_link, callback=self.list_car)
            elif int(current_page) > 1 and int(current_page) + 1 < int(last_page) + 1:
                # Invert the detail page url and replace the first page number
                next_link = response.url[::-1].replace("10"[::-1], "", 1)[::-1] + f"{int(current_page) + 1}"
                yield response.follow(url=next_link, callback=self.list_car)

    def detail_car(self, response):
        output = {}

        output["make"] = response.xpath("//div[@class='Breadcrumbs']/ol/li[3]/a/text()").get()
        form_data = response.xpath("//table[@class='AdTypesTable']//tr")
        for data in form_data:
            key = data.xpath("./td[@class='AdType']/text()").get()
            value = data.xpath("./td[@class='AdTypeValue']/text()").get()
            if not key:
                continue
            if "Model" in key:
                output["model"] = value.replace(":", "").strip()
            elif "Bouwjaar" in key:
                year = value.replace(":", "").strip()
                if year and year.isdigit():
                    output["year"] = int(year)
            elif "Brandstof" in key:
                output["fuel"] = value.replace(":", "").strip()
            elif "Aantal deuren" in key:
                output['doors'] = int(value.split(' ')[1])
            elif "Carrosserie" in key:
                output['body_type'] = value.split(' ')[1]
            elif "Kleur" in key:
                output['exterior_color'] = value.split(' ')[1]
            elif "Kilometerstand" in key:
                mileage = "".join([i for i in list(value) if i.isdigit()])
                if mileage:
                    output["odometer_value"] = int(mileage)
                    output["odometer_unit"] = "km"
            elif "Locatie" in key:
                output["city"] = data.xpath("./td[@class='AdTypeValue']/a/text()").get()

        description = response.xpath("//div[@id='vip-ad-description']/text()").getall()
        for desc in description:
            if "Transmissie" in desc and "Automaat" in desc:
                output["transmission"] = "Automaat"
            elif "Transmissie" in desc and "Handgeschakeld" in desc:
                output["transmission"] = "Handgeschakeld"

        price = response.xpath("//section[@id='MainContent_ContentPlaceHolder_Price1']/text()").get()
        price = "".join([i for i in list(price) if i.isdigit()])
        if price:
            output["price_retail"] = float(price)
            output["currency"] = "EUR"

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "aanbodpagina"
        output["scraped_listing_id"] = response.url.split("/").pop()
        output["vehicle_url"] = response.url
        output["country"] = "NL"

        picture_list = response.xpath("//ul[@class='bxslider']/li/img/@src").getall()
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

        apify.pushData(output)
        # yield output

