import datetime
import json
import re
import scrapy
from loguru import logger
import apify


class CarDirectSpider(scrapy.Spider):
    name = 'CarsDirect'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def start_requests(self):
        urls = [
            'https://www.carsdirect.com/used_cars/listings-sem?zipcode=33136&distance=-1&qString=Yes%607%600%600%600%60false%7C&sortColumn=Default&sortDirection=ASC&searchGroupId=2241377944&initialSearch=false&recentSearchId=13689408&browserType=desktop',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.get_makes)

    def get_makes(self, response):
        makes_with_count = response.xpath(
            '//div[@class="filter-block"][1]/div[@class="filter-body"]/ul/li/text()').getall()
        for make_with_count in makes_with_count:
            make = make_with_count.split("(")[0].strip()
            count = make_with_count.split("(")[-1].strip().replace(")", "")
            link = f'https://www.carsdirect.com/used_cars/listings-sem/{make.lower().replace(" ", "-")}?zipcode=33136&distance=-1&qString=Yes%607%600%600%600%60false%7C&sortColumn=Default&sortDirection=ASC&searchGroupId=2241377944&initialSearch=false&recentSearchId=13689408&browserType=desktop'
            if int(count) < 900:
                yield scrapy.Request(response.url, callback=self.parse, meta={"make": make})
            else:
                yield scrapy.Request(link, callback=self.get_models, meta={"make": make, "page": 1})

    def get_models(self, response):
        make = response.meta["make"]
        models_with_count = response.xpath(
            '//div[@class="filter-block"][2]/div[@class="filter-body"]/ul/li/text()').getall()
        for model_with_count in models_with_count:
            model = model_with_count.split("(")[0].strip()
            link = f'https://www.carsdirect.com/used_cars/listings-sem/{make.lower().replace(" ", "-")}/{model.lower().replace(" ", "-")}?zipcode=33136&distance=-1&qString=Yes%607%600%600%600%60false%7C&sortColumn=Default&sortDirection=ASC&searchGroupId=2241377944&initialSearch=false&recentSearchId=13689408&browserType=desktop'
            yield scrapy.Request(link, callback=self.parse, meta={"make": make, "model": model, "page": 1})

    def parse(self, response):
        page = response.meta["page"]
        make = response.meta["make"]
        if "model" in response.meta:
            model = response.meta["model"]

        # Traverse product links
        product_links = response.xpath('//div[@class="list-details"]/h3/a/@href').getall()
        meta = {"make": make}
        if model:
            meta["model"] = model

        for link in product_links:
            yield scrapy.Request(url=f'https://www.carsdirect.com{link}', callback=self.detail, meta=meta)

        next_page = response.xpath('//div[@class="pager"]/ul/@numofpages').get()
        if next_page is not None and page <= int(next_page):
            page += 1
            meta["page"] = page
            link = response.url + f'&pageNum={page}'
            yield response.follow(link, self.parse, meta=meta)

    def detail(self, response):
        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'CarsDirect'
        output['scraped_listing_id'] = response.url.split("/")[-3].replace("ul", "")
        output["vehicle_url"] = response.url

        output['country'] = 'US'

        output["make"] = response.meta["make"]
        if "model" in response.meta:
            output["model"] = response.meta["model"]

        title = response.xpath('//div[@class="top-bar-title-set"]//h1/text()').get()
        year = title.split(" ")[0]
        if year.isnumeric():
            output["year"] = int(year)

        model = title.lower().replace(year, "").replace(output["make"], "")
        if "model" not in output:
            output["model"] = model

        price = response.xpath('//span[@class="price"]/text()').get()
        if price is not None:
            output["price_retail"] = float(price.replace(",", "").replace("$", ""))
            output["currency"] = "USD"

        miles = response.xpath('//span[@class="miles"]/text()').get()
        if miles is not None:
            output["odometer_value"] = int(miles.split(" ")[0].replace(",", ""))
            output["odometer_unit"] = miles.split(" ")[-1]

        pictures_list = response.xpath('//img[@itemprop="image"]/@src').getall()
        # if pictures_list:
        # output["picture_list"] = json.dumps(pictures_list)

        details_k = response.xpath('//div[@class="vehicle-details"]//dt/text()').getall()
        details_v = response.xpath('//div[@class="vehicle-details"]//dd/text()').getall()
        for k in range(len(details_k)):
            key = details_k[k].lower().strip()
            value = details_v[k].lower().strip()
            if value != 'n/a':
                if key == "trim":
                    output["trim"] = value
                elif key == "transmission":
                    output["transmission"] = value
                elif key == "doors":
                    if value.isnumeric():
                        output["doors"] = int(value)
                elif key == "vin":
                    output["vin"] = value
                elif key == "interior color":
                    output["interior_color"] = value
                elif key == "exterior color":
                    output["exterior_color"] = value

        apify.pushData(output)
