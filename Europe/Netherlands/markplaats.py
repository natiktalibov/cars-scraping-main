import re
import json
import apify
import scrapy
import datetime
from loguru import logger


class MarktplaatsSpider(scrapy.Spider):
    name = 'Marktplaats'
    start_urls = [
        'https://www.marktplaats.nl/lrp/api/search?attributesById[]=10882&l1CategoryId=91&limit=30&offset=0&searchInTitleAndDescription=true&viewOptions=list-view']
    offset = 0

    def parse(self, response):
        result = response.json()
        if "listings" in result:
            listings = result["listings"]
            for listing in listings:
                yield scrapy.Request(url=f'https://www.marktplaats.nl{listing["vipUrl"]}', callback=self.product_detail)

            totalCount = result['totalResultCount']
            if self.offset < totalCount:
                self.offset += 30
                url = f'https://www.marktplaats.nl/lrp/api/search?attributesById[]=10882&l1CategoryId=91&limit=30&offset={self.offset}&searchInTitleAndDescription=true&viewOptions=list-view'
                yield scrapy.Request(url=url, callback=self.parse)

    def product_detail(self, response):
        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Marktplaats'
        output["vehicle_url"] = response.url

        output['country'] = 'NL'

        breadcrumbs = response.xpath("//nav[@class='Breadcrumbs-root']/a/text()").getall()
        output["make"] = breadcrumbs[-2]
        output["model"] = breadcrumbs[-1]

        id = response.xpath("//nav[@class='Breadcrumbs-root']/span/text()").get()
        output['scraped_listing_id'] = id.split(" ")[-1]

        price = response.xpath("//div[@class='Listing-price']//text()").get()
        if price is not None:
            price = price.replace(",", "").replace("-", "").replace(".", "").strip()

            if price.split("\xa0")[1].isnumeric():
                output["price_retail"] = float(price.split("\xa0")[1])
                output['currency'] = "EUR"

        description = response.xpath("//div[@class='Description-description']//text()").get()
        output["vehicle_disclosure"] = description

        details_keys = response.xpath(
            "//div[@class='CarAttributes-list ']/div/div[2]/div[@class='CarAttributes-label']/text()").getall()
        details_values = response.xpath(
            "//div[@class='CarAttributes-list ']/div/div[2]/div[@class='CarAttributes-value']/text()").getall()

        for k in range(len(details_keys)):
            key = details_keys[k]
            value = details_values[k]
            if "Motorinhoud" == key:
                output["engine_displacement_value"] = value.split(" ")[0]
                output["engine_displacement_units"] = "L"
            elif key == "Bouwjaar":
                output["year"] = int(value)
            elif key == "Brandstof":
                output["fuel"] = value
            elif key == "Transmissie":
                output["transmission"] = value
            elif key == "KM stand":
                value = value.strip().split(" ")
                output["odometer_value"] = int(value[0].replace(".", ""))
                output["odometer_unit"] = value[1]

        specs_keys = response.xpath(
            "//div[@class='hz-AccordionItem-accordionBody CarAttributesAccordion-body']/div/div[@class='CarAttributesAccordion-bodyItemLabel']/text()").getall()
        specs_values = response.xpath(
            "//div[@class='hz-AccordionItem-accordionBody CarAttributesAccordion-body']/div/div[@class='CarAttributesAccordion-bodyItemValue']/text()").getall()
        for k in range(len(specs_keys)):
            key = specs_keys[k]
            value = specs_values[k]
            if key == "Carrosserie":
                output["body_type"] = value
            elif key == "Aantal deuren":
                output["doors"] = int(value)
            elif key == "Aantal stoelen":
                output["seats"] = int(value)
            elif key == "Kleur":
                output["exterior_color"] = value
            elif key == "Aantal cilinders":
                output["engine_cylinders"] = int(value)

        apify.pushData(output)
