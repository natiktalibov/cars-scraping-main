import re
import json
import scrapy
import datetime
import apify

class OlxSpider(scrapy.Spider):
    name = 'olx'
    start_urls = ['https://www.olx.sa.com/en/vehicles/cars/']

    def parse(self, response):
        link_list = response.xpath('//li[@aria-label="Listing"]/article/div[1]/a/@href').getall()
        link_list = ["https://www.olx.sa.com" + i for i in link_list]
        yield from response.follow_all(link_list, self.product_detail, dont_filter=True)

        # pagination
        next_page = response.xpath('//div[@role="navigation"]//li[last()]/a/@href').get()
        if next_page:
            page_link = f'https://www.olx.sa.com{next_page}'
            yield response.follow(url=page_link, callback=self.parse)

    def product_detail(self, response):
        output = {}

        form_data = response.xpath('//div[@aria-label="Details and description"]//div[@class="_676a547f"]')
        for data in form_data:
            key = data.xpath('./div/span[1]/text()').get()
            value = data.xpath('./div/span[2]/text()').get()
            if "Brand" in key:
                output["make"] = value
            elif "Model" in key:
                output["model"] = value
            elif "Year" in key:
                output["year"] = int(value)
            elif "Transmission " in key:
                output["transmission"] = value
            elif "Kilometers" in key:
                odometer_value = value.split(" ")[-1].replace(",", "")
                if odometer_value:
                    output["odometer_value"] = int(odometer_value)
                    output["odometer_unit"] = "km"

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "olx"
        output["scraped_listing_id"] = re.findall("ID(.*?).html", response.url, re.S)[0]
        output["vehicle_url"] = response.url
        output["country"] = "SA"
        city = response.xpath('//span[@aria-label="Location"]/text()').get()
        output["city"] = city.split(",")[0].strip()
        price = response.xpath('//span[@class="_56dab877"]/text()').get()
        if price:
            output["price_retail"] = float(price.split(" ")[1].replace(",", ""))
            output["currency"] = "SAR"

        picture_list = response.xpath('//div[@class="image-gallery-slides"]/div/picture/img/@src').getall()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        apify.pushData(output)