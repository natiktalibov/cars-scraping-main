import scrapy
import datetime
import json

import apify


class UsedcarstzSpider(scrapy.Spider):
    name = 'usedcarstz'
    download_timeout = 120
    start_urls = ['https://www.usedcarstz.com/search.html?&action=search&page=1']

    def parse(self, response):

        link_list = response.xpath('//div[@class="itemsContainer"]/a/@href').getall()
        link_list = ["https://www.usedcarstz.com" + i for i in link_list if len(i) > 10]
        yield from response.follow_all(link_list, self.product_detail)

        next_page_button = response.xpath("//a[contains(text(), 'Next page >>>')]/@href").get()
        if next_page_button:
            next_link = "https://www.usedcarstz.com" + next_page_button
            yield response.follow(next_link, self.parse)

    def product_detail(self, response):
        output = {}

        output["make"] = response.xpath('//div[@id="images_block_start"]/span[@itemprop="manufacturer"]/text()').get()
        output["model"] = response.xpath('//div[@id="images_block_start"]/span[@itemprop="model"]/text()').get()

        form_data = response.xpath(
            '//div[@id="specs_block_start"]/div/text() | //div[@id="specs_block_start"]/div/a/text()').getall()
        form_data = [i.strip() for i in form_data if i.strip() != '']
        del form_data[-1]  # Delete product details
        for data in form_data:
            if 'Year' in data:
                output["year"] = int(data.replace("Year", "").strip())
            elif 'Transmission' in data:
                output["transmission"] = data.replace("Transmission", "").strip()
            elif 'Engine' in data:
                engine = data.replace("Engine", "").strip()
                output["engine_displacement_value"] = "".join([i for i in engine if i.isdigit() or i == "."])
                output["engine_displacement_units"] = "".join([i for i in engine if i.isalpha()])
            elif 'Fuel' in data:
                output["fuel"] = data.replace("Fuel", "").strip()
            elif 'mileage' in data:
                mileage = data.replace("mileage", "").strip()
                output["odometer_value"] = int("".join([i for i in mileage if i.isdigit()]))
                output["odometer_unit"] = "".join([i for i in mileage if i.isalpha()])

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = 'Used Cars TZ'
        output["scraped_listing_id"] = response.url.split("-")[-1]
        output["vehicle_url"] = response.url
        output["country"] = "TZ"

        output["city"] = response.xpath('//div[@itemprop="address"]/a/text()').get()
        price_data = response.xpath('//span[@id="price_chng"]/text()').get()
        price = "".join([i for i in list(price_data) if i.isdigit() or i == '.'])
        if price and "TShs" in price_data and "m" in price_data:
            output["price_retail"] = float(price) * 1000000
            output["currency"] = "TZS"

        picture_list = response.xpath('//a[@class="gallery-item"]/@href').getall()
        picture_list = ["https://www.usedcarstz.com" + i for i in picture_list]
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        # yield output
        apify.pushData(output)