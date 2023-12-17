import json
import scrapy
import datetime
import apify
from loguru import logger


def parse_body_type(bt_id):
    body_type = ""
    if bt_id == 7698:
        body_type = "coupe"
    elif bt_id == 7696:
        body_type = "hatchback"
    elif bt_id == 7704:
        body_type = "pickup"
    elif bt_id == 7694:
        body_type = "suv"
    elif bt_id == 7692:
        body_type = "sedan"
    elif bt_id == 7706:
        body_type = "truck"
    elif bt_id == 7702:
        body_type = "bus - minivan"
    elif bt_id == 7700:
        body_type = "convertible"
    return body_type


class OpensooqSpider(scrapy.Spider):
    name = 'opensooq'

    def start_requests(self):
        initial_url = 'https://sa.opensooq.com/en/find?PostSearch[categoryId]=1729&PostSearch[subCategoryId]=1731'
        body_type_ids = [7698, 7696, 7704, 7694, 7692, 7706, 7702, 7700]
        for bt_id in body_type_ids:
            url = initial_url + f'&PostSearch[dynamicAttributes][Cars_body_types][0]={bt_id}'
            yield scrapy.Request(url, callback=self.parse, meta={"bt_id": bt_id, "c_page": 1})

    def parse(self, response):
        c_page = response.meta["c_page"]
        link_list = response.xpath(
            '//a[@class="flex flexNoWrap p-16 blackColor radius-8 grayHoverBg ripple boxShadow2 relative"]/@href').getall()
        link_list = ["https://sa.opensooq.com" + i for i in link_list]
        yield from response.follow_all(link_list, self.product_detail,
                                       meta={"body_type": parse_body_type(response.meta["bt_id"])})

        next_button = response.xpath(f'//a[@aria-label="page {c_page + 1}"]/@href').get()
        if next_button:
            yield response.follow("https://sa.opensooq.com" + next_button, self.parse,
                                  meta={"bt_id": response.meta["bt_id"], "c_page": c_page + 1})

    def product_detail(self, response):
        output = dict()
        output["body_type"] = response.meta["body_type"]

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        options = []

        form_data = response.xpath('//div[@class="flex flexSpaceBetween flexWrap mt-8"]/div')
        for data in form_data:
            key = data.xpath('./p/text()').get().strip()
            value = data.xpath('./a/text()').get()
            value2 = data.xpath('./p[@class="width-75"]/text()').get()

            if value is not None:
                value = value.strip()

                if "Make" in key:
                    output["make"] = value
                elif "Model" in key:
                    output["model"] = value
                elif "Year" in key:
                    output["year"] = int(value)
                elif "Transmission" in key:
                    output["transmission"] = value
                elif "Fuel" in key:
                    output["fuel"] = value
                elif "Color" in key:
                    output["exterior_color"] = value
                elif "City" in key:
                    output["city"] = value
                elif "Condition" == key:
                    if value == "Used":
                        output["is_used"] = 1
                    if value == "New":
                        output["is_used"] = 0
                elif "Kilometers" in key:
                    output["odometer_value"] = int("".join([i for i in list(value.split("-")[-1]) if i.isdigit()]))
                    output["odometer_unit"] = "KM"
                elif "Options" in key:
                    value2 = value2.strip()
                    options.append(value2)

        if len(options) > 0:
            output["vehicle_options"] = json.dumps(options)

        car_options = response.xpath('//div[@class="flex flexSpaceBetween flexWrap mt-8"]/div')
        for data in car_options:
            key = data.xpath('./p/text()').get().strip()
            value = data.xpath('./p[@class="width-75"]/text()').get()
            if value is not None:
                for option in value.split(","):
                    option = option.strip()
                    if option == "Leather Seats":
                        output["upholstery"] = "leather"
                    if option == "Air Condition":
                        output["ac_installed"] = 1

        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Opensooq"
        output["scraped_listing_id"] = response.url.split("/")[5]
        output["vehicle_url"] = response.url

        output["country"] = "SA"
        price = response.xpath('//span[@class="sc-1ccec9e8-6 ctLAcG font-30 bold"]/text()').get()
        if price:
            output["price_retail"] = float(price.split(" ")[0].replace(",", ""))
            output["currency"] = price.split(" ")[-1]

        picture_list = response.xpath('///img[@class="image-gallery-image"]/@src').getall()
        if picture_list:
            output["picture_list"] = json.dumps([i.replace("75x75", "560x400") for i in picture_list])

        apify.pushData(output)
