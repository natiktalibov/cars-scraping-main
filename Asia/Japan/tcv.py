import datetime
import json
import scrapy
from scrapy import Selector
import re
import apify

class TCVSpider(scrapy.Spider):
    name = 'TCV'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def start_requests(self):
        urls = [
            'https://www.tc-v.com/used_car/all/all/',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': 0})

    def parse(self, response, page):
        # Traverse product links
        product_links = response.xpath('//div[@class="car-item__pic-area"]/a/@href').getall()

        for link in product_links:
            yield scrapy.Request(url=f'https://www.tc-v.com{link}', callback=self.product_detail,
                                )
        # pagination
        next_page = response.xpath(f'//span[@class="next"]/a/@href').get()
        if next_page is not None:
            page += 1
            page_link = f'https://www.tc-v.com/used_car/all/all/?pn={page}'
            yield response.follow(url=page_link, callback=self.parse, cb_kwargs={'page': page})

    def product_detail(self, response):
        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'TCV'
        output['scraped_listing_id'] = response.url.split('/')[-2]
        output["vehicle_url"] = response.url

        output['country'] = 'JP'
        breadcrumbs = response.xpath("//li[@class='breadcrumbs__area-item']/a/text()").getall()
        output["make"] = breadcrumbs[1]
        output["model"] = breadcrumbs[2].replace(output["make"],  "").strip()
        output["year"] = int(breadcrumbs[3].split(" ")[-1])

        images = response.xpath("//li[@class='image__gallery-thumb-item used__thumb-item slide__gallery-thumbItem--js']/img/@src").getall()
        if images is not None and len(images) > 0:
            output['picture_list'] = json.dumps(images)

        price = response.xpath("//div[@class='car__price-body']/text()").get()
        if price is not None:
            price = price.replace("$", "").replace("US", "").replace(",", "").strip()
            if price.isnumeric():
                output["price_retail"] = price
                output['currency'] = "USD"

        details_table = response.xpath("//table[@class='car__info-table']//tr").getall()
        for k in range(len(details_table)):
            key = Selector(text=details_table[k]).xpath("//th//text()").get()
            value = Selector(text=details_table[k]).xpath("//td//text()").get()
            if value != "-":
                if "VIN" == key:
                    output["vin"] = value.strip()
                elif "registration year" in key:
                    year = value.split("/")[0]
                    if year.isnumeric():
                        output["registration_year"] = int(year)
                elif "Mileage" in key:
                    output['odometer_value'] = int(value.split(" ")[0].replace(",", ""))
                    output["odometer_unit"] = "km"
                elif "Transmission" in key:
                    output["transmission"] = value.strip()
                elif "Engine Capacity" in key:
                    output["engine_displacement_value"] = re.findall(r'\d+', value.replace(",", ""))[0]
                    output["engine_displacement_units"] = value.replace(",", "").replace(output["engine_displacement_value"], "")
                elif "Fuel" in key:
                    output["fuel"] = value
                elif "BodyStyle1" in key:
                    output["body_type"] = value
                elif "Steering" in key:
                    output["steering_position"] = value
                elif "Exterior Color" in key:
                    output["exterior_color"] = value
                elif "Interior Color" in key:
                    output["interior_color"] = value
                elif "Drive Type" in key:
                    output["drive_train"] = value
                elif "Door" in key:
                    if value.isnumeric():
                        output["doors"] = int(value)
                elif "Number of Seats" in key:
                    if value.isnumeric():
                        output["seats"] = int(value)
                elif "Condition" in key:
                    if "Used" in value:
                        output["is_used"] = 1
                    else:
                        output["is_used"] = 0
                elif "Comment" in key:
                    output["vehicle_disclosure"] = value

        info_area = response.xpath("//section[@class='car_info-area']//dd").getall()
        options = []
        for k in range(len(info_area)):
            dd_class = Selector(text=info_area[k]).xpath("//@class").get()
            dd_text = Selector(text=info_area[k]).xpath("//text()").get()
            if "active" in dd_class:
                options.append(dd_text)

        if len(options) > 0:
            output['vehicle_options'] = json.dumps(options)

        apify.pushData(output)




