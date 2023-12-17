import json
import scrapy
import datetime

import apify


class ExpatriatesSpider(scrapy.Spider):
    name = 'expatriates'
    start_urls = ['https://www.expatriates.com/classifieds/jordan/vehicles-cars-trucks/']

    def parse(self, response):
        link_list = response.xpath('//li[@class="has-picture"]/a[1]/@href').getall()
        link_list = ["https://www.expatriates.com" + i for i in link_list]

        yield from response.follow_all(link_list, self.product_detail)

    def product_detail(self, response):
        output = {}

        # Part of the information is parsed in the title
        title = response.xpath('//div[@class="page-title"]/h1/text()').get().strip()
        output["make"] = title.split(",")[1].strip()
        if len(output["make"].split(" ")) == 2:
            output["model"] = output["make"].split(" ")[1]
            output["make"] = output["make"].split(" ")[0]
        output["year"] = int(title.split(",")[2].strip())
        output["transmission"] = title.split(",")[3].strip()
        description_list = response.xpath('//div[@class="post-body"]/text()').getall()
        for desc in description_list:
            if "Engine:" in desc:
                output["engine_displacement_value"] = desc.replace("Engine:", "").split(" ")[0]
                output["engine_displacement_units"] = desc.replace("Engine:", "").split(" ")[1]
            elif "cc" in desc:
                engine_displacement_value = [i for i in desc.split(" ") if i.isdigit()]
                if engine_displacement_value:
                    output["engine_displacement_value"] = engine_displacement_value[0]
                    output["engine_displacement_units"] = "cc"

        if "KM" in title.split(",")[4].strip():
            output["odometer_value"] = int(title.split(",")[4].strip().split(" ")[0])
            output["odometer_unit"] = title.split(",")[4].strip().split(" ")[1]
        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "expatriates"
        output["scraped_listing_id"] = response.url.split("/")[-1].replace(".html", "")
        output["vehicle_url"] = response.url
        output["city"] = response.xpath("//strong[text()='Region']/parent::li/text()").get().split("(")[0].strip()
        output["country"] = "JO"
        price = title.split(",")[0].strip().split(" ")[1]
        if price:
            output["price_retail"] = float(price)
            output["currency"] = title.split(",")[0].strip().split(" ")[0]

        picture_list = response.xpath("//div[@class='posting-images top-margin']/img/@src").getall()
        picture_list = ["https://www.expatriates.com" + i for i in picture_list]
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