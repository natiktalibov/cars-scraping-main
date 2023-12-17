import json
import scrapy
import datetime
import apify

class Hatla2eeSpider(scrapy.Spider):
    name = 'hatla2ee'
    start_urls = ['https://ksa.hatla2ee.com/en/car']

    def parse(self, response):
        link_list = response.xpath("//div[@class='newCarListUnit_header']//a/@href").getall()
        link_list = [response.url.split("/en/")[0] + i for i in link_list]
        yield from response.follow_all(link_list, self.product_detail)

        # pagination
        li = response.xpath('//div[@class="pagination pagination-right"]//li').getall()
        next_button = response.xpath(f'//div[@class="pagination pagination-right"]//li[{len(li)}]/a/@href').get()
        if next_button:
            yield response.follow("https://ksa.hatla2ee.com" + next_button, self.parse)

    def product_detail(self, response):
        output = {}

        form_data_keys = response.xpath("//span[@class='DescDataSubTit']/text()").getall()
        form_data_values = response.xpath("//span[@class='DescDataVal']/text()").getall()
        for k in range(len(form_data_keys)):
            key = form_data_keys[k].strip()
            value = form_data_values[k].strip()
            if "Make" in key:
                output["make"] = value
            elif "Model" in key:
                output["model"] = value
            elif "Used since" in key:
                output["year"] = int(value)
            elif "Fuel" in key:
                output["fuel"] = value
            elif "Transmission" in key:
                output["transmission"] = value
            elif "Color" in key:
                output["exterior_color"] = value
            elif "Body Style" in key:
                output["body_type"] = value
            elif "Km" in key:
                output["odometer_value"] = int(value.split(" ")[0].replace(",", ""))
                output["odometer_unit"] = value.split(" ")[1]
            elif "City" in key:
                output["city"] = value

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Hatla2ee"
        output["scraped_listing_id"] = response.url.split("/").pop()
        output["vehicle_url"] = response.url
        output["country"] = "SA"
        price_data = response.xpath("//span[@class='usedUnitCarPrice']/text()").get()
        price = price_data.split(" ")[0].replace(",", "").strip()
        if price.isdigit():
            output["price_retail"] = float(price)
            if price_data.split(" ")[1] == 'Riyal':
                output["currency"] = "SAR"

        picture_list = response.xpath("//img[@class='swiper-lazy']/@src | //img[@class='swiper-lazy']/@data-src").getall()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        equipment = response.xpath("//div[@class='equipDataWrap']//li/text()").getall()
        for k in equipment:
            k = k.strip()
            if k == "Air Conditioning":
                output["ac_installed"] = 1
            if k == "Leather Seats":
                output["upholstery"] = "leather"

        apify.pushData(output)