import datetime
import json
import scrapy
import apify

class MotorySpider(scrapy.Spider):
    name = 'Motory.com'
    download_timeout = 120
    start_urls = ['https://ksa.motory.com/en/cars-for-sale/?page=1']

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//div[@class="d-flex flex-column  w-100 h-100 flex-md-row"]//a/@href').getall()
        for i in product_links:
            yield scrapy.Request(url=i, callback=self.detail)

        # pagination
        page_link = response.xpath('//div[@class="item next"]/a/@href').get()
        if page_link is not None:
            yield response.follow(page_link, self.parse)

    def detail(self, response):
        output = dict()
        output["ac_installed"] = 0
        output["tpms_installed"] = 0

        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Motory.com"
        output["scraped_listing_id"] = response.url.split("/")[-2]
        output["vehicle_url"] = response.url

        # location details
        output["country"] = "SA"

        specifications_keys = response.xpath("//div[@class='vehicles-detail-overview-item']/span[1]/text()").getall()
        specifications_values = response.xpath("//div[@class='vehicles-detail-overview-item']/span[2]/text()").getall()

        for k in range(len(specifications_keys)):
            key = specifications_keys[k]
            value = specifications_values[k].strip()
            if key == "Vehicle Condition":
                if value.strip() == "New":
                    output["is_used"] = 0
                if value.strip() == "Used":
                    output["is_used"] = 1
            if key == "Body Type":
                output["body_type"] = value.strip()
            if key == "Make":
                output["make"] = value
            if key == "Model":
                output["model"] = value
            if key == "City":
                output["city"] = value
            if key == "Year":
                output["year"] = int(value)
            if key == "Transmission":
                output["transmission"] = value.strip()
            if key == "Color":
                output["exterior_color"] = value.strip()
            if key == "Mileage":
                output["odometer_value"] = int(value.strip().split("-")[-1].replace(",", "")) \
                    if "-" in value else int(value.strip().replace(",", "").replace("+", ""))
                output["odometer_unit"] = "km"

        price_details = response.xpath(
            '//div[@class="price-container-value  "]/text()').get()
        if price_details is not None:
            if price_details.strip().replace(",", "").isnumeric():
                output["price_retail"] = float(price_details.strip().replace(",", ""))
                output["currency"] = "SR"

        picture_list = response.xpath('//a[@data-fancybox="gallery"]//img/@src').get()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        description_array = response.xpath('//div[@class="vehicles-detail-description-content intro"]//text()').getall()
        description = ""
        for line in description_array:
            line = line.strip()
            if line not in ["Read less", "Read More"] and len(line)> 0:
                if "VIN Number" in line:
                    output["vin"] = line.split(":")[-1].strip()
                description = description + " " + line
        output["vehicle_disclosure"] = description

        apify.pushData(output)
