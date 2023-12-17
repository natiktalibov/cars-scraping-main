import json
import scrapy
import datetime

import apify


class TrovitSpider(scrapy.Spider):
    name = 'trovit'
    start_urls = ['https://autos.trovit.nl/']

    global false, null, true
    false = null = true = ''

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.get_brand)

    def get_brand(self, response):  # The website uses brand classification, so you need to get all the brands
        brand_list = response.xpath("//div[@class='lh-make']//li/a")
        for brand in brand_list:
            yield scrapy.Request(url=brand.xpath("./@href").get(), callback=self.parse)

    def parse(self, response):
        link_list = response.xpath("//ul[@id='wrapper_listing']/li/div/a/@href").getall()
        for link in link_list:
            yield response.follow(url=link, callback=self.product_detail)

        next_button = response.xpath("//a[@data-test='p-next']/@href").get()
        if next_button:
            yield response.follow(next_button, self.parse)

    def product_detail(self, response):
        output = {}

        if "heycar.nl/vehicle" in response.url:
            jsn = response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
            jsn_data = eval(jsn)["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]
            data_list = jsn_data.get("aggregations")

            output["make"] = data_list.get("make")[0]["displayName"]
            output["model"] = data_list.get("model")[0]["displayName"]
            year = data_list.get("year")[0]["displayName"]
            if year:
                output["year"] = int(year)
            output["transmission"] = response.xpath("//dt[text()='Transmissie']/following-sibling::dd/text()").get()
            output["fuel"] = data_list.get("fuelType")[0]["displayName"]

            output["scraped_listing_id"] = jsn_data.get("id")
            odometer_value = jsn_data["content"][0].get("mileage")
            if odometer_value:
                output["odometer_value"] = int(odometer_value)
            output["odometer_unit"] = "KM"
            pictures = jsn_data["content"][0].get("images")
            picture_list = ["https://img.heycar.nl/unsafe/1440x/filters:quality(90):no_upscale()/" + i["url"] for i in
                            pictures]
            if picture_list:
                output["picture_list"] = json.dumps(picture_list)
            output["city"] = jsn_data["content"][0]["dealer"]["location"].get("city")
            output["country"] = "NL"
            price = jsn_data["content"][0].get("price")
            if price:
                output["price_retail"] = float(price)
                output["currency"] = "EUR"

            output["ac_installed"] = 0
            output["tpms_installed"] = 0
            output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
            output["scraped_from"] = "trovit"
            output["vehicle_url"] = response.url

            apify.pushData(output)
        elif "autos.trovit.nl" in response.url:
            robot_verification = response.xpath("//h1[text()='We are sorry']/text()").get()
            if robot_verification:  # The robot verification page appears
                return None
            data_key = response.xpath("//div[@id='amenities']//dt/text()").getall()
            data_value = response.xpath("//div[@id='amenities']//dd/text()").getall()
            for i in range(len(data_key)):
                if "Merk" in data_key[i]:
                    output["make"] = data_value[i]
                elif "Model" in data_key[i]:
                    output["model"] = data_value[i]
                elif "Jaar" in data_key[i]:
                    year = data_value[i]
                    if year:
                        output["year"] = int(year)
                elif "Transmissie" in data_key[i]:
                    output["transmission"] = data_value[i]
                elif data_key[i] == "Brandstof":
                    output["fuel"] = data_value[i]
                elif "Kilometers" in data_key[i]:
                    odometer_value = data_value[i].replace(".", "")
                    if odometer_value:
                        output["odometer_value"] = int(odometer_value)
                        output["odometer_unit"] = "KM"

            output["ac_installed"] = 0
            output["tpms_installed"] = 0
            output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
            output["scraped_from"] = "trovit"
            output["scraped_listing_id"] = response.url.split("/").pop()
            output["vehicle_url"] = response.url
            output["country"] = "Netherlands"
            price = response.xpath("//span[@class='amount']/text()").get()
            if price:
                output["price_retail"] = float("".join([i for i in list(price.replace(".", "")) if i.isdigit()]))
                output["price_wholesale"] = output["price_retail"]
                output["currency"] = "EUR"

            picture_list = response.xpath("//ul[@class='images']/li/img/@src").getall()
            picture_list = ["https:" + i for i in picture_list]
            if picture_list:
                output["picture_list"] = json.dumps(picture_list)

            apify.pushData(output)


