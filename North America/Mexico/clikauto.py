import datetime
import json
import scrapy
import apify


class ClikautoSpider(scrapy.Spider):
    name = 'Clikauto'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    url = f'https://api.clikauto.com/api/inventory?perPage=24&firstPage=true'

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, headers={'Origin': 'https://clikauto.com'})

    def parse(self, response):
        json_data = response.json()
        results = json_data["inventory"]

        for item in results:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Clikauto'
            output['scraped_listing_id'] = item["id"]
            output["vehicle_url"] = f'https://clikauto.com/auto-seminuevo/{item["maker"]}-{item["model"].replace(" ", "-")}-{item["year"]}/{item["id"]}/vdpeasy'

            output['country'] = 'MX'
            output["make"] = item["maker"]
            output["model"] = item["model"]
            output["year"] = int(item["year"])

            if "body" in item and item["body"] != "":
                output["body_type"] = item["body"]
            if "city" in item and item["city"] != "":
                output["city"] = item["city"]
            if "exteriorColor" in item and item["exteriorColor"] != "":
                output["exterior_color"] = item["exteriorColor"]
            if "fuelType" in item and item["fuelType"] != "":
                output["fuel"] = item["fuelType"]
            if "interiorColor" in item and item["interiorColor"] != "":
                output["interior_color"] = item["interiorColor"]
            if "mileage" in item and item["mileage"] is not None:
                output["odometer_value"] = int(item["mileage"])
                output["odometer_unit"] = "km"
            if "passengers" in item and item["passengers"] is not None:
                output["seats"] = int(item["passengers"])
            if "state" in item and item["state"] != "":
                output["state_or_province"] = item["state"]
            if "transmission" in item and item["transmission"] != "":
                output["transmission"] = item["transmission"]
            if "trim" in item and item["trim"] != "":
                output["trim"] = item["trim"]
            if "used" in item:
                if item["used"]:
                    output["is_used"] = 1
                else:
                    output["is_used"] = 0
            if "vin" in item and item["vin"] != "":
                output["vin"] = item["vin"]
            if "cesviInteriorMaterial" in item and item["cesviInteriorMaterial"] != "":
                if item["cesviInteriorMaterial"] == "TELA":
                    output["upholstery"] = "leather"
            if "dealerName" in item and item["dealerName"] != "":
                output["dealer_name"] = item["dealerName"]
            if "price" in item and item["price"] != "" and item["price"].replace(".", "").isnumeric():
                output["price_retail"] = float(item["price"].replace(".", ""))
                output["currency"] = "MXN"
            if "images" in item and len(item["images"]) > 0:
                pictures_list = []
                for image in item["images"]:
                    pictures_list.append(f'{image["url"]}/{image["filename"]}')
                output["picture_list"] = json.dumps(pictures_list)
            apify.pushData(output)

        pagination = json_data["pagination"]
        next_page = pagination["nextPage"]
        if next_page is not None:
            yield scrapy.Request(url=f'https://api.clikauto.com/api/inventory?page={next_page}&perPage=24', callback=self.parse, headers={'Origin': 'https://clikauto.com'})



