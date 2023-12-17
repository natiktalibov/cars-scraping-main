import datetime
import json
import scrapy
import apify


class BuyACarSpider(scrapy.Spider):
    name = 'BuyACar'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    url = f'https://www.buyacar.co.uk/_search/v1?f[0]=vehicle_type:car&offset=0&limit=100'

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, cb_kwargs={'offset': 0})

    def parse(self, response, offset):
        json_data = response.json()
        results = json_data["results"]

        for item in results:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'BuyACar'
            output['scraped_listing_id'] = item["prodAdvertId"]
            output["vehicle_url"] = f'https://www.buyacar.co.uk{item["variant_url"]}/deal-{item["prodAdvertId"]}'

            output['country'] = 'GB'
            if "bodyStyle" in item and item["bodyStyle"] is not None and item["bodyStyle"] != "":
                output["body_type"] = item["bodyStyle"]
            if "gearbox" in item and item["gearbox"] is not None and item["gearbox"] != "":
                output["transmission"] = item["gearbox"]
            if "genericColour_S" in item and item["genericColour_S"] is not None and item["genericColour_S"] != "":
                output["exterior_color"] = item["genericColour_S"]
            if "CTrim_Name" in item and item["CTrim_Name"] is not None and item["CTrim_Name"] != "":
                output["trim"] = item["CTrim_Name"]
            if "make" in item and item["make"] is not None and item["make"] != "":
                output["make"] = item["make"]
            if "term_model_short_name" in item and item["term_model_short_name"] is not None and item["term_model_short_name"] != "":
                output["model"] = item["term_model_short_name"]
            if "mileage" in item and item["mileage"] is not None and item["mileage"] != "":
                output['odometer_value'] = int(item["mileage"])
                output["odometer_unit"] = "km"
            if "yearOfRegistration" in item and item["yearOfRegistration"] is not None and item["yearOfRegistration"] != "":
                output["registration_year"] = int(item["yearOfRegistration"])
            if "dennis_fuel_type" in item and item["dennis_fuel_type"] is not None and item["dennis_fuel_type"] != "":
                output["fuel"] = item["dennis_fuel_type"]
            if "price" in item and item["price"] is not None and item["price"] != "":
                output["price_retail"] = float(item["price"].replace(",", ""))
                output["currency"] = "GBP"
            if "isNew_B" in item and item["isNew_B"] is not None and item["isNew_B"] != "":
                if item["isNew_B"]:
                    output["is_used"] = 0
                else:
                    output["is_used"] = 1
            if "dealerTown_S" in item and item["dealerTown_S"] is not None and item["dealerTown_S"] != "":
                output["city"] = item["dealerTown_S"]
            if "imageUrls_S" in item and item["imageUrls_S"] is not None and item["imageUrls_S"] != "":
                pictures = []
                for img in item["imageUrls_S"]:
                    pictures.append(f'https://images.buyacar.co.uk{img}')
                if len(pictures) > 0:
                    output['picture_list'] = json.dumps(pictures)
                
            apify.pushData(output)

            if len(results) > 0:
                yield scrapy.Request(url=f'https://www.buyacar.co.uk/_search/v1?f[0]=vehicle_type:car&offset={offset+100}&limit=100', callback=self.parse, cb_kwargs={'offset': offset + 100})




