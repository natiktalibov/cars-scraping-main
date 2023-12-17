import datetime
import json
import scrapy
from urllib.parse import urlencode
import apify
from loguru import logger

class CardoorSpider(scrapy.Spider):
    name = 'Cardoor'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    q_params = {
        "x-algolia-agent": "Algolia for JavaScript (4.9.1); Browser (lite); JS Helper (3.4.4)",
        "x-algolia-api-key": "ec7553dd56e6d4c8bb447a0240e7aab3",
        "x-algolia-application-id": "V3ZOVI2QFZ"
    }
    encode_q_params = urlencode(q_params)
    facets = "%5B%22features%22%2C%22our_price%22%2C%22lightning.lease_monthly_payment%22%2C%22lightning.finance_monthly_payment%22%2C%22type%22%2C%22api_id%22%2C%22year%22%2C%22make%22%2C%22model%22%2C%22model_number%22%2C%22trim%22%2C%22body%22%2C%22doors%22%2C%22miles%22%2C%22ext_color_generic%22%2C%22features%22%2C%22lightning.isSpecial%22%2C%22lightning.locations%22%2C%22lightning.status%22%2C%22lightning.class%22%2C%22fueltype%22%2C%22engine_description%22%2C%22transmission_description%22%2C%22metal_flags%22%2C%22city_mpg%22%2C%22hw_mpg%22%2C%22days_in_stock%22%2C%22ford_SpecialVehicle%22%2C%22lightning.locations.meta_location%22%2C%22title_vrp%22%2C%22ext_color%22%2C%22int_color%22%2C%22certified%22%2C%22lightning%22%2C%22location%22%2C%22drivetrain%22%2C%22int_options%22%2C%22ext_options%22%2C%22cylinders%22%2C%22vin%22%2C%22stock%22%2C%22msrp%22%2C%22our_price_label%22%2C%22finance_details%22%2C%22lease_details%22%2C%22thumbnail%22%2C%22link%22%2C%22objectID%22%2C%22algolia_sort_order%22%2C%22date_modified%22%2C%22hash%22%5D"
    url = f'https://v3zovi2qfz-dsn.algolia.net/1/indexes/*/queries?{encode_q_params}'

    def start_requests(self):
        payload = json.dumps({
            "requests": [
                {
                    "indexName": "ridescanadainc_production_inventory_year_high_to_low",
                    "params": f'maxValuesPerFacet=250&page=0&hitsPerPage=20&facets={self.facets}&tagFilters='
                }
            ]
        })

        yield scrapy.Request(url=self.url, method="POST", callback=self.parse,  body=payload, headers={'Content-Type': 'application/json'})

    def parse(self, response):
        json_data = response.json()
        results = json_data["results"][0]["hits"]

        for item in results:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Cardoor'
            output['scraped_listing_id'] = item["objectID"]
            output["vehicle_url"] = item["link"]

            output['country'] = 'CA'
            output["make"] = item["make"]
            output["model"] = item["model"]

            if "body" in item:
                output["body_type"] = item["body"]
            if "cylinders" in item:
                output["engine_cylinders"] = int(item["cylinders"])
            if "year" in item:
                output["year"] = int(item["year"])
            if "trim" in item and item["trim"] != "":
                output["trim"] = item["trim"]
            if "vin" in item and item["vin"] != "":
                output["vin"] = item["vin"]
            if "int_color" in item and item["int_color"] != "":
                output["interior_color"] = item["int_color"]
            if "ext_color" in item and item["ext_color"] != "":
                output["exterior_color"] = item["ext_color"]
            if "drivetrain" in item and item["drivetrain"] != "":
                output["drive_train"] = item["drivetrain"]
            if "doors" in item and item["doors"] != "":
                output["doors"] = int(item["doors"])
            if "fueltype" in item and item["fueltype"] != "":
                output["fuel"] = item["fueltype"]
            if "transmission_description" in item and item["transmission_description"] != "":
                output["transmission"] = item["transmission_description"]
            if "type" in item and item["type"] != "":
                if item["type"] == "Used":
                    output["is_used"] = 1
                else:
                    output["is_used"] = 0
            if "thumbnail" in item:
                output["picture_list"] = json.dumps([item["thumbnail"]])
            if "our_price" in item and isinstance(item["our_price"], int):
                output["price_retail"] = float(item["our_price"])
                output["currency"] = "CAD"
            if "miles" in item and item["miles"] != "":
                output["odometer_value"] = int(item["miles"])
                output["odometer_unit"] = "miles"
            if "location" in item and item["location"] != "":
                city_province = item["location"].split("<br/>")
                if len(city_province) > 1:
                    city_province = city_province[-2]
                    output["city"] = city_province.split(",")[0].strip()
                    output["state_or_province"] = city_province.split(",")[1].split(" ")[1]

            if "ext_options" in item and "int_options" in item:
                if item["ext_options"] is not None and item["int_options"] is not None:
                    output["vehicle_options"] = json.dumps(item["ext_options"] + item["int_options"])
            apify.pushData(output)

        last_page = json_data["results"][0]["nbPages"]
        current_page = json_data["results"][0]["page"]
        if current_page <= last_page:
            payload = json.dumps({
                "requests": [
                    {
                        "indexName": "ridescanadainc_production_inventory_year_high_to_low",
                        "params": f'maxValuesPerFacet=250&page={current_page+1}&hitsPerPage=20&facets={self.facets}&tagFilters='
                    }
                ]
            })
            yield scrapy.Request(url=self.url, method="POST", callback=self.parse,  body=payload, headers={'Content-Type': 'application/json'})
