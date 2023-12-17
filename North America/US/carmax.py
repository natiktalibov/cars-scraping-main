import datetime
import scrapy
import apify


class CarmaxSpider(scrapy.Spider):
    name = 'Carmax'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.carmax.com/cars/api/search/run?uri=%2Fcars%2Fall&skip=0&take=24&zipCode=14174&radius=radius-nationwide&shipping=-1&sort=best-match&scoringProfile=GenericV2&visitorID=6e8dc925-3cf3-42aa-8a74-d408a9140118',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'count': 0})

    def parse(self, response, count):
        result = response.json()
        totalCount = result["totalCount"]
        for item in result["items"]:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Carmax'
            output['scraped_listing_id'] = item["stockNumber"]
            output["vehicle_url"] = f'https://www.carmax.com/car/{item["stockNumber"]}'

            output['country'] = 'US'

            if "driveTrain" in item and item["driveTrain"] is not None:
                output["drive_train"] = item["driveTrain"]
            if "engineSize" in item and item["engineSize"] is not None:
                output["engine_displacement_value"] = item["engineSize"].replace("L", "")
                output["engine_displacement_unit"] = "L"
            if "exteriorColor" in item and item["exteriorColor"] is not None:
                output["exterior_color"] = item["exteriorColor"]
            if "interiorColor" in item and item["interiorColor"] is not None:
                output["interior_color"] = item["interiorColor"]
            if "fuelType" in item and item["fuelType"] is not None:
                output["fuel"] = item["fuelType"]
            if "stateAbbreviation" in item and item["stateAbbreviation"] is not None:
                output["state_or_province"] = item["stateAbbreviation"]
            if "storeCity" in item and item["storeCity"] is not None:
                output["city"] = item["storeCity"]
            if "transmission" in item and item["transmission"] is not None:
                output["transmission"] = item["transmission"]
            if "vin" in item and item["vin"] is not None:
                output["vin"] = item["vin"]
            if "year" in item and item["year"] is not None:
                output["year"] = int(item["year"])
            if "series" in item and item["series"] is not None:
                output["trim"] = item["series"]
            if "trim" in item and "trim" not in output and item["trim"] is not None:
                output["trim"] = item["trim"]
            if "model" in item and item["model"] is not None:
                output["model"] = item["model"]
            if "make" in item and item["make"] is not None:
                output["make"] = item["make"]
            if "mileage" in item and item["mileage"] is not None:
                output["odometer_value"] = int(item["mileage"])
                output["odometer_unit"] = "miles"
            if "basePrice" in item and item["basePrice"] is not None:
                output["price_retail"] = float(item["basePrice"])
                output["currency"] = "USD"
            if "body" in item and item["body"] is not None:
                output["body_type"] = item["body"]
            if "cylinders" in item and item["cylinders"] is not None:
                output["engine_cylinders"] = int(item["cylinders"])
            if "features" in item:
                for feature in item['features']:
                    if feature == "Air Conditioning":
                        output['ac_installed'] = 1
                    if feature == "Leather Seats":
                        output['upholstery'] = "leather"
            apify.pushData(output)

        if count < totalCount:
            count += 24
            link = f'https://www.carmax.com/cars/api/search/run?uri=%2Fcars%2Fall&skip={count}&take=24&zipCode=14174&radius=radius-nationwide&shipping=-1&sort=best-match&scoringProfile=GenericV2&visitorID=6e8dc925-3cf3-42aa-8a74-d408a9140118'
            yield scrapy.Request(url=link, callback=self.parse, cb_kwargs={'count': count})

