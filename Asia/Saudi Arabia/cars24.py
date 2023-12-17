import scrapy
import json
import datetime
import apify


class CarsSpider(scrapy.Spider):
    name = 'cars24'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/103.0.0.0 Safari/537.36',
        'x_country': 'KSA',
        'x_vehicle_type': 'CAR',
        'x_language': 'EN'
    }

    def start_requests(self):
        urls = [
            'https://listing-service-sa.c24.tech/v1/vehicle?sf=city:CC_SA_3&size=25&spath=buy-used-cars-riyadh'
            '&variant=filterV2&page=0',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers, cb_kwargs={'page': 0})

    def parse(self, response, page):
        # Traverse product links
        jsn = response.json()
        results = jsn["results"]
        total = int(jsn["total"])
        product_links = []
        for record in results:
            link = f'https://listing-service-sa.c24.tech/v1/vehicle/{record["appointmentId"]}'
            product_links.append(link)

        for link in product_links:
            yield scrapy.Request(url=link, callback=self.product_detail, headers=self.headers)

        # pagination
        if page != total:
            page += 1
            page_link = f'https://listing-service-sa.c24.tech/v1/vehicle?sf=city:CC_SA_3&size=25&spath=buy-used-cars-riyadh&variant=filterV2&page={page}'
            print(page_link)

            yield response.follow(url=page_link, callback=self.parse, headers=self.headers, cb_kwargs={'page': page})

    def product_detail(self, response):
        details = response.json()
        output = dict()
        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        meta = details["detail"]
        # scraping info
        output["vehicle_url"] = meta["shareUrl"]
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Cars24"
        output["scraped_listing_id"] = meta["appointmentId"]

        # location info
        output["city"] = meta["city"]
        output["country"] = "SA"

        # basic info
        if "make" in meta:
            output["make"] = meta["make"]
        if "model" in meta:
            output["model"] = meta["model"]
        if "trim" in meta:
            output["trim"] = meta["trim"]
        if "variant" in meta:
            output["trim"] = meta["variant"]
        if "year" in meta:
            output["year"] = int(meta["year"])

        # odometer info
        if "odometerReading" in meta:
            output["odometer_value"] = int(meta["odometerReading"])
            output["odometer_unit"] = "km"

        # other data
        if "transmissionType" in meta:
            output["transmission"] = meta["transmissionType"]
        if "fuelType" in meta:
            output["fuel"] = meta["fuelType"]
        if "bodyType" in meta:
            output["body_type"] = meta["bodyType"]
        if "noOfCylinders" in meta:
            output["engine_cylinders"] = int(meta["noOfCylinders"])
        if "interiorTrimType" in meta:
            output["upholstery"] = meta["interiorTrimType"]
        if "driveType" in meta:
            output["drive_train"] = meta["driveType"]
        if "color" in meta:
            output["exterior_color"] = meta["color"]
        if "engineSize" in meta:
            output["engine_displacement_value"] = meta["engineSize"]
            output["engine_displacement_units"] = "L"

        features = meta["basicDetails"]
        for feature in features:
            feature_name = feature["name"]
            if feature_name == "Seating Capacity":
                output["seats"] = int(feature["value"])
            if feature_name == "VIN Number":
                output["vin"] = feature["value"]

        # pricing details
        if "targetPrice" in meta and meta["targetPrice"] is not None:
            output["price_retail"] = float(meta["targetPrice"])
            output["currency"] = "SAR"
        if "price" in meta and meta["price"] is not None:
            if meta["price"] != meta["targetPrice"]:
                output["promotional_price"] = float(meta["price"])

        # pictures
        pictures = meta["featureSpecImages"]
        picture_list = []
        for picture in pictures:
            picture_list.append(f'https://fastly-production.24c.in/{picture["path"]}')
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        apify.pushData(output)
