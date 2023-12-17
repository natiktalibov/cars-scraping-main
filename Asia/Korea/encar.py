import datetime
import json
import scrapy
import apify

class EncarSpider(scrapy.Spider):
    name = 'Encar'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    start_urls = ['https://api.encar.com/search/car/list/premium?count=true&q=(And.Hidden.N._.CarType.Y.)&sr=%7CModifiedDate%7C0%7C20']

    def parse(self, response):
        data = response.json()
        total_records = data["Count"]
        listings = data["SearchResults"]

        current_count = 0
        if "current_count" in response.meta:
            current_count = int(response.meta["current_count"])

        for listing in listings:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Encar'
            output['scraped_listing_id'] = listing["Id"]
            output["vehicle_url"] = f'http://www.encar.com/dc/dc_cardetailview.do?pageid=dc_carsearch&listAdvType=normal&carid={listing["Id"]}'

            output['country'] = 'KR'
            if "OfficeCityState" in listing:
                output["city"] = listing["OfficeCityState"]

            if "Mileage" in listing:
                output["odometer_value"] = int(listing["Mileage"])
                output["odometer_unit"] = "km"

            output["price_retail"] = float(listing["Price"]) * 10000
            output["currency"] = "KRW"

            output["make"] = listing["Manufacturer"]
            output["model"] = listing["Model"]

            if "FormYear" in listing:
                output["year"] = int(listing["FormYear"])

            if "FuelType" in listing:
                fuel = listing["FuelType"]
                if fuel == "가솔린":
                    output["fuel"] = "gasoline"
                if fuel == "디젤":
                    output["fuel"] = "diesel"
                if fuel == "LPG(일반인 구입)":
                    output["fuel"] = "LPG (Purchased by the general public)8,2"
                if fuel == "가솔린+전기":
                    output["fuel"] = "gasoline + electricity"
                if fuel == "LPG+전기":
                    output["fuel"] = "LPG + electricity"
                if fuel == "가솔린+LPG":
                    output["fuel"] = "Hybrid(LPG)"
                if fuel == "전기":
                    output["fuel"] = "electricity"

            if "Transmission" in listing:
                transmission = listing["Transmission"]
                if transmission == "오토":
                    output["transmission"] = "auto"
                if transmission == "수동":
                    output["transmission"] = "manual"
                if transmission == "세미오토":
                    output["transmission"] = "semi-auto"
                if transmission == "CVT":
                    output["transmission"] = "cvt"

            if "Photos" in listing:
                pictures_list = []
                for picture in listing["Photos"]:
                    pictures_list.append(f'http://ci.encar.com/carpicture{picture["location"]}')

            if len(pictures_list) > 0:
                output["picture_list"] = json.dumps(pictures_list)

            apify.pushData(output)

        if current_count < total_records:
            yield scrapy.Request(url=f'https://api.encar.com/search/car/list/premium?count=true&q=(And.Hidden.N._.CarType.Y.)&sr=%7CModifiedDate%7C{current_count+20}%7C20', callback=self.parse, meta={"current_count": current_count + 20})
