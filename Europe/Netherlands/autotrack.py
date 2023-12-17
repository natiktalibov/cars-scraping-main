import datetime
import json
import scrapy
import apify


class AutoTrackSpider(scrapy.Spider):
    name = 'AutoTrack'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    url = f'https://search.autotrack.nl/api/v1/listings/search-with-counters?pageNumber=1&pageSize=90&sortField=relevance&sortOrder=desc&abTest=A'

    def start_requests(self):
       yield scrapy.Request(url=self.url, method="POST", callback=self.parse,  body=json.dumps({}), headers={'Authorization': 'Basic YXBwczo2U21Oa1dSRmJCdGM=', "Content-Type": "application/json"})

    def parse(self, response):
        json_data = response.json()
        results = json_data["hits"]

        for item in results:
            output = dict()

            output['ac_installed'] = 0
            output['tpms_installed'] = 0

            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'AutoTrack'
            output['scraped_listing_id'] = item["advertentieId"]
            output["vehicle_url"] = item["url"]

            output['country'] = 'NL'
            car_data = item["autogegevens"]
            general_data = car_data["algemeen"]
            output["make"] = general_data["merknaam"]
            output["model"] = general_data["modelnaam"]
            if "kleur" in general_data:
                output["exterior_color"] = general_data["kleur"]
            if "brandstofsoort" in general_data:
                output["fuel"] = general_data["brandstofsoort"]
            if "transmissietype" in general_data:
                output["transmission"] = general_data["transmissietype"]
            if "carrosserievorm" in general_data:
                output["body_type"] = general_data["carrosserievorm"]
            if "aantalDeuren" in general_data:
                output["doors"] = int(general_data["aantalDeuren"])

            history_data = car_data["geschiedenis"]
            if "kilometerstand" in history_data:
                output["odometer_value"] = history_data['kilometerstand']
                output["odometer_unit"] = "km"
            if "bouwjaar" in history_data:
                output["year"] = int(history_data["bouwjaar"])

            photos = item["fotos"]
            if len(photos) > 0:
                output['picture_list'] = json.dumps(photos)
            output["price_retail"] = float(item["prijs"]["totaal"])
            output["currency"] = "EUR"
            apify.pushData(output)

        page_size = json_data["pageSize"]
        page_number = json_data["pageNumber"]
        total = json_data['total']
        if page_size * page_number < total:
            url = f'https://search.autotrack.nl/api/v1/listings/search-with-counters?pageNumber={page_number+1}&pageSize=90&sortField=relevance&sortOrder=desc&abTest=A'
            yield scrapy.Request(url=url, method="POST", callback=self.parse, body=json.dumps({}),
                                 headers={'Authorization': 'Basic YXBwczo2U21Oa1dSRmJCdGM=',
                                          "Content-Type": "application/json"})
