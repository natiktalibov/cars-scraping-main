import datetime
import json
import scrapy

import apify

class AutoweekSpider(scrapy.Spider):
    name = 'AutoWeek'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.autoweek.nl/v2/occasions/?filters_new=true&ascending=1&sort=publishdate&page=1&format=json',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': 1})

    def parse(self, response, page):
        jsn = response.json()
        if jsn['occasions'] != []:
            for des in jsn['occasions']:
                output = {}
                output['ac_installed'] = 0
                output['tpms_installed'] = 0
                output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
                output['scraped_from'] = 'AutoWeek'
                try:
                    output['scraped_listing_id'] = str(des['occasion_id'])
                except Exception as a:
                    continue
                output['country'] = 'NL'
                try:
                    output['price_retail'] = float(des['price'])
                except Exception as a:
                    continue
                output['currency'] = 'EUR'
                output['year'] = int(des['build'])
                output['make'] = des['url'].split('/')[2]
                output['model'] = des['url'].split('/')[3]
                output['transmission'] = des['transmission']
                output['fuel'] = des['fuel']
                output['city'] = des['location']['city']
                output['odometer_value'] = int(des['mileage'])
                output['odometer_unit'] = 'km'
                output['vehicle_url'] = 'https://www.autoweek.nl' + des['url']
                img = des['media']['thumbnail']
                if img != []:
                    output['picture_list'] = json.dumps(img)
                # process empty fields
                list1 = []
                list2 = []
                for k, v in output.items():
                    if v or v == 0:
                        list1.append(k)
                        list2.append(v)
                output = dict(zip(list1, list2))

                apify.pushData(output)
                # yield output

            # pagination
            page += 1
            page_link = f'https://www.autoweek.nl/v2/occasions/?filters_new=true&ascending=1&sort=publishdate&page={page}&format=json'
            yield response.follow(url=page_link, callback=self.parse, cb_kwargs={'page':page})

