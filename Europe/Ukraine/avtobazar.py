import datetime
import json
import scrapy

import apify

class AvtobazarSpider(scrapy.Spider):
    name = 'avtobazar'
    download_timeout = 120
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'referer': 'https://ab.ua/',
    }

    def start_requests(self):
        urls = [
            'https://ab.ua/avto/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//a[@class="_2DqBB stretched"]/@href').getall()
        yield from response.follow_all(urls=product_links, callback=self.detail, headers=self.headers)

        # pagination
        page_link = response.xpath('//li[@class="_2Lfan"]/a/@href').get()
        if page_link is not None:
            yield response.follow(url=page_link, callback=self.parse, headers=self.headers)

    def detail(self,response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'avtobazar'
        output['scraped_listing_id'] = response.url.split('-')[-2]
        output['country'] = 'UA'
        script = response.xpath('//script[@type="application/ld+json"]/text()').getall()[-1]
        jsn = json.loads(script)
        output['make'] = jsn['brand']['name']
        output['model'] = jsn['model']
        output['fuel'] = jsn['fuelType']
        output['transmission'] = jsn['vehicleTransmission']
        output['year'] = int(jsn['productionDate'])
        output['price_retail'] = float(jsn['offers']['price'])
        output['currency'] = jsn['offers']['priceCurrency']
        try:
            output['odometer_value'] = int(jsn['mileageFromOdometer']['value'].replace(' ', '').strip())
            output['odometer_unit'] = 'km'
        except Exception as e:
            pass
        output['city'] = response.xpath('//div[@class="_2Gupx"]/div[1]/div[2]/span[2]/text()').get().strip()
        output['vehicle_url'] = response.url
        img = response.xpath('//img[@class="_1Sghg"]/@src').getall()
        if img:
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