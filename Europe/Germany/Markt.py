import scrapy
import datetime
import re
import json
import apify
from scrapy.http.response import Response

class MySpider(scrapy.Spider):
    name = 'markt'
    def start_requests(self):
        urls = [
             'https://www.markt.de/fahrzeuge/autos/',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        product_links = response.xpath('//ul[@class="clsy-c-result-list "]/li/@data-onclick-url').getall()
        yield from response.follow_all(product_links, self.detail)

        next_link = response.xpath('//a[@class="clsy-c-pagination__next clsy-c-btn--cta clsy-c-btn"]/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)


    def detail(self, response):
        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "markt"
        output['country'] = "DE"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('/')[-2]
        output['tpms_installed'] = 0
        output['ac_installed'] = 0

        # get picture
        picture = response.xpath('//a[@class="clsy-c-expose-media__link clsy-c-expose-media__link--image"]/@data-src').getall()
        if picture:
            output['picture_list'] = json.dumps(picture)


        data = response.xpath('//li[@class="clsy-attribute-list__item"]')
        for d in data:
            label = d.xpath('./span[@class="clsy-attribute-list__label"]/text()').get()
            if label == None:
                continue
            label = label.strip()
            if label == 'Kategorie':
                make = d.xpath('./span[@class="clsy-attribute-list__description"]/a')
                output['make'] = make[-1].xpath('./text()').get()
            elif label == 'Modell':
                output['model'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get()
            elif label == 'Getriebe':
                output['transmission'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get()
            elif label == 'Baujahr':
                output['year'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get()
            elif label == 'Kilometerstand':
                output['odometer_unit'] = 'km'
                output['odometer_value'] = int(re.sub("[^0-9]", "", d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get()))
            elif label == 'Ort':
                city = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get()
                city = re.sub('[0-9]', '', city).strip()
                output['city'] = city.strip()
            elif label == 'Klimatisierung':
                output['ac_installed'] = 1
            elif label == 'Farbe':  ## by NT
                output['exterior_color'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get() ## by NT
            elif label == 'Kraftstoff':  ## by NT
                output['fuel']= d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get() ## by NT
            elif label == 'Bauart': ## by NT
                output['body_type'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get() ## by NT
            elif label == 'Anzahl Sitzplätze': ## by NT
                output['seats'] = d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get() ## by NT
            elif label == 'Anzahl Türen': ## by NT
                output['doors'] = int(d.xpath('./span[@class="clsy-attribute-list__description"]/text()').get().split('/')[0]) ## by NT
            elif label == 'Klimatisierung':  ## by NT
                output['ac_installed'] = 1  ## by NT

        cost = response.xpath('//meta[@property="product:price:amount"]/@content').get()
        if cost is not None:
            output['price_retail'] = float(cost)
        cost = response.xpath('//meta[@property="product:price:currency"]/@content').get()
        if cost is not None:
            output['currency'] = cost

        apify.pushData(output)
