import scrapy
import datetime
import json
import os
from scrapy import Request,Selector

import apify


class VoituresSpider(scrapy.Spider):
    name = 'voitures'
    download_timeout = 120
    start_urls = ['https://www.voitures.ci/en/vehicle_listings?car_type=voitures&listing%5Bcondition%5D=2']

    def parse(self, response):
        sel = Selector(response)
        div_list = sel.css('div#ads-list a.common-ad-card::attr(href)').getall()
        for item in div_list:
            #construct the url
            url = 'http://www.voitures.ci' + item
            #get datas
            yield Request(url=url,meta={'vehicle_url':url},callback=self.getData)

        next_page = sel.css('a.next_page::attr(href)').get()
        next_url = 'http://www.voitures.ci' + next_page
        if next_url is not None:
            yield Request(url=next_url, callback=self.parse)


    '''get data'''
    def getData(self,response):
        sel = Selector(response)
        output = {}

        #defualt
        output['currency'] = 'XOF'
        output['vehicle_url'] = response.meta['vehicle_url']
        output['scraped_listing_id'] = response.meta['vehicle_url'].split('-')[-1]
        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_from'] = 'Voitures'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['country'] = "CI"

        response.css('div.vehicle-properties div.prop div span').getall()

        #get image
        output['picture_list'] = json.dumps(response.css('div.slider-nav div img::attr(data-src)').getall())

        #get price
        output['price_retail'] = float(sel.css('div.ad-info-wrapper div.ad-price span.price-wrap span.price::text').get().replace(' ',''))

        #get city
        title = sel.css('div.ad-title')
        output['city'] = title.css('a span::text').get()
        car_list = title.css('h2::text').get().split(' ')
        output['make'] = car_list[0]
        try:
            output['model'] = car_list[1]
        except:
            pass

        #get content
        content_list = response.css('div.vehicle-properties div.prop')
        for content in content_list:
            items = content.css('div span::text').getall()
            try:
                if items[0] == 'Engine':
                    output['engine_displacement_value'] = items[1]  ## by NT
                    output['engine_displacement_units'] = 'L'  ## by NT

                elif items[0] == 'Gearbox':  ## by NT
                    output['transmission'] = items[1] ## by NT

                elif items[0] == 'Mileage':  ## by NT
                    output['odometer_value'] = int(''.join(items[1].split(' ')[:-1]))  ## by NT
                    output['odometer_unit'] = items[1].split(' ')[-1]  ## by NT

                elif items[0] == 'Year':  ## by NT
                    output['year'] = int(items[1])  ## by NT

                elif items[0] == 'Fuel Type':  # by NT
                    output['fuel'] = items[1]  # by NT

                elif items[0] == 'Color':  # by NT
                    output['exterior_color'] = items[1]  # by NT

                elif items[0] == 'Air Con':  # by NT
                    output['ac_installed'] = 1  # by NT

                elif items[0] == 'Body Type':  # by NT
                    output['body_type'] = items[1]  # by NT

                elif items[0] == 'Condition':  # by NT
                    output['is_used'] = items[1]  # by NT

                elif items[0] == 'Drive Type':  # by NT
                    output['steering_position'] = items[1]  # by NT

            except Exception as e:
                pass

        apify.pushData(output)
        # yield output
