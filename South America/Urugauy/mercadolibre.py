import os
import json
import datetime

import scrapy
from scrapy import Request, Selector

import apify


class MercadolibreSpider(scrapy.Spider):
    name = 'MercadoLibre'
    download_timeout = 120
    # allowed_domains = ['www.mercadolibre.com.uy']
    start_urls = ['https://autos.mercadolibre.com.uy/']

    def parse(self, response):
        sel = Selector(response)
        li_list = sel.xpath(
            '//ol[@class="ui-search-layout ui-search-layout--grid"]/li[@class="ui-search-layout__item"]'
        )
        for li in li_list:
            url = li.xpath('./div//a[@class="ui-search-result__content ui-search-link"]/@href').get()
            yield Request(url=url, meta={'vehicle_url': url}, callback=self.get_data)

        # next page
        # next_url = sel.xpath(
        #     '//li[@class="andes-pagination__button andes-pagination__button--next"]'
        #     '/a[@class="andes-pagination__link ui-search-link"]/@href'
        # ).get()
        next_url = sel.xpath("//a[@title='Siguiente']/@href").get()
        if next_url is not None:
            yield Request(url=next_url, callback=self.parse)

    def get_data(self, response):
        sel = Selector(response)

        output = {}

        # defualt
        output['vehicle_url'] = str(response.meta['vehicle_url'])
        output['scraped_listing_id'] = response.meta['vehicle_url'].split('-')[1]
        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['country'] = 'Uruguay'
        output['scraped_from'] = 'Mercado Libre'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        # get picture
        picture = sel.xpath('//span[@class="ui-pdp-gallery__wrapper"]/figure//img/@data-zoom').getall()
        if picture:
            output['picture_list'] = json.dumps(picture)

        # get price
        price = sel.xpath('//div[@id="price"]//span[@class="andes-money-amount__fraction"]/text()').get()
        if price:
            price = float(price)
            output['price_retail'] = price
            output['price_wholesale'] = price
            output['currency'] = 'USD'

        # get details
        tr_list = sel.xpath('//div[@id="technical_specifications"]//div[@class="ui-pdp-specs__table"]//tr')
        for tr in tr_list:
            key = tr.xpath('./th/text()').get()
            value = tr.xpath('./td/span/text()').get()

            if key == 'Marca' and value:
                output['make'] = value

            elif key == 'Modelo' and value:
                output['model'] = value

            elif key == 'Año' and value:
                output['year'] = int(value)

            elif key == 'Tipo de combustible' and value:
                output['fuel'] = value

            elif key == 'Transmisión' and value:
                output['transmission'] = value

            elif key == 'Kilómetros' and value:
                output['odometer_value'] = int(value.split(' ')[0])
                output['odometer_unit'] = 'km'

        apify.pushData(output)
        # yield output
