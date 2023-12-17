import json
import os
import datetime
import re
import os
import scrapy
from scrapy import Selector, Request

import apify


class CoinafriqueSpider(scrapy.Spider):
    name = 'coinafrique'
    download_timeout = 120
    allowed_domains = ['ci.coinafrique.com']
    start_urls = ['https://ci.coinafrique.com/categorie/voitures']

    def parse(self, response):
        sel = Selector(response)

        href_list = sel.xpath(
            '//a[has-class("card-image","ad__card-image","waves-block","waves-light")]/@href'
        ).getall()
        for href in href_list:
            url = 'https://' + self.allowed_domains[0] + href
            yield Request(url=url, meta={'vehicle_url': url}, callback=self.get_data)

        # next page
        next_url = sel.xpath(
            '//li[has-class("pagination-indicator","direction")]/a/span[contains(@class,"next")]/../@href'
        ).get()
        if next_url is not None:
            url = self.start_urls[0] + next_url
            yield Request(url=url, callback=self.parse)

    def get_data(self, response):
        sel = Selector(response)

        output = {}

        # defualt
        output['vehicle_url'] = str(response.meta['vehicle_url'])
        output['scraped_listing_id'] = response.meta['vehicle_url'].split('/')[-1].split('-')[-1]
        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_from'] = 'CoinAfrique'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        # get picture
        output['picture_list'] = json.dumps(sel.xpath(
            '//div[has-class("slider-thumbs","swiper-container")]//div[has-class("swiper-slide")]/@style'
        ).re('\((.*)\)'))

        price = sel.xpath('//p[@class="price"]/text()').get()
        if price != 'Prix sur demande':
            output['price_retail'] = float(''.join(re.findall('\d', price)))
            currency = ''.join(re.findall('\D', price)).replace(' ', '')
            if currency == "CFA":
                output['currency'] = "XOF"

        place = response.xpath(
            '//div[has-class("row","valign-wrapper","extra-info-ad-detail")]'
            '//img[contains(@src,"https://static.coinafrique.com/static/images/location_11_16.png")]/../span/text()'
        ).get()
        country = place.split(',')[-1].strip()
        if country == "Côte d'Ivoire":
            output['country'] = "CI"
        try:
            output['city'] = place.split(',')[-2].strip()
        except ImportError:
            pass

        li_list = sel.xpath('//div[has-class("details-characteristics")]/ul/li')
        for li in li_list:
            key = li.xpath('.//span[1]/text()').get()
            value = li.xpath('.//span[2]/text()').get()

            if key == 'Constructeur' and value != 'N/A':
                output['make'] = value

            elif key == 'Modèle' and value != 'N/A':
                output['model'] = value

            elif key == 'Kilométrage' and value.split(' ')[0] != 'undefined':
                output['odometer_value'] = int(value.split(' ')[0])
                try:
                    output['odometer_unit'] = value.split(' ')[1]
                except ImportError:
                    pass

            elif key == 'Transmission' and value != 'Non renseigné':
                output['transmission'] = value

            elif key == 'Carburant' and value != 'Non renseigné':
                output['fuel'] = value

        year = sel.xpath(
            '//h1[has-class("title","title-ad","hide-on-large-and-down")]/text()'
        ).re_first('(20\d\d|19\d\d)')
        if year:
            output['year'] = int(year)

        apify.pushData(output)
        # yield output
