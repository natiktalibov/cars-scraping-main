import re
import json
import math
import scrapy
import datetime
from lxml import etree
from scrapy import FormRequest, Selector, Request

import apify


class DrivenSpider(scrapy.Spider):
    name = 'Driven'
    start_urls = ['https://www.driven.co.nz/umbraco/surface/ListingResults/ListingSearchResults', ]

    # get post requests parameter
    def get_data(self, num=0):
        return {
            'startIndex': str(1 + num * 24),  # 0 25 49 73 97   num = 0
            'endIndex': str((1 + num) * 24),  # 24 48 72
            'pageSize': '24',  # 24
            'bodytype': "",
            'categoryId': '0',
            'colour': "",
            'currentview': 'null',
            'districtId': '0',
            'districtName': "",
            'enginefrom': '0',
            'engineto': '0',
            'fuel': "",
            'keywords': "",
            'listingType': "u",
            'model': "",
            'odometerfrom': '0',
            'odometerto': '0',
            'pricefrom': '0',
            'priceto': '0',
            'reachedEnd': 'false',
            'regionId': '0',
            'regionName': "",
            'sortOrder': "latest",
            'totalResults': '48250',
            'transmission': "",
            'yearfrom': '0',
            'yearto': '0',
        }

    def start_requests(self):
        yield FormRequest(url=self.start_urls[0], formdata=self.get_data(), callback=self.parse, cb_kwargs={'num': 0})

    def parse(self, response, num):
        json_response = json.loads(response.text)
        sel = etree.HTML(json_response["d"]['resultsHtml'])
        if not sel:
            return None
        href_list = sel.xpath('//div[@class="listing-image"]/a/@href')
        for href in href_list:
            url = 'https://www.driven.co.nz' + href
            yield Request(url=url, meta={'vehicle_url': url}, callback=self.get_details)

        # show more
        num += 1
        if num < math.ceil(json_response["d"]['totalResults'] / 24):
            yield FormRequest(url=self.start_urls[0], formdata=self.get_data(num=num), callback=self.parse,
                              cb_kwargs={'num': num})

    def get_details(self, response):
        sel = Selector(response)
        output = {}
        # defualt
        output['vehicle_url'] = str(response.meta['vehicle_url'])
        output['scraped_listing_id'] = re.findall('\d{6,}', response.meta['vehicle_url'])[0]
        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['country'] = 'NZ'
        output['scraped_from'] = 'Driven'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        # get picture
        picture = ['https:' + src for src in sel.xpath('//img[@class="rsTmb"]/@src').getall()]
        if picture:
            output['picture_list'] = json.dumps(picture)

        # get price
        price = sel.xpath('//h3[@class="price"]/strong/text()').get()
        price = ''.join(re.findall('\d{1,}', str(price)))
        if price:
            output['price_retail'] = float(price)
            output['currency'] = 'NZD'

        li_list = sel.xpath('//div[@class="listing-details-container"]/ul/li')
        for li in li_list:
            key = li.xpath('./h5/text()').get()
            value = li.xpath('./p/text()').get()

            if key == 'Odometer' and value:
                try:
                    output['odometer_value'] = int(''.join(re.findall('\d{1,}', value)))
                    if ''.join(re.findall('[a-zA-Z]', value)):
                        output['odometer_unit'] = ''.join(re.findall('[a-zA-Z]', value))
                except TypeError:
                    pass

            elif key == 'Transmission' and value:
                output['transmission'] = value

            elif key == 'Fuel Type' and value:
                output['fuel'] = value

            elif key == 'Body Type' and value:  ## by NT
                output['body_type'] = value  ## by NT

            elif key == 'Engine Size' and value:
                if ''.join(re.findall('\d{1,}', value)):
                    output['engine_displacement_value'] = ''.join(re.findall('\d{1,}', value))
                    if ''.join(re.findall('[a-zA-Z]', value)):
                        output['engine_displacement_units'] = ''.join(re.findall('[a-zA-Z]', value))

            elif key == 'Location' and value:
                output['city'] = value

        # get details
        tr_list = response.xpath('//div[@class="listing-table-info"]/table//tr')
        for tr in tr_list:
            data_list = tr.xpath('./td/text()').getall()
            key = data_list[0]
            try:
                value = data_list[1]
            except IndexError:
                value = ''

            if key == 'Make' and value:
                output['make'] = value

            elif key == 'Model' and value:
                output['model'] = value

            elif key == 'Year' and value:
                output['year'] = int(value)

            elif key == 'Exterior colour' and value:  ## by NT
                output['exterior_color'] = value  ## by NT

            elif key == 'Variant' and value:  ## by NT
                output['trim'] = value  ## by NT

        apify.pushData(output)
        # yield output

