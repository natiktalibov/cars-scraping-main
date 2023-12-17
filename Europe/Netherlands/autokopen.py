import datetime
import json
import scrapy

import apify


class AutokopenSpider(scrapy.Spider):
    name = 'autokopen'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.autokopen.nl/tweedehands-auto/overzicht?',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': 1})

    def parse(self, response, page):
        # Traverse product links
        product_links = response.xpath('//a[@class="clearfix"]/@href').getall()
        yield from response.follow_all(product_links, self.detail)
        # pagination
        li = response.xpath('//ul[@class="pagination"]/li').getall()
        last_page = response.xpath(f'//ul[@class="pagination"]/li[{len(li) - 1}]/a/text()').get()
        if page != int(last_page) or page == 1:
            page += 1
            page_link = f'https://www.autokopen.nl/tweedehands-auto/overzicht?page={page}'
            yield response.follow(url=page_link, callback=self.parse, cb_kwargs={'page': page})

    def detail(self, response):
        price = response.xpath('//strong[@class="vehicle-price hidden-xxs"]/text()').get()
        if price:
            output = {}
            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'autokopen'
            output['scraped_listing_id'] = response.url.split('/')[-1].split('-')[0]
            output['country'] = 'NL'
            text = response.xpath('//script[@type="application/ld+json"]/text()').get()
            jsn = json.loads(text)
            output['make'] = jsn['mainEntity']['brand']['name']
            output['model'] = jsn['mainEntity']['model']
            output['price_retail'] = float(jsn['mainEntity']['offers']['price'])
            output['currency'] = 'EUR'
            output['fuel'] = jsn['mainEntity']['fuelType']
            output['transmission'] = jsn['mainEntity']['vehicleTransmission']
            ### new fields
            output['body_type'] = jsn['mainEntity']['bodyType']
            output['doors'] = int(jsn['mainEntity']['numberOfDoors'])
            output['exterior_color'] = jsn['mainEntity']['color']
            output['engine_displacement_value'] = jsn['mainEntity']['vehicleEngine']['engineDisplacement']['value']
            output['engine_displacement_unit'] = "cc"
            upholstery_keys = response.xpath('//div[@class="property-lining"]//dt//text()').getall()
            upholstery_values = response.xpath('//div[@class="property-lining"]//dd//text()').getall()
            for k in range(len(upholstery_keys)):
                key = upholstery_keys[k]
                value = upholstery_values[k]
                if value != "-":
                    if key == "Materiaal:":
                        output["upholstery"] = value
                    if key == "Kleur:":
                        output['interior_color'] = value
            vehicle_highlights = response.xpath('//div[@class="vehicle-highlights clearfix"]//li//text()').getall()
            if "Airconditioning" in vehicle_highlights:
                output['ac_installed'] = 1
            ###
            text = response.xpath('//span[@class="main-details visible-xs-block visible-sm-block"]/text()').get()
            try:
                output['year'] = int(text.split('-')[0].strip())
            except Exception as e:
                pass
            try:
                output['odometer_value'] = int(text.split('-')[1].replace('.', '').replace('km', '').strip())
                output['odometer_unit'] = 'km'
            except Exception as e:
                pass
            output['vehicle_url'] = response.url
            img = response.xpath('//a[@class="rsImg"]/@href').getall()
            for i in range(len(img)):
                img[i] = 'https:' + img[i]
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
