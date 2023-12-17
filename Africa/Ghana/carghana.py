import scrapy
import os
import json
import datetime

import apify


class CarghanaSpider(scrapy.Spider):
    name = 'carghana'
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    download_timeout = 120
    allowed_domains = ['carghana.com']

    def start_requests(self):
        urls = [
            'https://www.carghana.com/buy-cars2',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath("/html/body/main/div[1]/div/div[2]/div[2]/div[3]/a[*]/@href").getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        page_link = response.xpath('//*[@id="ads-list"]/div[7]/div/a[@class="next_page"]/@href').get()
        if page_link is not None:
            yield response.follow(page_link, self.parse)

    def detail(self, response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Carghana'
        output['scraped_listing_id'] = response.url.split('-')[-1]
        output['vehicle_url'] = response.url
        output['picture_list'] = json.dumps(
            response.xpath('//div[@class="carousel-wrapper"]/div[@class="slider-for"]/div/img/@src').getall())
        output['country'] = 'GH'

        # description head
        title = response.xpath('/html/body/main/div[1]/div[1]/div/div/div[2]/div/h2/text()').get().strip().split(' ')
        brand_vehicles = response.xpath('/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div[1]/h2/text()').getall()
        make = ''
        for t in title:
            for b in brand_vehicles:
                if 'Buy' in b:
                    if t in b:
                        make += t
                        make += ' '
        output['make'] = make.strip()
        output['model'] = response.xpath('/html/body/main/div[1]/div[1]/div/div/div[2]/div/h2/text()').get().replace(
            make.strip(), '').strip()

        try:
            output['price_retail'] = float(response.xpath(
                '/html/body/main/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div[2]/span/span/text()').get().replace(',',
                                                                                                                   '').strip())
        except Exception:
            return
        output['currency'] = 'GHS'
        output['city'] = response.xpath('/html/body/main/div[1]/div[1]/div/div/div[2]/div/a/span/text()').get().strip()

        des_key = response.xpath(
            "/html/body/main/div[1]/div[2]/div[2]/div[1]/div[7]/div[3]/div/div/span[1]/text()").getall()
        des_key = [v.strip() for v in des_key]
        des_value = response.xpath(
            "/html/body/main/div[1]/div[2]/div[2]/div[1]/div[7]/div[3]/div/div/span[2]/text()").getall()
        des_value = [v.strip() for v in des_value]

        for i in range(len(des_key)):
            if des_key[i] == 'Engine':
                if des_value[i] != 'N/A':
                    output['engine_displacement_value'] = des_value[i].replace('L', '').strip()
                    output['engine_displacement_units'] = 'L'

            elif des_key[i] == 'Gearbox':
                if des_value[i] != 'N/A':
                    output['transmission'] = des_value[i]

            elif des_key[i] == 'Mileage':
                if des_value[i] != 'N/A':
                    output['odometer_value'] = int(des_value[i].replace('km', '').replace(',', '').strip())
                    output['odometer_unit'] = 'km'

            elif des_key[i] == 'Year':
                if des_value[i] != 'N/A':
                    output['year'] = int(des_value[i])

            elif des_key[i] == 'Fuel Type':
                if des_value[i] != 'N/A':
                    output['fuel'] = des_value[i]

            elif des_key[i] == 'Color':  ## by NT
                if des_value[i] != 'N/A':  ## by NT
                    output['exterior_color'] = des_value[i]  ## by NT

            elif des_key[i] == 'Air Con':  ## by NT
                output['ac_installed'] = 1  ## by NT

            elif des_key[i] == 'Body Type':  ## by NT
                if des_value[i] != 'N/A':  ## by NT
                    output['body_type'] = des_value[i]  ## by NT

            elif des_key[i] == 'Condition':  ## by NT
                if des_value[i] != 'N/A':  ## by NT
                    output['is_used'] = des_value[i]  ## by NT

            elif des_key[i] == 'Drive Type':  ## by NT
                if des_value[i] != 'N/A':  ## by NT
                    output['steering_position'] = des_value[i]  ## by NT

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
