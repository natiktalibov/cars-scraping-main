import scrapy
import os
import json
import datetime

import apify


class CarsensorcarsSpider(scrapy.Spider):
    name = 'carsensor'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.carsensor.net/usedcar/index1.html',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page':1})


    def parse(self, response, page):
        # Traverse product links
        product_links = response.xpath("/html/body/div[1]/div[3]/div[2]/div[3]/div/div/div[1]/div/div[2]/h3/a/@href").getall()
        yield from response.follow_all(product_links, self.detail)

        # Add one more judgment on the last page
        last_page_button = response.xpath("//a[contains(text(), '最後')]/@href").get()

        # pagination
        page_next = response.xpath('/html/body/div[1]/div[3]/div[3]/div/div[2]/button[2]/@onclick').get()
        # if page_next != None:
        if page_next and last_page_button:
            page += 1
            page_link = f'https://www.carsensor.net/usedcar/index{page}.html'
            yield response.follow(url=page_link, callback=self.parse, cb_kwargs={'page':page})

    def detail(self, response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'CarsensorJP'
        output['scraped_listing_id'] = response.url.split('/')[-2]
        output['vehicle_url'] = response.url
        output['picture_list'] = json.dumps(response.xpath('//*[@id="js-slider"]/div/ul/li/a/img/@src').getall())
        output['country'] = 'JP'

        # description head
        try:
            price_retail = ''
            for i in response.xpath('/html/body/div[1]/div[2]/main/section/div/div[2]/div[1]/div[2]/p[2]/span/text()').getall():
                price_retail += i.strip()
            output['price_retail'] = float(price_retail)*10000
            output['currency'] = 'JPY'
        except Exception:
            return

        output['make'] = response.xpath('/html/body/div[1]/div[2]/div/ul/li[3]/a/span/text()').get().replace('の中古車', '').strip()
        output['model'] = response.xpath('/html/body/div[1]/div[2]/div/ul/li[4]/a/span/text()').get().replace('の中古車', '').strip()

        if response.xpath('//div[@class="specWrap"]/div[1]/p[2]/text()').get().strip() != '不明':
            year = response.xpath('//div[@class="specWrap"]/div[1]/p[2]/text()').get()
            if year:
                output['year'] = int(year.strip())
        if response.xpath('//div[@class="specWrap"]/div[2]/p[2]/text()').get().strip() != '不明':
            odometer_value = response.xpath('//div[@class="specWrap"]/div0[2]/p[2]/text()').get()    
            if odometer_value and odometer_unit.strip().isdigit():
                output['odometer_value'] = int(float(odometer_value)*10000)
                output['odometer_unit'] = 'km'

        city = ''
        for i in response.xpath(
                '/html/body/div[1]/div[2]/main/section/div/div[2]/div[2]/div[5]/p[2]/text()').getall():
            city += i.strip()
        output['city'] = city


        for i in range(1,11):
            if response.xpath(f'/html/body/div[1]/div[4]/div/div[1]/section[{i}]/h2/@id').get() == 'sec-kihon':
                output['engine_displacement_value'] = response.xpath(f'/html/body/div[1]/div[4]/div/div[1]/section[{i}]/div/table/tbody/tr[4]/td[1]/text()').get().replace('cc', '').strip()
                output['engine_displacement_units'] = 'cc'
                output['fuel'] = response.xpath(f'/html/body/div[1]/div[4]/div/div[1]/section[{i}]/div/table/tbody/tr[5]/td[1]/text()').get().strip()

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
