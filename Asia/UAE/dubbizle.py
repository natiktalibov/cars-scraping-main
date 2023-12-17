import scrapy
import os
import json
import datetime
import html
import re

import apify


class DubizzlecarsSpider(scrapy.Spider):
    name = 'dubizzlecars'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://uae.dubizzle.com/motors/used-cars/?page=1',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//*[@id="listings-top"]/div/div[1]/div/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        url_page = response.url.split('/')[-1].replace('?page=', '')
        page_link = response.xpath('//a[@title="Go to next page"]/@href').get().strip()
        next_page = page_link.split('/')[-1].replace('?page=', '')
        if url_page != next_page:
            yield response.follow(page_link, self.parse)

    def detail(self, response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Dubizzle'
        output['scraped_listing_id'] = response.url.split('/')[-2].split('---')[-1]
        output['vehicle_url'] = response.url
        output['picture_list'] = json.dumps(response.xpath(
            '//*[@id="__next"]/div[2]/div/div[3]/div/div[4]/div[1]/div[1]/div[2]/div/div[1]/div[1]/div/div/div/img/@src').getall())
        output['country'] = 'AE'
        output['currency'] = 'AED'

        # output['city'] = response.xpath('//div[@class="sc-il7hse-10 kIZzUo"]/text()').getall()[1].split(',')[-2].strip()
        city = response.xpath("//div[@class='sc-bdnxRM kzWrME']/span/text()").get()
        if city:
            output['city'] = city.split(",")[-2].strip()
        price = response.xpath('//p[@class="sc-1q498l3-0 sc-1q498l3-1 WNwBg jwMZvh sc-1pns9yx-2 fVIUHE"]/text()').get()
        if price is not None:
            if price and 'درهم' in price:
                output['price_retail'] = float(price.replace('درهم', '').replace(',', '').strip())
            else:
                output['price_retail'] = float(price.replace('AED', '').replace(',', '').strip())

        output['make'] = response.url.split('/')[-7]
        output['model'] = response.url.split('/')[-6]

        for i in range(len(response.xpath('//div[@class="sc-bdnxRM gklgS"]/div').getall())):
            if 'Kilometers' == response.xpath(f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/p/text()').get():
                if response.xpath(f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/p/text()').get().strip() != '0':
                    output['odometer_value'] = int(response.xpath(
                        f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/div/p/text()').get().strip())
                    output['odometer_unit'] = 'km'
            if 'كيلومترات' == response.xpath(f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/p/text()').get():
                if response.xpath(f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/p/text()').get().strip() != '0':
                    output['odometer_value'] = int(response.xpath(
                        f'//div[@class="sc-bdnxRM gklgS"]/div[{i}]/div/div[3]/div/p/text()').get().strip())
                    output['odometer_unit'] = 'km'

        # description head
        des_key = response.xpath(
            '//*[@id="__next"]/div[2]/div/div[3]/div/div[4]/div[1]/div[3]/ul/li/div/div/div[1]/div/p/text()').getall()
        des_key = [v.strip() for v in des_key]
        des_value = response.xpath(
            '//*[@id="__next"]/div[2]/div/div[3]/div/div[4]/div[1]/div[3]/ul/li/div/div/div[2]/div/p/text()').getall()
        des_value = [v.strip() for v in des_value]
        for i in range(len(des_key)):
            if des_key[i] == 'Year':
                output['year'] = int(des_value[i])
            elif des_key[i] == 'Transmission Type':
                output['transmission'] = des_value[i].replace('Transmission', '').strip()
            elif des_key[i] == 'Fuel Type':
                output['fuel'] = des_value[i]  ##by NT
            elif des_key[i] == 'Doors':
                output['doors'] = int(des_value[i].split()[0])  ##by NT
            elif des_key[i] == 'No. of Cylinders':  ##by NT
                output['engine_cylinders'] = int(des_value[i])  ##by NT
            elif des_key[i] == 'السنة':
                output['year'] = int(des_value[i])
            elif des_key[i] == 'نوع النقل':
                output['transmission'] = des_value[i].replace('Transmission', '').strip()
            elif des_key[i] == 'نوع الوقود':
                output['fuel'] = des_value[i]

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
