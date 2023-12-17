import datetime
import json
import scrapy

import apify
class AutotraderSpider(scrapy.Spider):
    name = 'AutoTrader'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.autotrader.nl/auto/zoeken/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//a[@class="css-kshabm"]/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        page_link = response.xpath('//a[@class="page-nav-next"]/@href').get()
        if page_link is not None:
            yield response.follow(page_link, self.parse)

    def detail(self,response):
        price = response.xpath('//span[@class="css-1uilb84"]/text()').get()
        if price:
            output = {}

            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'AutoTrader'
            output['scraped_listing_id'] = response.url.split('/')[-2]
            output['country'] = 'NL'
            try:
                output['city'] = response.xpath('//div[@class="css-kv0743 css-14i4j5l"]/div/div[4]/main/div[6]/div/div[2]/div[1]/p/text()').get().split(' ')[-1].strip()
            except Exception as e:
                output['city'] = response.xpath('//div[@class="css-kv0743 css-14i4j5l"]/div/div[4]/main/div[5]/div/div[2]/div[1]/p/text()').get().split(' ')[-1].strip()
            output['make'] = response.xpath('//div[@class="css-hvk2ao"]/div/a[2]/text()').get()
            output['model'] = response.xpath('//div[@class="css-hvk2ao"]/div/a[3]/text()').get()
            output['price_retail'] = float(price.replace('€', '').replace(',', '').replace('.', '').replace('-', '').strip())
            output['currency'] = 'EUR'

            des = response.xpath('//ul[@class="css-1xc4vku"]/li').getall()
            for i in range(len(des)):
                des_key = response.xpath(f'//ul[@class="css-1xc4vku"]/li[{i+1}]/span[1]/text()').get()
                des_value = response.xpath(f'//ul[@class="css-1xc4vku"]/li[{i+1}]/span[2]/text()').get()
                if des_key == 'Brandstof':
                    output['fuel'] = des_value.strip()
                elif des_key == 'Schakeling':
                    output['transmission'] = des_value.strip()
                elif des_key == 'Bouwjaar':
                    output['year'] = int(des_value.split(' ')[-1].strip())
                elif des_key == 'Kilometerstand':
                    output['odometer_value'] = int(des_value.replace('.', '').replace('km', '').strip())
                    output['odometer_unit'] = 'km'

            output['vehicle_url'] = response.url
            img = response.xpath('//div[@class="thumbs-container"]//div/img/@src').getall()
            output['picture_list'] = json.dumps(img)

            # process empty fields
            list1 = []
            list2 = []
            for k, v in output.items():
                if v or v == 0:
                    list1.append(k)
                    list2.append(v)
            output = dict(zip(list1, list2))

            # yield output
            apify.pushData(output)
