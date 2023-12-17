import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response

class MySpider(scrapy.Spider):
    name = 'autoville'

    def start_requests(self):
        urls = [
             'https://auto-ville.com.ua/en/catalog?a=0&page=1',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):


        product_links = response.xpath('//div[@class="car_card"]/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)


        if len(product_links) > 0:
            current_page = int(response.url.split('=')[-1])
            next_link = 'https://auto-ville.com.ua/en/catalog?a=0&page=' + str(current_page+1)
            yield response.follow(next_link, self.parse)


    def detail(self, response):

        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "autoville"
        output['country'] = "UA"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('-')[-1]
        output['city'] = response.xpath('//div[@class="car_buy_bottom box"]/div/p[2]/b/text()').get().split(',')[0]


        cost = response.xpath('//span[@class="car_card_price_usd"]/text()').get()
        if cost is not None:
            output['price_wholesale'] = cost
            output['currency'] = 'USD'


        data = response.xpath('//div[@class="car_card_param_other"]/div')
        for d in data:
            label = d.xpath('./div/span[1]/text()').get()
            info = d.xpath('./div/span[2]/text()').get()
            if label == 'Release year':
                output['year'] = int(info.strip())
            elif label == 'Brand':
                output['make'] = info
            elif label == 'Model':
                output['model'] = info
            elif label == 'Mileage':
                output['odometer_value'] = re.sub("[^0-9]", "", info)
                output['odometer_unit'] = re.sub("[^a-z]", "", info)
            elif label == 'Gearbox':
                output['transmission'] = info
            elif label == 'Engine capacity':
                info = info.split(' ')
                output['engine_displacement_value'] = info[0]
                output['engine_displacement_unit'] = info[-1]

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['picture_list'] = ','.join(response.xpath('//ul[@class="car_info_images lightSlider lSSlide"]/li/img/@src').getall())
        apify.pushData(output)
