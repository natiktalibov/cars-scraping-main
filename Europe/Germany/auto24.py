import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response
import json

class MySpider(scrapy.Spider):
    name = 'auto24'
    custom_settings = {'USER_AGENT':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    def start_requests(self):
        urls = [
             'https://auto24.de/autos',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        product_links = response.xpath('//article/div/div/div/h3/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        next_link = response.xpath('//li[@class="page-item text-right col-8 col-sm-2 px-sm-0"]/a/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)


    def detail(self, response):
        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "auto24"
        output['country'] = "DE"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('-')[-1].split('.')[0]
        output['tpms_installed'] = 0
        output['ac_installed'] = 0
        output['city'] = [*response.xpath('//address/text()').getall()][1].split(' ')[1] ## by NT

        # get picture
        picture = response.xpath("//div[contains(@class,'card-body')]//img/@src").getall()
        if picture:
            output['picture_list'] = json.dumps(picture)

        labels = [*response.xpath('//div[@class="col-12 bg-gray p-1"]/text()').getall(), *response.xpath('//div[@class="col-12 p-1"]/text()').getall()]
        data = [*response.xpath('//div[@class="col-12 bg-gray p-1 text-break text-wrap"]/descendant-or-self::*/text()').getall(), *response.xpath('//div[@class="col-12 p-1 text-break text-wrap"]/descendant-or-self::*/text()').getall()]
        for i in range(len(data)):
            l = labels[i]
            d = data[i]
            if l == 'Preis':
                if d.split(' ')[0] == "€":
                    output['currency'] = "EUR"
                d = re.sub("[^0-9]", "", d)
                output['price_retail'] = float(d)
            elif l == 'Netto-Preis':
                d = re.sub("[^0-9]", "", d)
                output['price_wholesale'] = d
            elif l == 'Hersteller':
                output['make'] = d
            elif l == 'Modell':
                output['model'] = d
            elif l == 'Erstzulassung':
                output['registration_year'] = d.split('/')[-1] ## by NT
            elif l == 'Kilometerstand':
                output['odometer_value'] = int(re.sub("[^0-9]", "", d))
                output['odometer_unit'] = 'km'
            elif l == 'Getriebe':
                output['transmission'] = d
            elif l == 'Klimatisierung':
                output['ac_installed'] = 1
            elif l == 'Hubraum':
                output['engine_displacement_unit'] = d.split(' ',1)[-1]
                output['engine_displacement_value'] = float(re.sub("[^0-9]", "", d))
            elif l == 'Außenfarbe':   ## by NT
                output['exterior_color']= d  ## by NT
            elif l == 'Kraftstoff':  ## by NT
                output['fuel'] = d ## by NT
            elif l == 'Aufbau': ## by NT
                output['body_type'] = d  ## by NT
            elif l == 'Polsterung': ## by NT
                output['upholstery'] = d  ## by NT
            elif l == 'Innenfarbe': ## by NT
                output['interior_color'] = d  ## by NT
  
        #pictures dont seem to load properly in scrapy
        apify.pushData(output)
