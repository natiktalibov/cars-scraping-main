import scrapy
import json
import datetime
import apify

class CarswitchcarsSpider(scrapy.Spider):
    name = 'carswitchcars'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://ksa.carswitch.com/en/saudi/used-cars/search',
        ]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=headers, cb_kwargs={'page':1})

    def parse(self, response, page):
        # Traverse product links
        product_links = response.xpath("//a[@class='image-wrapper']/@href").getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        page_key = response.xpath('//div[@class="inner"]/a').getall()
        page_link = response.xpath(f'//div[@class="inner"]/a[{len(page_key)}]/@href').get()
        if '/saudi/used-cars/' in page_link:
            page += 1
            page_link = f'https://ksa.carswitch.com/en/saudi/used-cars/search?page={page}'
            yield response.follow(url=page_link, callback=self.parse,cb_kwargs={'page': page})


    def detail(self, response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Carswitch'
        output['scraped_listing_id'] = response.url.split('/')[-1]
        output['country'] = 'SA'
        price_retail = response.xpath('//div[@class="contact-form"]/div/div[@class="price"]/text()').get()
        if price_retail:
            output['price_retail'] = float(price_retail.replace('SAR', '').replace(',', '').strip())
        output['currency'] = 'SAR'

        # description head
        output['make'] = response.url.split('/')[-4]
        output['model'] = response.url.split('/')[-3]

        div = response.xpath('//div[@class="title-holder"]/div').getall()

        for i in range(len(div)):
            year = response.xpath(f'//div[@class="title-holder"]/div[{i + 1}]/div[2]/div/div[1]/span/text()').get()
            if year is not None:
                output['year'] = int(year.strip())
            else:
                continue
            city = response.xpath(f'//div[@class="title-holder"]/div[{i + 1}]/div[3]/span/text()').get()
            if city is not None:
                output['state_or_province'] = city.split(',')[-1].strip()  ## by NT
                output['city'] = city.split(',')[0].strip()  ## by NT
            odometer_value = response.xpath(f'//div[@class="title-holder"]/div[{i + 1}]/div[2]/div/div[2]/span/text()').get()
            if odometer_value is not None:
                if 'KM' in odometer_value:
                    output['odometer_value'] = int(odometer_value.replace('KM', '').replace(',', '').strip())
                    output['odometer_unit'] = 'km'
                    break
                elif 'Miles' in odometer_value:
                    output['odometer_value'] = int(odometer_value.replace('Miles', '').replace(',', '').strip())
                    output['odometer_unit'] = 'miles'
                    break

        feature_name = response.xpath('//div[@class="features-list"]/div/div[1]').getall()
        for i in range(len(feature_name)):
            des_key = response.xpath(f'//div[@class="features-list"]/div[{i + 1}]/div[1]/text()').get()
            des_value = response.xpath(f'//div[@class="features-list"]/div[{i + 1}]/div[2]/text()').get()
            if des_key is not None:
                if 'Transmission Type' in des_key:
                    output['transmission'] = des_value.strip()
                    continue
                if 'Fuel Type' in des_key:
                    if des_value:
                        output['fuel'] = des_value.strip()
                    break
                if 'Body Type' in des_key:  ## by NT
                    output['body_type'] = des_value.strip()  ## by NT
                if 'Number of Seats' in des_key:  ## by NT
                    output['seats'] = des_value.strip()  ## by NT
                if 'Number Of Cylinders' in des_key:  ## by NT
                    output['engine_cylinders'] = int(des_value.strip())  ## by NT
                if 'Drive Type' in des_key:  ## by NT
                    output['drive_train'] = des_value.strip()  ## by NT

        output['vehicle_url'] = response.url
        img = response.xpath('//div[@class="slides-holder"]/div/img[1]/@data-src').getall()
        img.append(response.xpath('//div[@class="slides-holder"]/div/img[1]/@src').get())
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

