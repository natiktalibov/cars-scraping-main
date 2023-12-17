import scrapy
import os
import json
import datetime
import html
import re

import apify


class CarsSpider(scrapy.Spider):
    name = 'cars'

    def start_requests(self):
        urls = [
            'https://www.cars24.com/ae/buy-used-cars-abu-dhabi?sf=city:CC_AE_2&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-fujairah?sf=city:CC_AE_3&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-ras-al-khaimah?sf=city:CC_AE_4&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-ajman?sf=city:CC_AE_5&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-sharjah?sf=city:CC_AE_6&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-umm-al-quwain?sf=city:CC_AE_7&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-al-ain?sf=city:CC_AE_8&sf=gaId:1883552063.1660609699',
            'https://www.cars24.com/ae/buy-used-cars-dubai?sf=city:DU_DUBAI&sf=gaId:1883552063.1660609699',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': 0})

    def parse(self, response, page):
        # Traverse product links
        if page == 0:
            product_links = response.xpath('/html/body/div[1]/div[2]/div[3]/div/div/div/a/@href').getall()
            yield from response.follow_all(product_links, self.detail)
            city_url = response.url.split('/')[-1].split('?')[0].replace('buy-used-cars-', '').strip()
        else:
            # Concatenate details page links
            jsn = response.json()
            if jsn['results'] == []:
                page = 'over'
            else:
                car_id = []
                make = []
                model = []
                year = []
                city = []
                for i in jsn['results']:
                    car_id.append(i['appointmentId'])
                for i in jsn['results']:
                    make.append(i['make'])
                for i in jsn['results']:
                    model.append(i['model'])
                for i in jsn['results']:
                    year.append(i['year'])
                for i in jsn['results']:
                    city.append(i['city'])
                for i in range(len(car_id)):
                    product_link = f'https://www.cars24.com/ae/buy-used-{year[i]}-{make[i]}-{model[i]}-{year[i]}-cars-{city[i]}-{car_id[i]}/'
                    yield response.follow(product_link, self.detail)
                city_url = response.url.split('buy-used-cars-')[-1].split('&page=')[0].strip()

        # pagination
        if page != 'over':
            page += 1
            page_link = f'https://listing-service.c24.tech/v1/vehicle?page={page}&sf=city:CC_AE_6&sf=gaId:1883552063.1660609699&size=25&spath=buy-used-cars-{city_url}&page={page}&variant=filterV3'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
                'x_country': 'AE',
                'x_vehicle_type': 'CAR',
            }
            yield response.follow(url=page_link, callback=self.parse, headers=headers, cb_kwargs={'page': page})

    def detail(self, response):
        product_text = response.xpath('/html/body/script[1]/text()').get()

        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Cars24'
        output['scraped_listing_id'] = re.findall('appointmentId":"(.*?)","make', product_text)[0]
        output['vehicle_url'] = response.url
        output['picture_list'] = json.dumps(response.xpath('//div[@class="Q_GQs"]/span/img/@src').getall())
        output['country'] = 'AE'
        output['city'] = re.findall('city":"(.*?)","cityCode', product_text)[0]
        output['make'] = re.findall('make":"(.*?)","model":"', product_text)[0]
        output['model'] = re.findall('"model":"(.*?)","year":"', product_text)[0]
        output['transmission'] = \
        re.findall('name":"Transmission","value":"(.*?)","key":"transmissionType', product_text)[0]
        output['fuel'] = re.findall('name":"Fuel","value":"(.*?)","key":"fuelType', product_text)[0]
        output['upholstery'] = \
        re.findall('name":"Interior Trim","value":"(.*?)","key":"interiorTrimType', product_text)[0]  ### by NT
        output["drive_train"] = re.findall('"driveType":"(.*?)"', product_text)[0]  ## by NT
        output["body_type"] = re.findall('"bodyType":"(.*?)"', product_text)[0]  ## by NT
        output["exterior_color"] = re.findall('"color":"(.*?)"', product_text)[0]  ## by NT
        output["vin"] = re.findall('"vin":"(.*?)"', product_text)[0]  ## by NT
        output["engine_cylinders"] = int(re.findall('"noOfCylinders":"(.*?)"', product_text)[0])  ## by NT
        output['year'] = int(re.findall('"year":"(.*?)","variant":', product_text)[0])

        if re.findall('price":(.*?),"odometerReading', product_text) is not None:
            output['price_retail'] = float(re.findall('price":(.*?),"odometerReading', product_text)[0])
            output['currency'] = 'AED'

        if re.findall('name":"Engine","value":"(.*?)","key":"engineSize', product_text)[0] is not None:
            output['engine_displacement_value'] = \
            re.findall('name":"Engine","value":"(.*?)","key":"engineSize', product_text)[0].replace('L', '')
            output['engine_displacement_units'] = 'l'

        if re.findall('value":"(.*?)","key":"odometerReading"}', product_text)[0] is not None:
            output['odometer_value'] = int(
                re.findall('"name":"Kilometers","value":"(.*?)","key":"odometerReading"}', product_text)[0])
            output['odometer_unit'] = 'km'

        # process empty fields
        list1 = []
        list2 = []
        for k, v in output.items():
            if v or v == 0:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))

        apify.pushData(output)
