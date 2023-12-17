import datetime
import json
import scrapy

import apify

class GancxadebebiSpider(scrapy.Spider):
    name = 'Gancxadebebi'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://gancxadebebi.ge/en/classified-ads/Cars-Vehicles-2/Used-Cars-12?mc=price+of+used+car+for+sale+in+tbilisi+georgia&search=Search',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse,cb_kwargs={'page':1})

    def parse(self, response,page):
        # Traverse product links
        product_links = response.xpath('//ul[@class="ua"]/li/div[2]/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        url_page = response.url.split('&')[-2]
        if 'page' in url_page:
            url_page = url_page.replace('page=', '')
            if str(page) == url_page:
                page += 1
                page_link = f'https://gancxadebebi.ge/en/classified-ads/Cars-Vehicles-2/Used-Cars-12?mc=price+of+used+car+for+sale+in+tbilisi+georgia&page={page}&search=Search'
                yield response.follow(page_link, self.parse, cb_kwargs={'page':page})
        else:
            page_link = 'https://gancxadebebi.ge/en/classified-ads/Cars-Vehicles-2/Used-Cars-12?mc=price+of+used+car+for+sale+in+tbilisi+georgia&page=2&search=Search'
            page += 1
            yield response.follow(page_link, self.parse, cb_kwargs={'page':page})

    def detail(self, response):
        output = {}
        price = response.xpath('//div[@itemprop="offers"]/div/span[2]/text()').get()
        if price:
            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Gancxadebebi'
            output['scraped_listing_id'] = response.url.split('?')[0].split('GEO')[-1]
            output['country'] = 'GE'
            make = response.xpath('//li[@class="dtl_marque_id fl3"]/text()').get()
            if make:
                output['make'] = make.strip()
                output['model'] = response.url.split(f"{output['make']}")[-1].split('-')[1]

            year = response.xpath('//li[@class="dtl_annee fl3"]/text()').get()
            if year:
                output['year'] = int(year.strip())

            transmission = response.xpath('//li[@class="dtl_boite_vitesse fl3"]/text()').get()
            if transmission:
                output['transmission'] = transmission.strip()

            fuel = response.xpath('//li[@class="dtl_energie fl3"]/text()').get()
            if fuel:
                output['fuel'] = fuel.strip()
            
            types = response.xpath('//li[@class="dtl_type_auto fl3"]/text()').get()  ## by NT
            if types:  ## by NT
                 output['body_type'] = types.strip()  ## by NT

            color = response.xpath('//li[@class="dtl_couleur fl3"]/text()').get()  ## by NT
            if color:  ## by NT
                output['exterior_color'] = color.strip()  ## by NT

            interior = response.xpath('//li[@class="dtl_interieur fl3"]/text()').get()  ## by NT
            if interior:  ## by NT
                output['upholstery'] = interior.strip()  ## by NT

            intcolor = response.xpath('//li[@class="dtl_couleur_interieur fl3"]/text()').get()  ## by NT
            if intcolor:  ## by NT
                output['interior_color'] = intcolor.strip()  ## by NT

            doors = response.xpath('//li[@class="dtl_nb_portes fl3"]/text()').get()  ## by NT
            if doors:  ## by NT
                dr = doors.strip()  ## by NT
                if dr.split(' ')[0] == '4' or dr.split(' ')[0] == '3' or dr.split(' ')[0] == '2':  ## by NT
                    output['doors'] = int(dr.split(' ')[0])  ## by NT
                elif dr.split(' ')[0] != '4' or dr.split(' ')[0] != '3' or dr.split(' ')[0] != '2':  ## by NT
                    output['doors'] = int(dr.split('-')[0])  ## by NT

            desc1 = response.xpath('//li[@class="dtl_equipement_confort nfl"]/text()').get()  ## by NT
            desc2 = response.xpath('//li[@class="dtl_equipement_securite nfl"]/text()').get()  ## by NT
            desc3 = response.xpath('//li[@class="dtl_equipement_divers nfl"]/text()').get()  ## by NT

            try:  ## by NT
                desc = desc1 + ',' + desc2 + ',' + desc3 ## by NT
                output['vehicle_disclosure']=desc  ## by NT
            except TypeError:  ## by NT
                pass  ## by NT


            odometer_value = response.xpath('//li[@class="dtl_kilometrage fl3"]/text()').get()
            if odometer_value:
                output['odometer_value'] = int(odometer_value.replace('Km', '').strip())
                output['odometer_unit'] = 'Km'

            output['price_retail'] = float(price.replace('GEL', '').replace(' ', ''))
            output['currency'] = 'GEL'
            

            output['vehicle_url'] = response.url
            img = response.xpath('//div[@class="ai sveai"]/ul/li/span/img/@src').getall()
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
            # yield output

