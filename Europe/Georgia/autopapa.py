import datetime
import json
import scrapy

import apify

class ApSpider(scrapy.Spider):
    name = 'ap'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://ap.ge/ge/search?&s%5Bcountry_id%5D=0&s%5Bengine_type%5D%5B%5D=0&s%5Bgearbox%5D%5B%5D=0&s%5Blegal_status%5D%5B%5D=&order=date&page=1&short_form=0&utf8=%E2%9C%93',
        ]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'referer': 'https://ap.ge/ge',
        }
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=headers)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//a[@class="with_hash1"]/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        page_link = response.xpath('//a[@rel="next"]/@href').get()
        if page_link is not None:
                yield response.follow(page_link, self.parse)

    def detail(self, response):
        output = {}
        price = response.xpath('//span[@class="priceObject"]/text()').get().replace(' ', '')
        if price:
            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = 'Autopapa'
            output['scraped_listing_id'] = response.url.split('?')[0].split('/')[-1]
            output['country'] = 'Georgia'
            output['make'] = response.url.split('/')[-3]
            output['model'] = response.url.split('/')[-2]
            output['city'] = response.xpath('//div[@class="contactObjectNew"]/div/text()').get().split(',')[0].strip()

            des = response.xpath('//div[@class="InfoObject"]/div/div/strong/text()').getall()
            for i in range(len(des)):
                des_key = response.xpath(f'//div[@class="InfoObject"]/div[{i+1}]/div/strong/text()').get()
                des_value = response.xpath(f'//div[@class="InfoObject"]/div[{i+1}]/div/text()').get()
                if 'გამოშვების წელი' in des_key:
                    output['year'] = int(des_value.strip())
                elif 'ძრავის მოცულობა' in des_key:
                    output['engine_displacement_value'] = str(des_value.replace('ლ', '').strip())
                    output['engne_displacement_units'] = 'ლ'
                elif 'ძრავის ტიპი' in des_key:
                    output['fuel'] = des_value.strip()
                elif 'ძარის ტიპი' in des_key:  # by NT
                    output['body_type'] = des_value.strip()  # by NT

            des = response.xpath('//div[@class="InfoObject InfoObjectRight"]/div/div/strong/text()').getall()
            for i in range(len(des)):
                des_key = response.xpath(f'//div[@class="InfoObject InfoObjectRight"]/div[{i + 1}]/div/strong/text()').get()
                des_value = response.xpath(f'//div[@class="InfoObject InfoObjectRight"]/div[{i + 1}]/div/text()').get()
                if 'გარბენი' in des_key:
                    output['odometer_value'] = int(des_value.replace('კმ.', '').replace(' ', '').strip())
                    output['odometer_unit'] = 'კმ'
                elif 'კარები' in des_key:  # by NT
                    door = des_value.strip()  # by NT
                    output['doors'] = int(door.split('/')[0])  # by NT
                elif 'ადგილების რაოდენობა' in des_key:  # by NT
                    seat = des_value.strip()  # by NT
                    output['seats'] = int(seat.split('-')[0])  # by NT
                elif 'ძარის ფერი' in des_key: # by NT
                    output['exterior_color'] = des_value.strip()  # by NT

            output['price_retail'] = float(price.strip())
            output['price_wholesale'] = output['price_retail']
            output['currency'] = 'GEL'
            output['vehicle_disclosure'] = response.xpath('normalize-space(//div[@class="comment-all"]/text())').getall() # by NT

            output['vehicle_url'] = response.url
            img = response.xpath('//div[@class="boxImgGallery image"]/a/@href').getall()
            for i in range(len(img)):
                img[i] = 'https://ap.ge' + img[i]
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
