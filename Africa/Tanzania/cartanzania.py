import scrapy
import json
import datetime
import apify

class CartanzaniaSpider(scrapy.Spider):
    name = 'cartanzania'

    def start_requests(self):
        urls = [
            'https://www.cartanzania.com/en/vehicle_listings?&button=&listing%5Bbrand_id%5D=&listing%5Bcar_type%5D=1&listing%5Bcity_id%5D%5B%5D=&listing%5Bcondition%5D=2&listing%5Bmaxprice%5D=&listing%5Bminprice%5D=&page=1&utf8=%E2%9C%93',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//*[@id="ads-list"]/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)
        # pagination
        page_link = response.xpath('//a[@class="next_page"]/@href').get()
        if page_link is not None:
            yield response.follow(url=page_link, callback=self.parse )

    def detail(self,response):
        output = {}

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Car Tanzania'
        output['scraped_listing_id'] = response.url.split('-')[-1]
        output['country'] = 'TZ'
        price = response.xpath('//div[@class="back-wrapper"]//span[@class="price"]/text()').get()
        if price:
            output['price_retail'] = float(price.replace(',', '').strip())
            output['price_wholesale'] = output['price_retail']
            output['currency'] = 'TZS'

            # description head
            output['make'] = response.url.split('/')[-1].split('-')[1]
            output['model'] = response.url.split('/')[-1].split('-')[2]

            # description head
            des_key = response.xpath('//div[@class="prop"]/div/span[1]/text()').getall()
            des_value = response.xpath('//div[@class="prop"]/div/span[2]/text()').getall()
            for i in range(len(des_key)):
                if des_key[i].strip() == 'Engine' and des_value[i].strip() != 'N/A':
                    output['engine_displacement_value'] = des_value[i].replace('L', '').strip()
                    output['engine_displacement_units'] = 'L'

                elif des_key[i].strip() == 'Gearbox' and des_value[i].strip() != 'N/A':
                    output['transmission'] = des_value[i].strip()

                elif des_key[i].strip() == 'Mileage' and des_value[i].strip() != 'N/A':
                    output['odometer_value'] = int(des_value[i].strip().replace('km', '').replace(',', '').strip())
                    output['odometer_unit'] = 'km'

                elif des_key[i].strip() == 'Year' and des_value[i].strip() != 'N/A':
                    output['year'] = int(des_value[i].strip())

                elif des_key[i].strip() == 'Fuel Type' and des_value[i].strip() != 'N/A':
                    output['fuel'] = des_value[i].strip()

            output['city'] = response.xpath('/html/body/main/div[1]/div[1]/div/div/div[2]/div/a/span/text()').get().strip()
            output['vehicle_url'] = response.url
            output['picture_list'] = json.dumps(response.xpath('//div[@class="slider-for"]/div/img/@src').getall())

            # process empty fields
            list1 = []
            list2 = []
            for k, v in output.items():
                if v or v == 0:
                    list1.append(k)
                    list2.append(v)
            output = dict(zip(list1, list2))
            apify.pushData(output)