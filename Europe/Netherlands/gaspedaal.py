import datetime
import scrapy
import apify

class GaspedaalSpider(scrapy.Spider):
    name = 'Gaspedaal'
    download_timeout = 120

    def start_requests(self):
        user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
        urls = [
            'https://www.gaspedaal.nl/zoeken?srt=df-a',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        code = response.xpath('//html/head//script/@src').getall()[-3].split('/')[3]
        api = f'https://www.gaspedaal.nl/_next/data/{code}/zoeken.json?srt=df-a&page=1&slug=zoeken'
        yield response.follow(url=api, callback=self.detail,cb_kwargs={'page': 1,'code':code})

    def detail(self, response, page, code):
        jsn = response.json()
        if jsn['pageProps']['initialState']['searchReducer']['occasions'] != []:
            for des in jsn['pageProps']['initialState']['searchReducer']['occasions']:
                output = {}
                output['ac_installed'] = 0
                output['tpms_installed'] = 0
                output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
                output['scraped_from'] = 'gaspedaal'
                output['scraped_listing_id'] = str(des['id'])
                output['country'] = 'NL'
                output['price_retail'] = float(des['price'].replace('.', '').strip())
                output['price_wholesale'] = output['price_retail']
                output['currency'] = 'EUR'
                output['year'] = int(des['year'])
                output['make'] = des['schemaOrg']['brand']['name']
                output['model'] = des['model']
                output['fuel'] = des['fuel']
                output['city'] = des['place']
                output['odometer_value'] = int(des['km'].replace('.','').strip())
                output['odometer_unit'] = 'km'
                text = des['schemaOrg']['sku']
                output['transmission'] = text.split(',')[3].strip()
                output['engine_displacement_value'] = text.split(',')[1].replace('cc', '').strip()
                output['engine_displacement_units'] = 'cc'
                output['vehicle_url'] = des['schemaOrg']['url']
                output['picture_list'] = des['schemaOrg']['image']['url']
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

            # pagination
            page += 1
            page_link = f'https://www.gaspedaal.nl/_next/data/{code}/zoeken.json?page={page}&srt=df-a&slug=zoeken'
            yield response.follow(url=page_link, callback=self.detail, cb_kwargs={'page': page,'code':code})
