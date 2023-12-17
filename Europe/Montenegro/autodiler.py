import datetime
import scrapy
import apify



class AutoSpider(scrapy.Spider):
    name = "learnscrapy"
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    start_urls = ['https://autodiler.me/automobili/pretraga?pageNumber=1&formStyle=basic&sortBy=dateDesc']

    max_req = {}  # Storage required retry link
    max_retry = 5  # retry count


    def parse(self, response):
        href_list = response.xpath('//div[@class="oglasi-item-tekst oglasi-item-tekst-automobili"]/a/@href').getall()
        for href in href_list:
            url = 'https://autodiler.me' + href
            yield scrapy.Request(url,
                    meta={'vehicle_url': url,
                          },
                          callback=self.get_data
                        )

        # next page
        next_href = response.xpath('//a[@class="ads-pagination__item-link"]/@href').getall()
        if len(href_list) > 0:
            yield response.follow('https://autodiler.me' + next_href[-1], self.parse)

    async def get_data(self, response):

        output = {}

        # default
        output['vehicle_url'] = str(response.meta['vehicle_url'])
        output['scraped_listing_id'] = response.meta['vehicle_url'].split('/')[-1]
        output['country'] = 'ME'
        output['scraped_from'] = 'Autodiler'
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())

        # get title
        try:
            output['make'] = response.xpath('//h1[@class="oglasi-headline-model"]/text()').get().strip().split(' ')[0]
        except AttributeError:
            pass

        output['model'] = response.xpath('//h1[@class="oglasi-headline-model"]/text()').get().strip().split('-')[1].strip()

        # Location details
        output['city'] = response.xpath('//div[@class="oglasi-dodatne-informacije"]//li//text()')[3].getall()
        try:
           output['state_or_province'] = response.xpath('//div[@class="oglasi-dodatne-informacije"]//li//text()')[5].getall()
        except IndexError:
            pass

        # price
        try:
            output['price_retail'] = float(response.xpath('//div[@class="cena"]//text()').get())
            output['currency'] = 'EUR'
        except TypeError:
            pass

        try:
            output['vehicle_disclosure'] = response.xpath('normalize-space(//p[@class="oglasi-opis-text"]//text())').get().strip()
        except AttributeError:
            pass

        ## extra details
        des_list = response.xpath('//div[@class="oglasi-osnovne-informacije"]//text()').getall()
        items_list = [value for value in des_list if value not in (":", "kW", "cm3", "KS")]
        res_dct = dict(map(lambda i: (items_list[i], items_list[i + 1]), range(len(items_list) - 1)[::2]))

        try:
            output["odometer_value"] = int(res_dct['Kilometraža'])
            output["odometer_unit"] = "KM"
        except (KeyError,ValueError):
            pass

        try:
            output["fuel"] = res_dct['Gorivo']
        except KeyError:
            pass

        try:
            output["engine_displacement_value"] = res_dct['Kubikaža']
            output["engine_displacement_units"] = 'cm3'
        except KeyError:
            pass

        try:
             output["transmission"] = res_dct['Mjenjač']
        except KeyError:
            pass

        try:
            output["registration_year"] = res_dct['Registrovan do']
        except KeyError:
            pass

        try:
            output["year"] = int(res_dct['Godište'])
        except KeyError:
            pass

        try:
            output["body_type"] = res_dct['Karoserija']
        except KeyError:
            pass

        try:
            output["steering_position"] = res_dct['Strana volana']
        except KeyError:
            pass

        try:
            output["exterior_color"] = res_dct['Boja spoljašnosti']
        except KeyError:
            pass

        ##picture
        pice=str(response.xpath('//div[@class="oglas-item-slika detail"]/@style').get())
        output['picture_list']=pice.replace('background-image:url(','').replace(')','')

        apify.pushData(output)
        #print(output)