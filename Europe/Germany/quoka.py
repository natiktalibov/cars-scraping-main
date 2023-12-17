import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response
import json

car_brands= ['Abarth', 'AC', 'Acura', 'Adler', 'Alfa Romeo', 'Alpina', 'Alpine', 'AMC', 'AM General', 'Ariel', 'Aro', 'Asia', 'Aston Martin', 'Audi', 'Aurus', 'Austin', 'Austin Healey', 'Autobianchi', 'Avtokam', 'Bajaj', 'Baltijas Dzips', 'Batmobile', 'Bedford', 'Beijing', 'Bentley', 'Bertone', 'Bilenkin', 'Bitter', 'BMW', 'Bolwell', 'Borgward', 'Brabus', 'Brilliance', 'Bristol', 'Bronto', 'Bufori', 'Bugatti', 'Buick', 'BYD', 'Byvin', 'Cadillac', 'Callaway', 'Carbodies', 'Caterham', 'Chana', 'Changan', 'ChangFeng', 'Chery', 'Chevlolet', 'Chevrolet', 'Chrysler', 'CHTC', 'Citroen', 'Cizeta', 'Coggiola', 'Dacia', 'Dadi', 'Daewoo', 'DAF', 'Daihatsu', 'Daimler', 'Datsun', 'Delage', 'DeLorean', 'Derways', 'DeSoto', 'De Tomaso', 'Dodge', 'DongFeng', 'Doninvest', 'Donkervoort', 'DS', 'DW Hower', 'Eagle', 'Eagle Cars', 'E-Car', 'Ecomotors', 'Excalibur', 'FAW', 'Ferrari', 'Fiat', 'Fisker', 'Flanker', 'Ford', 'FORD', 'Foton', 'FSO', 'Fuqi', 'GAC', 'GAZ', 'Geely', 'Genesis', 'Geo', 'GMC', 'Gonow', 'Gordon', 'GP', 'Great Wall', 'Hafei', 'Haima', 'Hanomag', 'Haval', 'Hawtai', 'Hindustan', 'Hispano-Suiza', 'Holden', 'Honda', 'Horch', 'HuangHai', 'Hudson', 'Hummer', 'Hyundai', 'Infiniti', 'Infinity', 'Innocenti', 'Invicta', 'Iran Khodro', 'Isdera', 'Isuzu', 'IVECO', 'Izh', 'JAC', 'Jaguar', 'Jeep', 'Jensen', 'Jinbei', 'JMC', 'Kanonir', 'Kia', 'Koenigsegg', 'Kombat', 'KTM', 'Lada', 'Lamborghini', 'Lancia', 'Land Rover', 'Landwind', 'Lexus', 'Liebao Motor', 'Lifan', 'Ligier', 'Lincoln', 'Lotus', 'LTI', 'LUAZ', 'Lucid', 'Luxgen', 'Mahindra', 'Marcos', 'Marlin', 'Marussia', 'Maruti', 'Maserati', 'Maybach', 'Mazda', 'McLaren', 'Mega', 'Mercedes', 'Mercedes-Benz', 'Mercury', 'Metrocab', 'MG', 'Microcar', 'Minelli', 'Mini', 'MINI', 'Mitsubishi', 'Mitsuoka', 'Morgan', 'Morris', 'Moskvich', 'Nash', 'Nissan', 'Noble', 'Oldsmobile', 'Opel', 'Osca', 'Packard', 'Pagani', 'Panoz', 'Perodua', 'Peugeot', 'PGO', 'Piaggio', 'Plymouth', 'Pontiac', 'Porsche', 'Premier', 'Proton', 'PUCH', 'Puma', 'Qoros', 'Qvale', 'Rambler', 'Range Rover', 'Ravon', 'Reliant', 'Renaissance', 'Renault', 'Renault Samsung', 'Rezvani', 'Rimac', 'Rolls-Royce', 'Ronart', 'Rover', 'Saab', 'Saipa', 'Saleen', 'Santana', 'Saturn', 'Scion', 'SEAT', 'Shanghai Maple', 'ShuangHuan', 'Simca', 'Skoda', 'Smart', 'SMZ', 'Soueast', 'Spectre', 'Spyker', 'SsangYong', 'Steyr', 'Studebaker', 'Subaru', 'Suzuki', 'TagAZ', 'Talbot', 'TATA', 'Tatra', 'Tazzari', 'Tesla', 'Think', 'Tianma', 'Tianye', 'Tofas', 'Toyota', 'Trabant', 'Tramontana', 'Triumph', 'TVR', 'UAZ', 'Ultima', 'Vauxhall', 'VAZ (Lada)c', 'Vector', 'Venturi', 'Volkswagen','Volkswagen' ,'Volvo', 'Vortex', 'Wanderer', 'Wartburg', 'Westfield', 'Wiesmann', 'Willys', 'W Motors', 'Xin Kai', 'Yo-mobile', 'Zastava', 'ZAZ', 'Zenos', 'Zenvo', 'Zibar', 'ZIL', 'ZiS', 'Zotye', 'ZX', 'ВАЗ']
def parse_make_model(input_string, output):
    make_model = re.split(',|_|-|!|\s', input_string)
    for m in list(make_model):
        if re.search('([12][0-9][0-9][0-9])', m) is not None:
            make_model.remove(m)
        elif re.search('year|type|sell|quick|sale|suv|for(?!d) |new|car|first|registration', m, re.IGNORECASE) is not None:
            make_model.remove(m)
        elif re.search('[()]', m, re.IGNORECASE) is not None:
            make_model = make_model[:make_model.index(m)]
            break

    if make_model[0] in car_brands:
         output['make'] = make_model[0].strip(' ,')
         output['model'] = ' '.join(make_model[1::]).strip(' ,')
    else:
        output['make'] = ' '.join(make_model[0:2]).strip(' ,')
        output['model'] = ' '.join(make_model[2::]).strip(' ,')

class MySpider(scrapy.Spider):
    name = 'quoka'
    custom_settings = {'USER_AGENT':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    def start_requests(self):

        url = 'https://www.quoka.de/automarkt/autos-nach-marken/kleinanzeigen.html'
        yield scrapy.Request(url=url, callback=self.parse)
        url = 'https://www.quoka.de/qmca/search/search.html?redirect=0&catid=82_8203&pageno=1'
        yield scrapy.Request(url=url, callback=self.parse2)

    def parse(self, response):

        product_links = [*response.xpath('//li[@class="q-ln hlisting"]/div[2]/a/@href').getall(),*response.xpath('//li[@class="q-ln hlisting highlight"]/div[2]/a/@href').getall()]
        yield from response.follow_all(product_links, self.detail)

        next_link = response.xpath('//li[@class="arr-rgt active"]/a/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)

    def parse2(self, response):

        product_links = [*response.xpath('//li[@class="q-ln hlisting"]/div[2]/a/@href').getall(),*response.xpath('//li[@class="q-ln hlisting highlight"]/div[2]/a/@href').getall()]
        yield from response.follow_all(product_links, self.detail)

        next_link = response.url.rsplit('=',1)
        curr_page = int(next_link[-1])
        next_link = str(next_link[0]+'='+str(curr_page+1))
        if curr_page < 100:
            yield response.follow(next_link, self.parse2)

    def detail(self, response):
        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "quoka"
        output['country'] = "DE"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.xpath('//div[@class="details"]/div[@class="date-and-clicks"]/strong/span/text()').get().strip()
        output['tpms_installed'] = 0
        output['ac_installed'] = 0
        output['city'] = response.xpath('//span[@class="locality"]/text()').get()

        cost = response.xpath('//div[@class="price has-type"]/strong/span/text()').get()
        if cost is None:
            return
        cost = re.sub('[^0-9]','',cost)
        output['price_retail'] = float(cost)
        cost = response.xpath('//div[@class="price has-type"]/strong/text()').get()
        if cost is not None and cost.strip() == "€":
            output['currency'] = "EUR"


        labels = response.xpath('//div[@class="struct-data"]/div/text()').getall()
        info = response.xpath('//div[@class="struct-data"]/strong')
        makemodel = response.xpath('//h1[@itemprop="name"]/text()').get()
        parse_make_model(makemodel, output)

        for i in range(len(labels)):
            label = labels[i]
            if label == None:
                continue
            label = label.strip()
            if label == 'Modell/Typ:':
                make = info[i].xpath('./a/span/span/text()').get()
                if make is not None:
                    output['make'] = make
                model = info[i].xpath('./a/span/text()').get()
                if model is not None:
                    output['model'] = model
            elif label == 'Bj./EZ:':
                output['registration_year'] =  info[i].xpath('./span/text()').get().split('/')[-1]
            elif label == 'Laufleistung:':
                output['odometer_unit'] = 'km'
                output['odometer_value'] = int(re.sub("[^0-9]", "", info[i].xpath('./span/text()').get()))
            elif label == 'Bauart:':  ## by NT
                output['body_type'] = info[i].xpath('./span/text()').get() ## by NT
            elif label == 'Farbe:':  ## by NT
                output['exterior_color'] = info[i].xpath('./span/text()').get() ## by NT
        
        output['vehicle_disclosure'] = response.xpath('//div[@class="text"]/text()').get() ## by NT

        picture = response.xpath('//div[@class="images"]//div[@class="more-pics"]//img/@data-src').getall()
        if picture:
            output['picture_list'] = json.dumps(picture)
        apify.pushData(output)
