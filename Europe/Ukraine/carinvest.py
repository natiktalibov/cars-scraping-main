import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response

car_brands= ['Abarth', 'AC', 'Acura', 'Adler', 'Alfa Romeo', 'Alpina', 'Alpine', 'AMC', 'AM General', 'Ariel', 'Aro', 'Asia', 'Aston Martin', 'Audi', 'Aurus', 'Austin', 'Austin Healey', 'Autobianchi', 'Avtokam', 'Bajaj', 'Baltijas Dzips', 'Batmobile', 'Bedford', 'Beijing', 'Bentley', 'Bertone', 'Bilenkin', 'Bitter', 'BMW', 'Bolwell', 'Borgward', 'Brabus', 'Brilliance', 'Bristol', 'Bronto', 'Bufori', 'Bugatti', 'Buick', 'BYD', 'Byvin', 'Cadillac', 'Callaway', 'Carbodies', 'Caterham', 'Chana', 'Changan', 'ChangFeng', 'Chery', 'Chevlolet', 'Chevrolet', 'Chrysler', 'CHTC', 'Citroen', 'Cizeta', 'Coggiola', 'Dacia', 'Dadi', 'Daewoo', 'DAF', 'Daihatsu', 'Daimler', 'Datsun', 'Delage', 'DeLorean', 'Derways', 'DeSoto', 'De Tomaso', 'Dodge', 'DongFeng', 'Doninvest', 'Donkervoort', 'DS', 'DW Hower', 'Eagle', 'Eagle Cars', 'E-Car', 'Ecomotors', 'Excalibur', 'FAW', 'Ferrari', 'Fiat', 'Fisker', 'Flanker', 'Ford', 'FORD', 'Foton', 'FSO', 'Fuqi', 'GAC', 'GAZ', 'Geely', 'Genesis', 'Geo', 'GMC', 'Gonow', 'Gordon', 'GP', 'Great Wall', 'Hafei', 'Haima', 'Hanomag', 'Haval', 'Hawtai', 'Hindustan', 'Hispano-Suiza', 'Holden', 'Honda', 'Horch', 'HuangHai', 'Hudson', 'Hummer', 'Hyundai', 'Infiniti', 'Infinity', 'Innocenti', 'Invicta', 'Iran Khodro', 'Isdera', 'Isuzu', 'IVECO', 'Izh', 'JAC', 'Jaguar', 'Jeep', 'Jensen', 'Jinbei', 'JMC', 'Kanonir', 'Kia', 'Koenigsegg', 'Kombat', 'KTM', 'Lada', 'Lamborghini', 'Lancia', 'Land Rover', 'Landwind', 'Lexus', 'Liebao Motor', 'Lifan', 'Ligier', 'Lincoln', 'Lotus', 'LTI', 'LUAZ', 'Lucid', 'Luxgen', 'Mahindra', 'Marcos', 'Marlin', 'Marussia', 'Maruti', 'Maserati', 'Maybach', 'Mazda', 'McLaren', 'Mega', 'Mercedes', 'Mercedes-Benz', 'Mercury', 'Metrocab', 'MG', 'Microcar', 'Minelli', 'Mini', 'MINI', 'Mitsubishi', 'Mitsuoka', 'Morgan', 'Morris', 'Moskvich', 'Nash', 'Nissan', 'Noble', 'Oldsmobile', 'Opel', 'Osca', 'Packard', 'Pagani', 'Panoz', 'Perodua', 'Peugeot', 'PGO', 'Piaggio', 'Plymouth', 'Pontiac', 'Porsche', 'Premier', 'Proton', 'PUCH', 'Puma', 'Qoros', 'Qvale', 'Rambler', 'Range Rover', 'Ravon', 'Reliant', 'Renaissance', 'Renault', 'Renault Samsung', 'Rezvani', 'Rimac', 'Rolls-Royce', 'Ronart', 'Rover', 'Saab', 'Saipa', 'Saleen', 'Santana', 'Saturn', 'Scion', 'SEAT', 'Shanghai Maple', 'ShuangHuan', 'Simca', 'Skoda', 'Smart', 'SMZ', 'Soueast', 'Spectre', 'Spyker', 'SsangYong', 'Steyr', 'Studebaker', 'Subaru', 'Suzuki', 'TagAZ', 'Talbot', 'TATA', 'Tatra', 'Tazzari', 'Tesla', 'Think', 'Tianma', 'Tianye', 'Tofas', 'Toyota', 'Trabant', 'Tramontana', 'Triumph', 'TVR', 'UAZ', 'Ultima', 'Vauxhall', 'VAZ (Lada)c', 'Vector', 'Venturi', 'Volkswagen', 'Volvo', 'Vortex', 'Wanderer', 'Wartburg', 'Westfield', 'Wiesmann', 'Willys', 'W Motors', 'Xin Kai', 'Yo-mobile', 'Zastava', 'ZAZ', 'Zenos', 'Zenvo', 'Zibar', 'ZIL', 'ZiS', 'Zotye', 'ZX', 'ВАЗ']

def parse_make_model(input_string, output):
    make_model = input_string.split(' ')
    for m in list(make_model):
        if re.search('([0-9][0-9][0-9][0-9])', m) is not None:
            output['year'] = m
            make_model.remove(m)
        elif re.search('year|type|model|sell|quick|sale|suv|for(?!d)', m, re.IGNORECASE) is not None:
            make_model.remove(m)
    if make_model[0] in car_brands:
         output['make'] = make_model[0]
         output['model'] = ' '.join(make_model[1::])
    else:
        output['make'] = ' '.join(make_model[0:2])
        output['model'] = ' '.join(make_model[2::])


class MySpider(scrapy.Spider):
    name = 'carinvest'

    def start_requests(self):
        urls = [
             'https://carinvestukraine.com/en/catalog/',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):


        product_info = response.xpath('//article[@class="product"]')
        for product in product_info:
            output = {}
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = "carinvest"
            output['country'] = "UA"
            output['vehicle_url'] = product.xpath('./div[@class="product-button-wrap"]/div/span/a/@href').get().strip()
            output['scraped_listing_id'] = 0
            output['engine_displacement_value'] = 0
            output['engine_displacement_unit'] = 0
            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['city'] = ''

            make_model = product.xpath('./div[@class="product-body"]/div[@class="product-title"]/a/div/text()').get()
            if make_model is not None:
                make_model = parse_make_model(make_model, output)

            cost = product.xpath('./div[@class="product-body"]/div[@class="product-price-wrap"]/div/span/span/bdi/text()').get()
            if cost is not None:
                cost = cost.strip(' "').split(' ')
                output['price_retail'] = float(cost[0].replace(',',''))
                output['currency'] = 'USD'

            data = product.xpath('./div[@class="product-body"]/div[@class="product-price-wrap"]/div[@class="product-info"]/ul/li')
            for d in data:
                label = d.xpath('./span[1]/text()').get()
                info = d.xpath('./span[2]/text()').get()
                if label == 'Year of issue:':
                    output['year'] = int(info.strip())
                elif label == 'Transmission:':
                    output['transmission'] = info.strip()
                elif label == 'Mileage:':
                    output['odometer_value'] = re.sub("[^0-9]", "", info)
                    output['odometer_unit'] = 'km'

            output['picture_list'] = product.xpath('//article[@class="product"]/div[@class="product-body"]/div[@class="product-figure"]/img/@src').get()
            apify.pushData(output)


        next_link = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)
