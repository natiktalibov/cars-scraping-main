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
        elif re.search('year|type|sell|quick|sale|suv|for(?!d)', m, re.IGNORECASE) is not None:
            make_model.remove(m)

    if make_model[0] in car_brands:
         output['make'] = make_model[0]
         output['model'] = ' '.join(make_model[1::])
    else:
        output['make'] = ' '.join(make_model[0:2])
        output['model'] = ' '.join(make_model[2::])


class MySpider(scrapy.Spider):
    name = 'bls'

    def start_requests(self):
        urls = [
             'https://bls.ua/en/sale',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        product_links = response.xpath('//div[@class="card_wrapper"]/p/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)


    def detail(self, response):
        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "bls"
        output['country'] = "UA"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = 0
        output['tpms_installed'] = 0
        output['city'] = ''
        output['ac_installed'] = 2

        make_model = response.xpath('//span[@class="current"]/text()').get()
        if make_model is not None:
            make_model = parse_make_model(make_model, output)

        data = response.xpath('//div[@class="car-sale-additional-info"]/div/text()').getall()
        for d in data:
            d = d.split(':')
            label = d[0]
            info = d[1]
            if label == 'Car Year':
                output['year'] = info.strip()
            elif label == 'Mileage':
                output['odometer_value'] = re.sub("[^0-9]", "", info)
                output['odometer_unit'] = 'km'

        cost = response.xpath('//div[@class="car-sale-price-value"]/text()').get()
        if cost is not None:
            cost = cost.strip(' "')
            output['price_wholesale'] = cost
            output['currency'] = 'USD'

        data = response.xpath('//div[@class="card_wrapper"]')
        for d in data:
            label = d.xpath('./p/text()[1]').get().strip()
            if label == 'AT' or label == 'MT':
                output['transmission'] = label
            elif label == 'Engine type':
                info = d.xpath('./p/text()[2]').get()
                output['engine_displacement_value'] = re.sub("[^0-9.,]", "", info)
                output['engine_displacement_unit'] = 'L'
            elif label == 'Air conditioner':
                output['ac_installed'] = 1

        output['picture_list'] = ','.join(response.xpath('//div[@class="pgwSlideshow wide"]/div/ul/li/img/@src').getall())
        apify.pushData(output)
