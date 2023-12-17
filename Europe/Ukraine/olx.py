import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response

car_brands= ['Abarth', 'AC', 'Acura', 'Adler', 'Alfa Romeo', 'Alpina', 'Alpine', 'AMC', 'AM General', 'Ariel', 'Aro', 'Asia', 'Aston Martin', 'Audi', 'Aurus', 'Austin', 'Austin Healey', 'Autobianchi', 'Avtokam', 'Bajaj', 'Baltijas Dzips', 'Batmobile', 'Bedford', 'Beijing', 'Bentley', 'Bertone', 'Bilenkin', 'Bitter', 'BMW', 'Bolwell', 'Borgward', 'Brabus', 'Brilliance', 'Bristol', 'Bronto', 'Bufori', 'Bugatti', 'Buick', 'BYD', 'Byvin', 'Cadillac', 'Callaway', 'Carbodies', 'Caterham', 'Chana', 'Changan', 'ChangFeng', 'Chery', 'Chevlolet', 'Chevrolet', 'Chrysler', 'CHTC', 'Citroen', 'Cizeta', 'Coggiola', 'Dacia', 'Dadi', 'Daewoo', 'DAF', 'Daihatsu', 'Daimler', 'Datsun', 'Delage', 'DeLorean', 'Derways', 'DeSoto', 'De Tomaso', 'Dodge', 'DongFeng', 'Doninvest', 'Donkervoort', 'DS', 'DW Hower', 'Eagle', 'Eagle Cars', 'E-Car', 'Ecomotors', 'Excalibur', 'FAW', 'Ferrari', 'Fiat', 'Fisker', 'Flanker', 'Ford', 'FORD', 'Foton', 'FSO', 'Fuqi', 'GAC', 'GAZ', 'Geely', 'Genesis', 'Geo', 'GMC', 'Gonow', 'Gordon', 'GP', 'Great Wall', 'Hafei', 'Haima', 'Hanomag', 'Haval', 'Hawtai', 'Hindustan', 'Hispano-Suiza', 'Holden', 'Honda', 'Horch', 'HuangHai', 'Hudson', 'Hummer', 'Hyundai', 'Infiniti', 'Infinity', 'Innocenti', 'Invicta', 'Iran Khodro', 'Isdera', 'Isuzu', 'IVECO', 'Izh', 'JAC', 'Jaguar', 'Jeep', 'Jensen', 'Jinbei', 'JMC', 'Kanonir', 'Kia', 'Koenigsegg', 'Kombat', 'KTM', 'Lada', 'Lamborghini', 'Lancia', 'Land Rover', 'Landwind', 'Lexus', 'Liebao Motor', 'Lifan', 'Ligier', 'Lincoln', 'Lotus', 'LTI', 'LUAZ', 'Lucid', 'Luxgen', 'Mahindra', 'Marcos', 'Marlin', 'Marussia', 'Maruti', 'Maserati', 'Maybach', 'Mazda', 'McLaren', 'Mega', 'Mercedes', 'Mercedes-Benz', 'Mercury', 'Metrocab', 'MG', 'Microcar', 'Minelli', 'Mini', 'MINI', 'Mitsubishi', 'Mitsuoka', 'Morgan', 'Morris', 'Moskvich', 'Nash', 'Nissan', 'Noble', 'Oldsmobile', 'Opel', 'Osca', 'Packard', 'Pagani', 'Panoz', 'Perodua', 'Peugeot', 'PGO', 'Piaggio', 'Plymouth', 'Pontiac', 'Porsche', 'Premier', 'Proton', 'PUCH', 'Puma', 'Qoros', 'Qvale', 'Rambler', 'Range Rover', 'Ravon', 'Reliant', 'Renaissance', 'Renault', 'Renault Samsung', 'Rezvani', 'Rimac', 'Rolls-Royce', 'Ronart', 'Rover', 'Saab', 'Saipa', 'Saleen', 'Santana', 'Saturn', 'Scion', 'SEAT', 'Shanghai Maple', 'ShuangHuan', 'Simca', 'Skoda', 'Smart', 'SMZ', 'Soueast', 'Spectre', 'Spyker', 'SsangYong', 'Steyr', 'Studebaker', 'Subaru', 'Suzuki', 'TagAZ', 'Talbot', 'TATA', 'Tatra', 'Tazzari', 'Tesla', 'Think', 'Tianma', 'Tianye', 'Tofas', 'Toyota', 'Trabant', 'Tramontana', 'Triumph', 'TVR', 'UAZ', 'Ultima', 'Vauxhall', 'VAZ (Lada)c', 'Vector', 'Venturi', 'Volkswagen', 'Volvo', 'Vortex', 'Wanderer', 'Wartburg', 'Westfield', 'Wiesmann', 'Willys', 'W Motors', 'Xin Kai', 'Yo-mobile', 'Zastava', 'ZAZ', 'Zenos', 'Zenvo', 'Zibar', 'ZIL', 'ZiS', 'Zotye', 'ZX', 'ВАЗ']

def parse_make_model(input_string, output):
    make_model = input_string.split(' ')
    for m in list(make_model):
        if re.search('([12][0-9][0-9][0-9])', m) is not None:
            output['year'] = m
            make_model.remove(m)
        elif re.search('year|type|sell|quick|sale|suv|for(?!d) |new|car', m, re.IGNORECASE) is not None:
            make_model.remove(m)
        elif re.search('[()]', m, re.IGNORECASE) is not None:
            make_model = make_model[:make_model.index(m)]
            break

    if make_model[0] in car_brands:
         output['make'] = make_model[0]
         output['model'] = ' '.join(make_model[1::])
    else:
        output['make'] = ' '.join(make_model[0:2])
        output['model'] = ' '.join(make_model[2::])


class MySpider(scrapy.Spider):
    name = 'olx'
    custom_settings = {'USER_AGENT':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    def start_requests(self):
        urls = [
             'https://www.olx.ua/d/uk/transport/legkovye-avtomobili/q-car/',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        product_links = response.xpath('//div[@data-cy="l-card"]/a/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        next_link = response.xpath('//a[@data-testid="pagination-forward"]/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)


    def detail(self, response):

        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "olx"
        output['country'] = "UA"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('-')[-1].split('.')[0]
        output['tpms_installed'] = 0
        output['ac_installed'] = 2

        make_model = response.xpath('//h1[@data-cy="ad_title"]/text()').get()
        if make_model is not None:
            make_model = parse_make_model(make_model, output)

        data = response.xpath('//li[@class="css-ox1ptj"]/descendant::text()').getall()
        for d in data:
            d = d.split(':')
            if len(d) < 2:
                continue
            label = d[0]
            info = d[1]
            if label == 'Пробіг':
                output['odometer_value'] = int(re.sub("[^0-9]", "", info))
                output['odometer_unit'] = 'km'
            elif label == "Об'єм двигуна (л.)":
                output['engine_displacement_value'] = float(re.sub("[^0-9.]", "", info))
                output['engine_displacement_unit'] = 'L'
            elif label == 'Комфорт' and info.find("Кондиціонер") != -1:
                output['ac_installed'] = 1
            elif label == 'Коробка передач':
                output['transmission'] = info

        cost = response.xpath('//h3[@class="css-okktvh-Text eu5v0x0"]/text()').getall()
        if cost is not None:
            cost[0] = re.sub("[^0-9]", "", cost[0])
            output['price_retail'] = float(cost[0])
            output['currency'] = cost[-1]
            if cost[-1] == '$':
                output['currency'] = 'USD'

        output['city'] = ''

        output['picture_list'] = ','.join([*response.xpath('//div[@class="swiper-zoom-container"]/img/@data-src').getall(), *response.xpath('//div[@class="swiper-zoom-container"]/img/@src').getall()])
        apify.pushData(output)
