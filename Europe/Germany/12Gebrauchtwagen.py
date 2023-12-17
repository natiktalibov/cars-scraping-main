import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response

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
    name = '12Gebrauchtwagen'
    custom_settings = {'USER_AGENT':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    def start_requests(self):

        url = 'https://www.12gebrauchtwagen.de/suchen?page=1'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        product_links = [*response.xpath('//div[@class="diversity-results"]/div[@class="columns"]'),*response.xpath('//div[@class="regular-results"]/div[@class="columns"]')]
        for p in product_links:
            self.detail(p)

        next_link = response.xpath('//div[@class="next"]/span/a/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)

    def detail(self, data):
        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "12Gebrauchtwagen"
        output['country'] = "DE"
        output['vehicle_url'] = output['vehicle_url'] = 'https://www.12gebrauchtwagen.de'+data.xpath('./div/a/@href').get()
        output['scraped_listing_id'] = 0
        output['tpms_installed'] = 0
        output['ac_installed'] = 0
        output['engine_displacement_value'] = 0
        output['engine_displacement_unit'] = 0
        output['vin'] = 0
        output['transmission'] = 0
        output['year'] =  data.xpath('./descendant-or-self::*/span[@class="ad-registration-date"]/text()').get().split('/')[-1]
        city = data.xpath('./descendant-or-self::*/div[@class="ad-extra-info"]/text()').get()
        if city is not None:
            output['city'] = re.sub("[0-9]", "", city)

        cost = data.xpath('./descendant-or-self::*/span[@class="ad-price"]/text()').get()
        if cost is not None:
            if cost.split(' ')[-1] == "€":
                output['currency'] = "EUR"
            cost = re.sub('[^0-9]','',cost)
            output['price_retail'] = float(cost)

        makemodel = data.xpath('./descendant-or-self::*/h3[@class="ad-display-name truncate"]/text()').get()
        parse_make_model(makemodel, output)

        mileage = data.xpath('./descendant-or-self::*/span[@class="ad-mileage"]/text()').get()
        if mileage is not None:
            output['odometer_unit'] = 'km'
            output['odometer_value'] = int(re.sub("[^0-9]", "", mileage))


        output['picture_list'] = data.xpath('./descendant-or-self::*/img/@src').get()
        apify.pushData(output)
