import scrapy
import datetime
import re
import apify
from scrapy.http.response import Response

car_brands= ['Abarth', 'AC', 'Acura', 'Adler', 'Alfa Romeo', 'Alpina', 'Alpine', 'AMC', 'AM General', 'Ariel', 'Aro', 'Asia', 'Aston Martin', 'Audi', 'Aurus', 'Austin', 'Austin Healey', 'Autobianchi', 'Avtokam', 'Bajaj', 'Baltijas Dzips', 'Batmobile', 'Bedford', 'Beijing', 'Bentley', 'Bertone', 'Bilenkin', 'Bitter', 'BMW', 'Bolwell', 'Borgward', 'Brabus', 'Brilliance', 'Bristol', 'Bronto', 'Bufori', 'Bugatti', 'Buick', 'BYD', 'Byvin', 'Cadillac', 'Callaway', 'Carbodies', 'Caterham', 'Chana', 'Changan', 'ChangFeng', 'Chery', 'Chevlolet', 'Chevrolet', 'Chrysler', 'CHTC', 'Citroen', 'Cizeta', 'Coggiola', 'Dacia', 'Dadi', 'Daewoo', 'DAF', 'Daihatsu', 'Daimler', 'Datsun', 'Delage', 'DeLorean', 'Derways', 'DeSoto', 'De Tomaso', 'Dodge', 'DongFeng', 'Doninvest', 'Donkervoort', 'DS', 'DW Hower', 'Eagle', 'Eagle Cars', 'E-Car', 'Ecomotors', 'Excalibur', 'FAW', 'Ferrari', 'Fiat', 'Fisker', 'Flanker', 'Ford', 'FORD', 'Foton', 'FSO', 'Fuqi', 'GAC', 'GAZ', 'Geely', 'Genesis', 'Geo', 'GMC', 'Gonow', 'Gordon', 'GP', 'Great Wall', 'Hafei', 'Haima', 'Hanomag', 'Haval', 'Hawtai', 'Hindustan', 'Hispano-Suiza', 'Holden', 'Honda', 'Horch', 'HuangHai', 'Hudson', 'Hummer', 'Hyundai', 'Infiniti', 'Infinity', 'Innocenti', 'Invicta', 'Iran Khodro', 'Isdera', 'Isuzu', 'IVECO', 'Izh', 'JAC', 'Jaguar', 'Jeep', 'Jensen', 'Jinbei', 'JMC', 'Kanonir', 'Kia', 'Koenigsegg', 'Kombat', 'KTM', 'Lada', 'Lamborghini', 'Lancia', 'Land Rover', 'Landwind', 'Lexus', 'Liebao Motor', 'Lifan', 'Ligier', 'Lincoln', 'Lotus', 'LTI', 'LUAZ', 'Lucid', 'Luxgen', 'Mahindra', 'Marcos', 'Marlin', 'Marussia', 'Maruti', 'Maserati', 'Maybach', 'Mazda', 'McLaren', 'Mega', 'Mercedes', 'Mercedes-Benz', 'Mercury', 'Metrocab', 'MG', 'Microcar', 'Minelli', 'Mini', 'MINI', 'Mitsubishi', 'Mitsuoka', 'Morgan', 'Morris', 'Moskvich', 'Nash', 'Nissan', 'Noble', 'Oldsmobile', 'Opel', 'Osca', 'Packard', 'Pagani', 'Panoz', 'Perodua', 'Peugeot', 'PGO', 'Piaggio', 'Plymouth', 'Pontiac', 'Porsche', 'Premier', 'Proton', 'PUCH', 'Puma', 'Qoros', 'Qvale', 'Rambler', 'Range Rover', 'Ravon', 'Reliant', 'Renaissance', 'Renault', 'Renault Samsung', 'Rezvani', 'Rimac', 'Rolls-Royce', 'Ronart', 'Rover', 'Saab', 'Saipa', 'Saleen', 'Santana', 'Saturn', 'Scion', 'SEAT', 'Shanghai Maple', 'ShuangHuan', 'Simca', 'Skoda', 'Smart', 'SMZ', 'Soueast', 'Spectre', 'Spyker', 'SsangYong', 'Steyr', 'Studebaker', 'Subaru', 'Suzuki', 'TagAZ', 'Talbot', 'TATA', 'Tatra', 'Tazzari', 'Tesla', 'Think', 'Tianma', 'Tianye', 'Tofas', 'Toyota', 'Trabant', 'Tramontana', 'Triumph', 'TVR', 'UAZ', 'Ultima', 'Vauxhall', 'VAZ (Lada)c', 'Vector', 'Venturi', 'Volkswagen', 'Volvo', 'Vortex', 'Wanderer', 'Wartburg', 'Westfield', 'Wiesmann', 'Willys', 'W Motors', 'Xin Kai', 'Yo-mobile', 'Zastava', 'ZAZ', 'Zenos', 'Zenvo', 'Zibar', 'ZIL', 'ZiS', 'Zotye', 'ZX', 'ВАЗ']

class MySpider(scrapy.Spider):
    name = 'autosvit'

    def start_requests(self):
        urls = [
             'https://autosvit.com.ua/auto/1.html',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):


        product_links = list(set(response.xpath('//div[@id="avto"]/form/div/a/@href').getall()))
        yield from response.follow_all(product_links, self.detail)
        if len(product_links) > 9:
            current_page = int(response.url.split('/')[-1].split('.')[0])
            next_link = 'https://autosvit.com.ua/auto/' + str(current_page+1)+'.html'
            yield response.follow(next_link, self.parse)


    def detail(self, response):

        output = {}
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "autosvit"
        output['country'] = "Ukraine"
        output['vehicle_url'] = response.url
        output['scraped_listing_id'] = response.url.split('_')[-1].split('.')[0]

        make_model = response.xpath('//div[@id="obyav_w"]/table/tr/td/font/text()').get().split(',')[0].split(' ',1)
        if make_model[0] in car_brands:
             output['make'] = make_model[0]
             output['model'] = ' '.join(make_model[1::])
        else:
            output['make'] = ' '.join(make_model[0:2])
            output['model'] = ' '.join(make_model[2::])

        cost = response.xpath('//div[@id="obyav_w"]/table/tr[2]/td[2]/b/text()').get()
        if cost is not None:
            cost = cost.split(' ')
            output['price_wholesale'] = cost[0]
            output['price_retail'] = float(cost[0])
            output['currency'] = cost[-1]

        data = response.xpath('//div[@id="obyav_w"]/table/tr')
        for d in data:
            label = d.xpath('./td[1]/font/text()').get()
            info = d.xpath('./td[2]/text()').get()
            if label == 'Год выпуска:\xa0':
                output['year'] = int(info.strip())
            elif label == 'Пробег, км.:\xa0':
                output['odometer_value'] = re.sub("[^0-9]", "", info)
                output['odometer_unit'] = 'km'
            elif label == 'КП:\xa0':
                output['transmission'] = info
            elif label == 'Город:\xa0':
                output['city'] = info

        output['engine_displacement_value'] = 0
        output['engine_displacement_unit'] = 0
        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        image_list = response.xpath('//table/tr/td/a/img/@src').getall()
        if 'img/smail.jpg' in image_list:
            image_list.remove('img/smail.jpg')
        image_list = [f'https://autosvit.com.ua/{image}' for image in image_list]
        output['picture_list'] = ','.join(image_list)
        apify.pushData(output)
