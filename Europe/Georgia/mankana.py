import scrapy
import datetime
import math
import apify
from scrapy.http.response import Response


car_brands= ['Abarth', 'AC', 'Acura', 'Adler', 'Alfa Romeo', 'Alpina', 'Alpine', 'AMC', 'AM General', 'Ariel', 'Aro', 'Asia', 'Aston Martin', 'Audi', 'Aurus', 'Austin', 'Austin Healey', 'Autobianchi', 'Avtokam', 'Bajaj', 'Baltijas Dzips', 'Batmobile', 'Bedford', 'Beijing', 'Bentley', 'Bertone', 'Bilenkin', 'Bitter', 'BMW', 'Bolwell', 'Borgward', 'Brabus', 'Brilliance', 'Bristol', 'Bronto', 'Bufori', 'Bugatti', 'Buick', 'BYD', 'Byvin', 'Cadillac', 'Callaway', 'Carbodies', 'Caterham', 'Chana', 'Changan', 'ChangFeng', 'Chery', 'Chevlolet', 'Chevrolet', 'Chrysler', 'CHTC', 'Citroen', 'Cizeta', 'Coggiola', 'Dacia', 'Dadi', 'Daewoo', 'DAF', 'Daihatsu', 'Daimler', 'Datsun', 'Delage', 'DeLorean', 'Derways', 'DeSoto', 'De Tomaso', 'Dodge', 'DongFeng', 'Doninvest', 'Donkervoort', 'DS', 'DW Hower', 'Eagle', 'Eagle Cars', 'E-Car', 'Ecomotors', 'Excalibur', 'FAW', 'Ferrari', 'Fiat', 'Fisker', 'Flanker', 'Ford', 'FORD', 'Foton', 'FSO', 'Fuqi', 'GAC', 'GAZ', 'Geely', 'Genesis', 'Geo', 'GMC', 'Gonow', 'Gordon', 'GP', 'Great Wall', 'Hafei', 'Haima', 'Hanomag', 'Haval', 'Hawtai', 'Hindustan', 'Hispano-Suiza', 'Holden', 'Honda', 'Horch', 'HuangHai', 'Hudson', 'Hummer', 'Hyundai', 'Infiniti', 'Infinity', 'Innocenti', 'Invicta', 'Iran Khodro', 'Isdera', 'Isuzu', 'IVECO', 'Izh', 'JAC', 'Jaguar', 'Jeep', 'Jensen', 'Jinbei', 'JMC', 'Kanonir', 'Kia', 'Koenigsegg', 'Kombat', 'KTM', 'Lada', 'Lamborghini', 'Lancia', 'Land Rover', 'Landwind', 'Lexus', 'Liebao Motor', 'Lifan', 'Ligier', 'Lincoln', 'Lotus', 'LTI', 'LUAZ', 'Lucid', 'Luxgen', 'Mahindra', 'Marcos', 'Marlin', 'Marussia', 'Maruti', 'Maserati', 'Maybach', 'Mazda', 'McLaren', 'Mega', 'Mercedes', 'Mercedes-Benz', 'Mercury', 'Metrocab', 'MG', 'Microcar', 'Minelli', 'Mini', 'MINI', 'Mitsubishi', 'Mitsuoka', 'Morgan', 'Morris', 'Moskvich', 'Nash', 'Nissan', 'Noble', 'Oldsmobile', 'Opel', 'Osca', 'Packard', 'Pagani', 'Panoz', 'Perodua', 'Peugeot', 'PGO', 'Piaggio', 'Plymouth', 'Pontiac', 'Porsche', 'Premier', 'Proton', 'PUCH', 'Puma', 'Qoros', 'Qvale', 'Rambler', 'Range Rover', 'Ravon', 'Reliant', 'Renaissance', 'Renault', 'Renault Samsung', 'Rezvani', 'Rimac', 'Rolls-Royce', 'Ronart', 'Rover', 'Saab', 'Saipa', 'Saleen', 'Santana', 'Saturn', 'Scion', 'SEAT', 'Shanghai Maple', 'ShuangHuan', 'Simca', 'Skoda', 'Smart', 'SMZ', 'Soueast', 'Spectre', 'Spyker', 'SsangYong', 'Steyr', 'Studebaker', 'Subaru', 'Suzuki', 'TagAZ', 'Talbot', 'TATA', 'Tatra', 'Tazzari', 'Tesla', 'Think', 'Tianma', 'Tianye', 'Tofas', 'Toyota', 'Trabant', 'Tramontana', 'Triumph', 'TVR', 'UAZ', 'Ultima', 'Vauxhall', 'VAZ (Lada)', 'Vector', 'Venturi', 'Volkswagen', 'Volvo', 'Vortex', 'Wanderer', 'Wartburg', 'Westfield', 'Wiesmann', 'Willys', 'W Motors', 'Xin Kai', 'Yo-mobile', 'Zastava', 'ZAZ', 'Zenos', 'Zenvo', 'Zibar', 'ZIL', 'ZiS', 'Zotye', 'ZX']

class MySpider(scrapy.Spider):
    name = 'ManakaGeorgia'

    def start_requests(self):
        urls = [
             'https://www.mankana.com/en/vehicle_listings',
             ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        product_links = response.xpath('//a[@class="common-ad-card "]/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        next_link = response.xpath('//a[@class="next_page"][1]/@href').get()
        if next_link is not None:
            yield response.follow(next_link, self.parse)

    def detail(self, response):

        output = {}

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = "Mankana"
        output['country'] = "GE"
        url = response.request.url
        output['vehicle_url'] = url
        url = url.split('-')
        if 'import' in url :
            output['country'] = url[-2]
        else:
            output['city'] = url[-2]
        output['scraped_listing_id'] = url[-1]

        price = response.xpath('//div[@class="ad-info-wrapper position-relative"]/div[@class="ad-price"]/span[@class="price-wrap "]/span/text()').get()
        if price is not None:
            output['price_wholesale'] = price
            output['price_retail'] = float(price.replace(',',''))
            output['currency'] = response.xpath('//div[@class="ad-info-wrapper position-relative"]/div[@class="ad-price"]/span[@class="price-wrap "]/text()').get()

        make_model = response.xpath('//div[@class="ad-title"]/h2/text()').get()
        make_model = make_model.split(' ')
        if make_model[0] in car_brands:
             output['make'] = make_model[0]
             output['model'] = ' '.join(make_model[1::])
        else:
            output['make'] = ' '.join(make_model[0:2])
            output['model'] = ' '.join(make_model[2::])

        #only gets the first image, i cannot figure out how to get the others they dont seem to exist until the page is rendered 
        output['picture_list'] = response.xpath('//meta[@property="og:image"]/@content').get()


        for i in range(10):

            input_type = response.xpath(f'//div[@class="vehicle-properties"]/div[{i}]/div/span[1]/text()').get()
            input_data = response.xpath(f'//div[@class="vehicle-properties"]/div[{i}]/div/span[2]/text()').get()

            if input_type == 'Engine':
                input_data = input_data.split(' ')
                if input_data[0] == 'N/A':
                    output['engine_displacement_value'] = 0
                    output['engine_displacement_unit'] = 0
                else:
                    output['engine_displacement_value'] = input_data[0]
                    output['engine_displacement_unit'] = input_data[1]
            elif input_type == 'Mileage':
                input_data = input_data.split(' ')
                if input_data[0] == 'N/A':
                    output['odometer_value'] = 0
                    output['odometer_unit'] = 0
                else:
                    output['odometer_value'] = int(input_data[0].replace(',',''))
                    output['odometer_unit'] = input_data[1]
            elif input_type == 'Year':
                    if input_data == 'N/A':
                        output['year'] = int(0)
                    else:
                        output['year'] = int(input_data)
            elif input_type == 'Air Con':
                if input_data == 'Yes':
                    output['ac_installed'] = 1
                elif input_data == 'No':
                    output['ac_installed'] = 2
                else :
                    output['ac_installed'] = 0
            elif input_type == 'Gearbox':
                output['transmission'] = input_data
            elif input_type == 'Color':  # by NT
                output['exterior_color'] = input_data  # by NT
            elif input_type == 'Body Type':  # by NT
                output['body_type'] = input_data  # by NT
            elif input_type == 'Fuel Type':  # by NT
                output['fuel_type'] = input_data  # by NT
            elif input_type =='Condition':# by NT
                output['is_used'] = input_data  # by NT
            elif input_type == 'Drive Type':  ## by NT
                output['steering_position'] = input_data # by NT

        output['tpms_installed'] = 0

        apify.pushData(output)