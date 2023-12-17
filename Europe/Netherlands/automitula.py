import re
import json
import apify
import scrapy
import requests
import datetime


class MitulaSpider(scrapy.Spider):
    name = 'mitula'
    start_urls = ['https://auto.mitula.nl/searchC/sorteren-0/merk-Toyota/q-toyota/pag-1?req_sgmt=REVTS1RPUDtVU0VSX1NFQVJDSDtTRVJQOw==']

    def start_requests(self):  # Post request
        make_list = self.get_make()   # Get all brands
        for make in make_list:
            url = f"https://auto.mitula.nl/searchC/sorteren-0/merk-{make}/q-{make}/pag-1?req_sgmt=REVTS1RPUDtVU0VSX1NFQVJDSDtTRVJQOw=="
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        output = {}

        # All models under the vehicle brand, Used to analyze the vehicle model from the title
        car_model_list = response.xpath("//div[@id='js-sort-model']/div/span[2]/text()").getall()
        car_model_list = [i.strip() for i in car_model_list if i.strip() != ""]

        car_list = response.xpath('//*[contains(@id, "sponsoredContainer")] | //*[contains(@id, "normalContainer")]')
        for car in car_list:
            output["make"] = car.xpath(".//span[@class='car-snippet-title car-snippet-title']/strong/text()").get()
            year = car.xpath(".//div[@class='characteristic characteristic__year']/span[@class='text']/text()").get()
            if year:
                output["year"] = int(year)
            mileage = car.xpath(".//div[@class='characteristic characteristic__mileage']/span[@class='text']/text()").get()
            if mileage:
                output["odometer_value"] = int(mileage.split(" ")[0].replace(".", ""))
                output["odometer_unit"] = mileage.split(" ")[1]
            output["fuel"] = car.xpath(".//div[@class='characteristic characteristic__engine']/span[@class='text']/text()").get()
            output["vehicle_url"] = car.xpath("./@data-href").get()
            if "http" not in output.get("vehicle_url"):
                continue
            output["scraped_listing_id"] = car.xpath("./@data-idanuncio").get()
            car_price = car.xpath("./@data-price").get()
            if car_price and str(car_price) != "0":
                output["price_retail"] = float(car_price)
                output["currency"] = car.xpath("./@data-currency").get()
            output["city"] = car.xpath("./@data-location").get()
            output["country"] = "NL"

            title = car.xpath(".//span[@class='car-snippet-title car-snippet-title']//text()").getall()
            title = "".join([i.strip() for i in title if i.strip() != ""])
            output["model"] = "".join([i for i in car_model_list if i in title])

            engine = [i for i in title.split(" ") if "." in i and i.replace(".", "").isdigit() and len(i)==3]
            if not output.get("model") and engine:
                output["model"] = title.split(engine[0])[0].replace(output.get("make"), "").strip()
            if engine:
                output["engine_displacement_value"] = int(float(engine[0]) * 1000)
                output["engine_displacement_units"] = 'cc'

            output["ac_installed"] = 0
            output["tpms_installed"] = 0
            output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
            output["scraped_from"] = "Mitula"

            picture = car.xpath(".//div[@class='car-snippet__image']/img/@data-lazy-sma").get()
            if picture:
                output["picture_list"] = json.dumps([picture.replace("small", "medium")])

            # process empty fields
            list1 = []
            list2 = []
            for k, v in output.items():
                if v or v == 0:
                    list1.append(k)
                    list2.append(v)
            output = dict(zip(list1, list2))
            yield apify.pushData(output)

            # yield output

        next_button = response.xpath("//li[@value='Next']")
        if next_button:
            current_page = re.findall("pag-(.*?)\?", response.url, re.S)[0]
            next_url = response.url.replace(f"pag-{str(current_page)}", f"pag-{str(int(current_page)+1)}")
            yield response.follow(next_url, self.parse)

    def get_make(self):
        url = 'https://auto.mitula.nl/servletAuxData?idOperation=1&idPais=10&campoBusqueda=marcas'
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
        response = requests.get(url=url, headers=headers).json()

        make_list = [i[0] for i in response["marcas"]]

        return make_list