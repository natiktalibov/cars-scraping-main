import json
import scrapy
import datetime

import apify


class AvtozarSpider(scrapy.Spider):
    name = 'avtozar'
    start_urls = ['http://www.avtozar.mn/car?made=&made2=&mark=&torol=&torol2=&erembe=0&userid=&q=&uon=&P=0']

    def parse(self, response):
        link_list = response.xpath("//div[@id='list_item_bg']/a/@href").getall()  # detail link list
        yield from response.follow_all(link_list, self.product_detail)

        # If the "next page" button does not exist, there is no need to continue
        next_button_status = response.xpath("//a[contains(text(), 'Дараах »')]")
        if next_button_status:
            current_page = int(str(response.url).split("q=&uon=&P=")[1])
            next_page = response.url.split('q=&uon=&P=')[0] + "q=&uon=&P=" + str(current_page + 15)
            yield response.follow(next_page, self.parse)

    def product_detail(self, response):
        output = {}

        form_data = response.xpath('//table[@id="carviewtd"]//tr')
        form_data = [i for i in form_data if i.xpath('./td')]
        for data in form_data:
            key = data.xpath('./td[@class="carviewtddetail1"]/text()').get()
            value = data.xpath('./td[@class="carviewtddetail2"]//text()').get()
            if "Үйлдвэрлэгч" in key:
                output["make"] = value
            elif "Марк" in key:
                output["model"] = value
            elif "Үйлдвэрлэсэн он" in key:
                output["year"] = int(value)
            elif "Хурдны хайрцаг, кроп" in key:
                output["transmission"] = value
            elif "Моторын багтаамж" in key:
                engine = value.split(" ")[0].replace(",", ".")
                output["engine_displacement_value"] = "".join(
                    [i for i in list(engine) if i.isdigit() or i == '.' or i == '-'])
                output["engine_displacement_units"] = "".join(
                    [i for i in list(engine) if not i.isdigit() and i != '.' and i != '-'])
            elif "Түлш" in key:
                output["fuel"] = value
            elif "Явсан км" in key:
                odometer_value = "".join([i for i in list(value) if i.isdigit()])
                if odometer_value:
                    output["odometer_value"] = int(odometer_value)
                    output["odometer_unit"] = key.split(" ")[1]
            elif "Байршил" in key:
                output["city"] = value

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = 'Avtozar'
        output["scraped_listing_id"] = response.url.split("/")[-1]
        output["vehicle_url"] = response.url
        output["country"] = "MN"

        price_data = response.xpath("//div[@id='view_detail_title_une']/text()").get()
        # Parse the number from the price string
        price = "".join([i for i in list(price_data) if i.isdigit() or i == '.'])
        if "сая" in price_data:
            output["price_retail"] = float(price) * 1000000
        if "₮" in price_data:
            output["currency"] = "MNT"

        picture_list = response.xpath('//div[@class="fotorama"]//img/@src').getall()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        list1 = []
        list2 = []
        for k, v in output.items():
            if v or v == 0:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))

        # yield output
        apify.pushData(output)