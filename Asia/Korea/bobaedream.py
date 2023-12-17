import re
import json
import scrapy
import datetime
from scrapy import Selector
import apify


class BobaeDreamSpider(scrapy.Spider):
    name = 'BobaeDream'

    def start_requests(self):
        url = 'https://www.bobaedream.co.kr/cyber/CyberCar.php?sel_m_gubun=ALL&page=1&order=S11&view_size=70'
        yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': 1})

    def parse(self, response, page):
        link_list = response.xpath('//li[@class="product-item"]//div[@class="mode-cell thumb"]/a/@href').getall()
        link_list = ["https://www.bobaedream.co.kr/" + i for i in link_list]
        yield from response.follow_all(link_list, self.product_detail, dont_filter=True)

        # pagination
        next_page = response.xpath('//a[@class="next"]/@href').get()
        if next_page:
            page += 1
            page_link = f'https://www.bobaedream.co.kr/cyber/CyberCar.php?sel_m_gubun=ALL&page={page}&order=S11&view_size=70'
            yield response.follow(url=page_link, callback=self.parse, cb_kwargs={'page': page})

    def product_detail(self, response):
        output = {}

        form_data_keys = response.xpath('//div[@class="info-basic"]//th').getall()
        form_data_values = response.xpath('//div[@class="info-basic"]//td').getall()

        for k in range(len(form_data_keys)):
            key = Selector(text=form_data_keys[k]).xpath("//text()").get().strip()
            value = Selector(text=form_data_values[k]).xpath("//text()").get().strip()

            if key == "연식":
                output["year"] = int(value.split(".")[0])
            elif key == "배기량":
                output["engine_displacement_value"] = value.split("(")[0].split(" ")[0].replace(",", "")
                output["engine_displacement_unit"] = "cc"
            elif "Year" in key:
                output["year"] = int(value)
            elif key == "주행거리":
                output['odometer_value'] = int(value.split(' ')[0].replace(',', ''))
                output['odometer_unit'] = value.split(' ')[-1]
            elif "	색상" == key:
                output["exterior_color"] = value
            elif "자동" == key:
                if value == "오토":
                    output["transmission"] = "auto"
                if value == "수동":
                    output["transmission"] = "manual"
                if value == "듀얼클러치":
                    output["transmission"] = "dual clutch"
                if value == "CVT":
                    output["transmission"] = "cvt"
            elif key == "연료":
                if value == "디젤":
                    output["fuel"] = "diesel"
                if value == "가솔린":
                    output["fuel"] = "gasoline"
                if value == "CNG":
                    output["fuel"] = "CNG"
                if value == "LPG":
                    output["fuel"] = "LPG"
                if value == "하이브리드(가솔린)":
                    output["fuel"] = "Hybrid(gasoline)"
                if value == "하이브리드(LPG)":
                    output["fuel"] = "Hybrid(LPG)"
                if value == "전기":
                    output["fuel"] = "electricity"

        spec_values = response.xpath('//div[@class="wrap-detail-spec mode-n6"]//dd/strong//text()').getall()
        if len(spec_values) > 1:
            if "kg" not in spec_values[-2]:
                output["drive_train"] = spec_values[-2]

        output["ac_installed"] = 0
        output["tpms_installed"] = 0

        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "BobaeDream"
        output["scraped_listing_id"] = response.url.split("no=")[-1].split("&")[0]
        output["vehicle_url"] = response.url

        output["country"] = "KR"

        price = response.xpath('//span[@class="price"]/b//text()').get()
        if price is not None and price.replace(",", "").isnumeric():
            output["price_retail"] = float(price.replace(",", "")) * 10000
            output["currency"] = "KRW"

        picture_list = response.xpath('//div[@class="gallery-thumb js-gallery-thumb"]//img/@src').getall()
        if picture_list is not None and len(picture_list) > 0:
            if "//image.bobaedream.co.kr/newimg/newcyber/opt1120.jpg" in picture_list:
                picture_list = picture_list.remove("//image.bobaedream.co.kr/newimg/newcyber/opt1120.jpg")
            if picture_list is not None and len(picture_list) > 0:
                picture_list = ["https:" + i for i in picture_list]
                output["picture_list"] = json.dumps(picture_list)

        spec_link = response.xpath('//div[@class="wrap-detail-spec mode-n6"]//dd[last()]/a/@href').get()
        if spec_link is not None:
            yield scrapy.Request(url="https://www.bobaedream.co.kr" + spec_link, callback=self.get_make_model_trim,
                                 cb_kwargs={'output': output})

    def get_make_model_trim(self, response, output):
        make_options = response.xpath('//select[@name="maker_no"]//option').getall()
        for option in make_options:
            attributes = Selector(text=option).xpath("//@*").getall()
            if "selected" in attributes:
                selected_make = Selector(text=option).xpath("//text()").get()
                output["make"] = selected_make

        model_options = response.xpath('//select[@name="model_no"]//option').getall()
        for option in model_options:
            attributes = Selector(text=option).xpath("//@*").getall()
            if "selected" in attributes:
                selected_make = Selector(text=option).xpath("//text()").get()
                output["model"] = selected_make

        trim_options = response.xpath('//select[@name="level_no"]//option').getall()
        for option in trim_options:
            attributes = Selector(text=option).xpath("//@*").getall()
            if "selected" in attributes:
                selected_make = Selector(text=option).xpath("//text()").get()
                output["trim"] = selected_make

        apify.pushData(output)
