import datetime
import json
import scrapy
import re
import apify
from scrapy_playwright.page import PageMethod
from loguru import logger
from scrapy import Selector


class KbchachachaSpider(scrapy.Spider):
    name = "kbchachacha"
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    max_req = {}  # Storage required retry link
    max_retry = 5  # retry count

    def start_requests(self):
        url = 'https://www.kbchachacha.com/public/search/main.kbc'
        yield scrapy.Request(url=url, callback=self.get_make_urls, meta={"playwright": True,
                                                                         "playwright_include_page": True,
                                                                         "playwright_context": datetime.datetime.isoformat(
                                                                             datetime.datetime.today()),
                                                                         "playwright_page_methods": [
                                                                             PageMethod('wait_for_selector',
                                                                                        'div.checkList__manufacturer__title', )],
                                                                         "playwright_page_goto_kwargs": {
                                                                             "timeout": 0,
                                                                         }})

    async def get_make_urls(self, response):
        page = response.meta["playwright_page"]
        makes_objects = []
        makes_list = response.xpath(
            "//div[@class='checkList checkList--model']//div[@class='checkList__manufacturer__title']").getall()
        for k in range(len(makes_list)):
            count = Selector(text=makes_list[k]).xpath("//span[@class='number']//text()").get()
            name = Selector(text=makes_list[k]).xpath("//a/span//text()").get()
            href_text = Selector(text=makes_list[k]).xpath("//a/@href").get()
            makes_objects.append({
                "make": name,
                "count": int(count.replace(",", "")),
                "code": re.findall("'([^']*)'", href_text)[0]
            })

        for make_object in makes_objects:
            url = f'https://www.kbchachacha.com/public/search/list.empty?page=1&sort=-orderDate&makerCode={make_object["code"]}&_pageSize=4&pageSize=5'
            yield scrapy.Request(url=url, callback=self.parse, meta={
                'make': make_object["make"],
                'code': make_object["code"],
                'count': make_object["count"],
                'page': 1,
                "playwright": True,
                "playwright_include_page": True,
                "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                "playwright_page_goto_kwargs": {"timeout": 0, }})

        await page.wait_for_load_state("networkidle")
        await page.context.close()  # close the context
        await page.close()
        del page

    async def parse(self, response):
        page = response.meta["playwright_page"]
        link_list = response.xpath("//div[@class='item']/a/@href").getall()
        link_list = ["https://www.kbchachacha.com" + i for i in link_list]
        for link in link_list:
            yield scrapy.Request(link, self.product_detail, meta={"make": response.meta["make"], "playwright": True,
                                                                  "playwright_include_page": True,
                                                                  "playwright_context": datetime.datetime.isoformat(
                                                                      datetime.datetime.today()),
                                                                  "playwright_page_methods": [
                                                                      PageMethod('wait_for_selector',
                                                                                 'div.car-detail-info', )],
                                                                  "playwright_page_goto_kwargs": {
                                                                      "timeout": 0,
                                                                  }})

        # pagination
        make_code = response.meta["code"]
        current_page = response.meta["page"]
        next_page = response.meta["page"] + 1
        total_count = response.meta["count"]
        total_pages = total_count / 39
        if current_page < total_pages and current_page < 401:
            url = f'https://www.kbchachacha.com/public/search/list.empty?page={next_page}&sort=-orderDate&makerCode={make_code}&_pageSize=4&pageSize=5'
            yield scrapy.Request(url=url, callback=self.parse, meta={'code': make_code, 'count': total_count,
                                                                     'page': next_page,
                                                                     "make": response.meta["make"], "playwright": True,
                                                                     "playwright_include_page": True,
                                                                     "playwright_context": datetime.datetime.isoformat(
                                                                         datetime.datetime.today()),
                                                                     "playwright_page_goto_kwargs": {"timeout": 0}})

        await page.wait_for_load_state("networkidle")
        await page.context.close()  # close the context
        await page.close()
        del page

    async def product_detail(self, response):
        page = response.meta["playwright_page"]

        output = {}
        output["make"] = response.meta["make"]
        car_name = response.xpath('//strong[@class="car-buy-name"]/text()').get().split(")")[-1]
        if output["make"] in car_name:
            output["model"] = car_name.replace(output["make"], "")
        else:
            output["model"] = car_name

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'kbchachacha'
        output['scraped_listing_id'] = response.url.split('carSeq=')[-1]
        output["vehicle_url"] = response.url

        car_info = response.xpath('//div[@class="car-buy-share"]/div[@class="txt-info"]/span/text()').getall()
        output["city"] = car_info[-1]
        output['country'] = 'KR'
        output["year"] = int(f'20{car_info[0][0:2]}')

        car_price_text = response.xpath('//div[@class="car-buy-price"]//dl//strong//text()').get()
        car_price = re.findall(r'\d+', car_price_text.replace(",", ""))
        output["price_retail"] = float(car_price[0]) * 10000
        output["currency"] = "KRW"

        picture_list = response.xpath("//a[@class='slide-img__link']/img/@src").getall()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        table_keys = response.xpath('//table[@class="detail-info-table"]/tbody//th//text()').getall()
        table_values = response.xpath('//table[@class="detail-info-table"]/tbody//td//text()').getall()
        for k in range(len(table_keys)):
            key = table_keys[k].strip()
            value = table_values[k].strip()
            if key == "차종":
                if value == "트럭":
                    output["body_type"] = "truck"
                if value == "대형":
                    output["body_type"] = "large"
                if value == "중형":
                    output["body_type"] = "medium"
                if value == "준중형":
                    output["body_type"] = "semi-medium"
                if value == "승합":
                    output["body_type"] = "ride"
                if value == "스포츠카":
                    output["body_type"] = "sports car"
                if value == "RV":
                    output["body_type"] = "RV"
                if value == "SUV":
                    output["body_type"] = "SUV"
                if value == "버스":
                    output["body_type"] = "bus"

            elif key == "배기량":
                output["engine_displacement_value"] = value.replace("cc", "").replace(",", "")
                output["engine_displacement_unit"] = "cc"
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

            elif key == "주행거리":
                output["odometer_value"] = int(value.replace("km", "").replace(",", ""))
                output["odometer_unit"] = "km"
            elif key == "변속기":
                if value == "오토":
                    output["transmission"] = "auto"
                if value == "수동":
                    output["transmission"] = "manual"
                if value == "세미오토":
                    output["transmission"] = "semi-auto"
                if value == "CVT":
                    output["transmission"] = "cvt"

        apify.pushData(output)

        await page.wait_for_load_state("networkidle")
        await page.context.close()  # close the context
        await page.close()
        del page

