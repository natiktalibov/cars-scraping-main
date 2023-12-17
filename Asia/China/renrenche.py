import datetime
import json
import scrapy
from scrapy_playwright.page import PageMethod
from loguru import logger
from scrapy import Selector
import apify


class RenrencheSpider(scrapy.Spider):
    name = 'Renrenche'
    download_timeout = 120
    max_req = {}  # Storage required retry link
    max_retry = 5  # retry count

    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def start_requests(self):
        urls = [
            'https://www.renrenche.com/hk/ershouche/p1/?&plog_id=25d960727c96a1588c9f441e5e0d6c45',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'pageN': 1},
                                 meta={"playwright": True, "playwright_include_page": True,
                                       "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                                       "playwright_page_methods": [
                                           PageMethod('wait_for_selector', 'div.jscroll-added', )],
                                       "playwright_page_goto_kwargs": {"timeout": 0}})

    async def parse(self, response, pageN):
        page = response.meta["playwright_page"]
        # Traverse product links
        product_links = response.xpath('//li[@class="span6 list-item car-item "]/a/@href').getall()
        for link in product_links:
            # r-basic__info
            yield scrapy.Request(url=f'https://www.renrenche.com{link}', callback=self.detail,
                                 meta={"playwright": True, "playwright_include_page": True,
                                       "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                                       "playwright_page_methods": [PageMethod('wait_for_selector', 'div.r-basic__info'),
                                                                   PageMethod('wait_for_timeout', 60 * 1000)],
                                       "playwright_page_goto_kwargs": {"timeout": 0}})

            # pagination
        li = response.xpath('//ul[@class="pagination js-pagination"]/li/a/text()').getall()
        last_page = li[-1]
        if pageN != int(last_page):
            pageN += 1
            page_link = f'https://www.renrenche.com/hk/ershouche/p{pageN}/'
            yield scrapy.Request(url=page_link, callback=self.parse, cb_kwargs={'pageN': pageN},
                                 meta={"playwright": True, "playwright_include_page": True,
                                       "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                                       "playwright_page_methods": [
                                           PageMethod('wait_for_selector', 'div.jscroll-added', )],
                                       "playwright_page_goto_kwargs": {"timeout": 0}})
            await page.context.close()
            await page.close()
            del page

    async def detail(self, response):
        page = response.meta["playwright_page"]
        output = {}
        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'renrenche'
        output["vehicle_url"] = response.url
        output['scraped_listing_id'] = response.url.split('/')[-1]

        output['country'] = 'CN'
        title = response.xpath("//div[@class='r-basic__info']/h1[@class='title']/text()").get()
        output["make"] = title.split(" ")[0].split("-")[0]
        output["model"] = title.split(" ")[0].split("-")[1]
        year = title.split(" ")[1][0:4]

        if year and year.isnumeric():
            output["year"] = year
        price = response.xpath("//span[@class='newcar-price']/span[@class='num']/text()").get()
        if price is not None:
            output["price_retail"] = float(price) * 10000
            output["currency"] = "CNY"

        pictures_list = response.xpath(
            "//div[@class='r-basic-pic__thund clearfix']//ul[@class='clearfix']/li/img/@src").getall()

        if pictures_list:
            output["picture_list"] = json.dumps(pictures_list)

        parameters_keys = response.xpath("//ul[@class='parameters']/li/p[@class='name']").getall()
        parameters_values = response.xpath("//ul[@class='parameters']/li/p[@class='value']").getall()
        for k in range(len(parameters_keys)):

            key = Selector(text=parameters_keys[k]).xpath("//text()").get()
            value = Selector(text=parameters_values[k]).xpath("//text()").get()
            if key == "表显里程":
                output["odometer_value"] = int(float(value) * 10000)
                output["odometer_unit"] = "km"
            elif key == "车辆所在地":
                output["city"] = value
            elif key == "变速箱":
                output["transmission"] = value

        apify.pushData(output)
        await page.context.close()
        await page.close()
        del page