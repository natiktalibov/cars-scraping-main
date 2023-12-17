import json
import scrapy
import datetime
import time
import gc
from scrapy import Selector

import apify
from scrapy.downloadermiddlewares.retry import get_retry_request


class AutotraderSpider(scrapy.Spider):
    name = 'autotradernz'
    # CONCURRENT_REQUESTS_PER_DOMAIN = 5
    start_urls = ['https://www.autotrader.co.nz/used-cars-for-sale/']
    download_timeout = 120

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                    "playwright_page_goto_kwargs": {
                        "url": url,
                        "wait_until": "networkidle"
                    }
                },
                errback=self.close_context_on_error,
            )

    async def close_context_on_error(self, failure):
        try:
            page = failure.request.meta["playwright_page"]
            await page.context.close()
            await page.close()
            del page
            gc.collect()
        except Exception as f:
            print('===========',f)

    async def parse(self, response):
        page = response.meta["playwright_page"]
        # Traverse product links
        product_links = response.xpath("//div[@class='show-for-medium']//div[@class='vehicle-image']/a/@href").getall()
        for link in product_links:
            yield scrapy.Request(
                url=link,
                cb_kwargs={'max_retry': 1},
                callback=self.product_detail,
                errback=self.close_context_on_error,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                    "playwright_page_goto_kwargs": {
                        "url": link,
                        "wait_until": "networkidle"
                    }
                },
            )

        # pagination
        self.pagination(response)

        # await page.context.close()  # close the context
        await page.close()
        del page
        gc.collect()

    async def pagination(self, response):
        page = response.meta["playwright_page"]

        current_page = response.xpath("//div[@class='paging-page current ng-binding']/text()").get()
        pages_list = response.xpath("//div[@class='paging-page ng-scope']/a/text()").getall()
        if not pages_list:
            pages_list = response.xpath("//div[@class='paging-page ng-scope']/a/text()").getall()

        while current_page != pages_list[-1]:
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector(selector="//div[@class='paging-next']/a", timeout=60000, state='visible')
            await page.click(selector="//div[@class='paging-next']/a")

            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector(selector="//div[@class='paging-next']", timeout=60000)
            # await page.screenshot(path=f"{index}.png", full_page=True)

            text = await page.content()
            html = Selector(text=text)

            # Traverse product links
            product_links = response.xpath(
                "//div[@class='show-for-medium']//div[@class='vehicle-image']/a/@href").getall()
            for link in product_links:
                yield scrapy.Request(
                    url=link,
                    cb_kwargs={'max_retry': 1},
                    callback=self.product_detail,
                    errback=self.close_context_on_error,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                        "playwright_page_goto_kwargs": {
                            "url": link,
                            "wait_until": "networkidle"
                        }
                    },
                )

            current_page = html.xpath("//div[@class='paging-page current ng-binding']/text()").get()
            pages_list = html.xpath("//div[@class='paging-page ng-scope']/a/text()").getall()

        await page.context.close()  # close the context
        await page.close()
        del page
        gc.collect()

    async def product_detail(self, response, max_retry):
        output = {}
        page = response.meta["playwright_page"]
        try:
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector(selector='//*[@id="VehicleController"]/script[2]', timeout=60000, state='attached')
        except Exception:
            date_text = None
        date_text = response.xpath('//*[@id="VehicleController"]/script[2]/text()').get()
        if date_text is None:
            if max_retry <= 20:
                max_retry += 1
                yield scrapy.Request(
                    url=response.url,
                    callback=self.product_detail,
                    dont_filter=True,
                    cb_kwargs={'max_retry': max_retry},
                    meta={"playwright": True,
                          "playwright_include_page": True,
                          "playwright_context": datetime.datetime.isoformat(datetime.datetime.today())
                          },
                    errback=self.close_context_on_error,
                )


        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Autotrader'
        output['scraped_listing_id'] = response.url.split('/')[-2]
        output['vehicle_url'] = response.url
        output['picture_list'] = json.dumps(response.xpath(
            "/html/body/div[2]/div/div[4]/div[1]/div[2]/div[1]/div[2]/div[1]/div[4]/div[1]/div/div/div[1]/div/div/div/a/img/@src").getall())
        output['country'] = 'NZ'

        if response.xpath('//*[@id="dealer-details-lg"]/div/div/div[1]/div/div/div[2]/text()').get() is not None:
            output['city'] = response.xpath(
                '//*[@id="dealer-details-lg"]/div/div/div[1]/div/div/div[2]/text()').get().strip()
        if response.xpath('//*[@id="vs-about-dealer"]/div/div[2]/div/div/ul/li[1]/text()').get() is not None:
            try:
                output['city'] = response.xpath(
                    '//*[@id="vs-about-dealer"]/div/div[2]/div/div/ul/li[1]/text()').get().strip()
            except Exception:
                pass
        if response.xpath('//*[@id="dealer-details-lg"]/div/div/ul[1]/li[1]/text()').get() is not None:
            output['city'] = response.xpath('//*[@id="dealer-details-lg"]/div/div/ul[1]/li[1]/text()').get().split(',')[
                -1].strip()


        if date_text is None:
            date_text = response.xpath('/html/body/div[2]/div/div[4]/div[1]/div[2]/script[2]/text()').get()
        jsn = json.loads(date_text)

        if jsn["manufacturer"] is not None:
            output['make'] = jsn["manufacturer"]
        if jsn["model"] is not None:
            output['model'] = jsn["model"]
        if jsn["vehicleModelDate"] is not None:
            output['year'] = int(jsn["vehicleModelDate"].replace('-01-01', ''))
        if jsn["offers"]["price"] is not None:
            output['price_retail'] = float(jsn["offers"]["price"])
        if jsn["offers"]["priceCurrency"] is not None:
            output['currency'] = jsn["offers"]["priceCurrency"]
        if jsn["mileageFromOdometer"]["value"] is not None:
            output['odometer_value'] = int(jsn["mileageFromOdometer"]["value"])
            output['odometer_unit'] = 'km'
        if jsn["fuelType"] is not None:
            output['fuel'] = jsn["fuelType"]
        try:
            if jsn["vehicleTransmission"] is not None:
                output['transmission'] = jsn["vehicleTransmission"]
        except KeyError:
            pass
        try:
            output['engine_displacement_value'] = int(jsn["vehicleEngine"]["engineDisplacement"]["value"])
            output['engine_displacement_units'] = jsn["vehicleEngine"]["engineDisplacement"]["unitText"]
        except KeyError:
            pass
        apify.pushData(output)
        # yield output
        await page.context.close()  # close the context
        await page.close()
        del page
        gc.collect()