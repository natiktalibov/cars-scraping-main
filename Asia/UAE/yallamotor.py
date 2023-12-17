# import os

import scrapy
import json
import datetime
import gc
import re

import apify


class YallamotorSpider(scrapy.Spider):
    name = "yallamotor"
    CONCURRENT_REQUESTS_PER_DOMAIN = 5
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    download_timeout = 120
    start_urls = [
        'https://uae.yallamotor.com/used-cars/search?page=1&sort=updated_desc', ]

    # 'https://ksa.yallamotor.com/used-cars/search?page=1&sort=updated_desc',
    # 'https://egypt.yallamotor.com/used-cars/search?page=1&sort=updated_desc',
    # 'https://qatar.yallamotor.com/used-cars/search?page=1&sort=updated_desc',
    # 'https://oman.yallamotor.com/used-cars/search?page=1&sort=updated_desc',
    # 'https://kuwait.yallamotor.com/used-cars/search?page=1&sort=updated_desc',
    # 'https://bahrain.yallamotor.com/used-cars/search?page=1&sort=updated_desc']

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
                    }
                },
                errback=self.close_context_on_error,
            )

    async def close_context_on_error(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.context.close()
        await page.close()
        del page
        gc.collect()

    async def parse(self, response):
        page = response.meta["playwright_page"]
        await page.wait_for_selector(selector="//div[@class='col is-8 p0 p4r']//a[@class='black-link']", timeout=60000,
                                     state='visible')
        await page.context.close()  # close the context
        await page.close()
        del page
        gc.collect()

        detail_link = response.xpath("//div[@class='col is-8 p0 p4r']//a[@class='black-link']/@href").extract()
        detail_link = [response.url.split("/used-cars")[0] + i for i in detail_link]

        for url in detail_link:
            yield response.follow(
                url=f"{url}",
                cb_kwargs={'max_retry':1},
                callback=self.product_detail,
                errback=self.close_context_on_error,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                    "playwright_page_goto_kwargs": {
                        "url": url,
                    }
                },
            )

        if response.xpath("//a[@class='next available']/@href").extract_first():
            if "page=" in response.url:
                current_page = int(re.findall("page=(.*?)&", response.url, re.S)[0])
                new_product_list_url = str(response.url).replace(f"page={str(current_page)}",
                                                                 f"page={str(current_page + 1)}")
            else:  # When the page number is 1, the detail page URL does not display the page number information
                current_page = 1
                new_product_list_url = f"https://uae.yallamotor.com/used-cars/search?page={current_page + 1}&sort=updated_desc"

            yield scrapy.Request(
                url=new_product_list_url,
                errback=self.close_context_on_error,
                callback=self.parse,
                # headers=self.headers,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": datetime.datetime.isoformat(datetime.datetime.today()),
                }
            )

    async def product_detail(self, response, max_retry):
        output = {}
        page = response.meta["playwright_page"]
        await page.wait_for_load_state("networkidle")
        await page.wait_for_selector(selector="//span[@class='selectedCountry font-b']", timeout=60000, state='visible')
        await page.wait_for_selector(selector='//div[@class="font18 m8t"]/span', timeout=60000, state='attached')
        await page.context.close()  # close the context
        await page.close()
        del page
        gc.collect()

        price = response.xpath('//div[@class="font18 m8t"]/span/text()').get()
        if price is None:
            if max_retry <= 5:
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
        if price:

            output['ac_installed'] = 0
            output['tpms_installed'] = 0
            output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
            output['scraped_from'] = "yallamotor"
            output['scraped_listing_id'] = response.url.split("-")[-1]
            output['country'] = 'AE'
            output['make'] = response.url.split("/")[4]
            output['model'] = response.url.split("/")[5]
            output['year'] = int(response.url.split("/")[6])
            output['price_retail'] = float(price.replace(",", ""))
            output['currency'] = response.xpath('//div[@class="font18 m8t"]/text()').extract_first()

            form_data = response.xpath("//div[@class='p16 p24t p0b linehight-normal']//div[contains(@class, 'box1')]")
            for i in form_data:
                key = i.xpath('./div[2]/text()').extract_first()
                value = i.xpath('./div[3]/text()').extract_first()
                if "N/A" in value:
                    continue
                if "Transmission" in key:
                    if "translation missing" in value:
                        continue
                    output['transmission'] = value
                elif "Fuel Type" in key:
                    output['fuel'] = value
                elif "Kilometers" in key:
                    if int("".join([i for i in list(value) if i.isdigit()])) != 0:
                        output['odometer_value'] = int("".join([i for i in list(value) if i.isdigit()]))
                        output['odometer_unit'] = "".join([i for i in list(value) if i.isalpha()])
                elif "Location" in key:
                    output['city'] = value

            output['vehicle_url'] = response.url

            picture_list = response.xpath(
                '//*[@id="overviewnav"]/div[2]/div/div[2]/div/div/div/div/div/div/div/img/@src').getall()
            if picture_list:
                output['picture_list'] = json.dumps(picture_list)

            # yield output
            apify.pushData(output)
