import datetime
import json
import scrapy
# import apify

class AutowereldSpider(scrapy.Spider):
    name = 'Autowereld'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.autowereld.nl/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.makes_url)

    def makes_url(self, response):
        makes_links = response.xpath('//div[@class="brand-logo-list vmar"]/ul/li/a/@href').getall()
        print(makes_links)

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath('//a[@class="frame"]/@href').getall()
        yield from response.follow_all(product_links, self.detail)

        # pagination
        page_link = response.xpath('//a[@class="aw arrow next"]/@href').get()
        if page_link is not None:
            yield response.follow(page_link, self.parse)
    def detail(self,response):
       pass
