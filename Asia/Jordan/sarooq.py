from datetime import datetime
from scrapy import Request
import apify
import re
import scrapy

class SarooqSpider(scrapy.Spider):
    name = "Sarooq"
    start_urls = [
        "https://sarooq.com/search?l=641&c=1&distance=500&sc=2",
    ]

    def parse(self, response):
        for listing in response.css("div.item-list"):
            listing_url = listing.css("h5.add-title a::attr(href)").get()
            yield Request(
                listing_url,
                meta={"vehicle_url": listing_url},
                callback=self.get_listing_details,
            )

        # link to the next page, if exists
        next_page_url = response.xpath(
            '//*[@id="wrapper"]/div[4]/div[2]/div/div[2]/nav/ul'
        ).get()

        if next_page_url is not None:
            yield Request(next_page_url, callback=self.parse)

    def get_listing_details(self, response):
        output = {}
        output["vehicle_url"] = str(response.meta["vehicle_url"])
        output["country"] = "JO"
        output["scraped_from"] = "Sarooq"
        output["scraped_date"] = datetime.isoformat(datetime.today())
        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        details = response.xpath('//*[@id="item-details"]/div/div/div[3]/div[2]/div')
        for field in details.css("div.col-12"):
            field_title = field.css(".fw-bolder::text").get().lower()
            if field_title == "fuel type":
                output["fuel"] = field.css(".text-start::text").get()
            elif field_title == "transmission":
                output["transmission"] = field.css(".text-start::text").get()
            elif field_title == "car model":
                output["model"] = field.css(".text-start::text").get()
            elif field_title == "car brand":
                output["make"] = field.css(".text-start::text").get()
            elif field_title == "year of registration":
                output["year"] = field.css(".text-start::text").get()
            elif field_title == "condition":
                output["is_used"] = field.css(".text-start::text").get()
            elif field_title == "number of doors":
                output["doors"] = field.css(".text-start::text").get()
            elif "number of cylinders":
                output["engine_cylenders"] = field.css(".text-start::text").get()
            elif field_title == "engine capacity":
                output["engine_displacement_value"] = field.css(
                    ".text-start::text"
                ).get()
            elif "number of seats":
                output["seats"] = field.css(".text-start::text").get()
        city = response.xpath(
            '//*[@id="wrapper"]/div[4]/div[2]/div/div[2]/aside/div[1]/div[2]/div[1]/div[1]/div[2]/span/a/text()'
        ).get()
        output["city"] = re.sub("\s+", "", city)
        self.handle_pictures(response, output)
        self.handle_price(response, output)
        apify.pushData(output)

    def handle_pictures(self, response, output):
        output["picture_list"] = []
        pictures_list = response.css("div.swiper-wrapper")[0]
        for picture in pictures_list.css("div.swiper-slide"):
            image_url = picture.css("img::attr(src)").get()
            output["picture_list"].append(image_url)

    def handle_price(self, response, output) -> bool:
        output["currency"] = "JOD"
        price = re.sub(
            "\s+",
            "",
            response.xpath(
                '//*[@id="item-details"]/div/div/div[1]/div[2]/h4/span[2]/text()'
            ).get(),
        )
        if price.lower() != "contactus":
            output["price_retail"] = price[: len(price) - 2]
            output["price_wholesale"] = price[: len(price) - 2]
            return True
        return True