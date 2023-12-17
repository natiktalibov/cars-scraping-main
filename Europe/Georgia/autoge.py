from datetime import datetime
from scrapy import Request
import scrapy
import json
import apify


class AutogeSpider(scrapy.Spider):
    name = "Autoge"
    start_urls = [
        "https://www.auto.ge/en/auto/search-results.html?action=search&post_form_key=listings_advanced&f%5BCategory_ID%5D=&f%5Bcategory_parent_ids%5D=&f%5Bsale_rent%5D=1&f%5Bbuilt%5D%5Bfrom%5D=0&f%5Bbuilt%5D%5Bto%5D=0&f%5Bcustom%5D=0&f%5Bsort_by%5D=date&f%5Bsort_type%5D=asc&search=Search",
    ]
    number_of_total_pages = 0
    number_of_scrapped_pages = 0

    def parse(self, response):
        if self.number_of_total_pages == 0:
            self.number_of_total_pages = int(
                response.xpath('//*[@id="controller_area"]/ul/li[1]/span[2]/text()')
                .extract()[0]
                .split()[1]
            )

        for listing in response.css("article.item"):
            listing_url = listing.css("div.main-column a::attr(href)").get()

            yield Request(
                listing_url,
                meta={"vehicle_url": listing_url},
                callback=self.get_listing_details,
            )

        # link to the next page, if exists
        next_page_url = response.css(
            "ul.pagination li.navigator.rs a::attr(href)"
        ).get()

        if next_page_url is not None and self.number_of_scrapped_pages <= self.number_of_total_pages:
            self.number_of_scrapped_pages = self.number_of_scrapped_pages + 1
            yield Request(next_page_url, callback=self.parse)

    def get_listing_details(self, response):
        output = {}
        output["vehicle_url"] = str(response.meta["vehicle_url"])
        output["country"] = "GE"
        output["currency"] = "USD"
        output["scraped_from"] = "Autoge"
        output["scraped_date"] = datetime.isoformat(datetime.today())
        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        price_field = response.xpath(
            '//*[@id="df_field_price"]/span[1]/text()'
        ).extract()
        if len(price_field) > 0:
            output["price_retail"] = price_field[0].split()[0].replace(",", "")

        make_model = (
            response.css("div.listing-header h1::text").extract_first()
        )
        if not make_model:  # error page
            return None
        else:
            make_model = make_model.split(",")
        output["make"] = make_model[0]
        output["model"] = "".join(make_model[1:])

        details = response.css("div.listing-fields")
        for field in details.css("div.table-cell"):
            field_title = field.css("div::attr(title)").extract_first()
            if field_title.lower() == "transmission":
                output["transmission"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "condition":
                output["is_used"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "cylinders":
                output["engine_cylenders"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "fuel":
                output["fuel"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "body style":
                output["body_type"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "wheel":
                output["steering_position"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "engine":
                output["engine_displacement_value"] = field.css(
                    "div.value::text"
                ).extract()[1]
                output["engine_displacement_units"] = "L"
            if field_title.lower() == "built":
                output["year"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "doors":
                output["doors"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "interior color":
                output["interior_color"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "color":
                output["exterior_color"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "reference number":
                output["scraped_listing_id"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "town":
                output["city"] = field.css("div.value::text").extract()[1]
            if field_title.lower() == "mileage":
                value = field.css("div.value::text").extract()[1].split()
                output["odometer_value"] = value[0]
                if len(value) > 1:
                    output["odometer_unit"] = value[1]

        pictures_list = response.xpath('//img[@class="swiper-lazy"]/@src').getall()

        if pictures_list:
            output["picture_list"] = json.dumps(pictures_list)
        apify.pushData(output)



