import datetime
import json
import re
import scrapy
import apify


class ParkersSpider(scrapy.Spider):
    name = 'Parkers'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"


    def start_requests(self):
        urls = [
            'https://www.parkers.co.uk/cars-for-sale/used/',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.get_location_links)

    def get_location_links(self, response):
        location_names = response.xpath('//a[@class="seo-crawl-paths__group__link"]/text()').getall()
        for location in location_names:
            yield scrapy.Request(url=f'https://www.parkers.co.uk/search-results/location-{location.lower()}', callback=self.parse, meta={"city": location})

    def parse(self, response):
        city = response.meta["city"]
        # Traverse product links
        product_links = response.xpath('//a[@class="for-sale-result-item__image"]/@href').getall()
        for link in product_links:
            yield scrapy.Request(url=f'https://www.parkers.co.uk{link}', callback=self.detail, meta={"city": city})

        next_page = response.xpath('//a[@class="results-paging__next__link"]/@href').get()
        if next_page is not None:
            yield response.follow(f'https://www.parkers.co.uk{next_page}', self.parse, meta={"city": city})

    def detail(self, response):
        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'Parkers'
        output['scraped_listing_id'] = response.url.split('/')[-2].split('-')[-1]
        output["vehicle_url"] = response.url

        output['country'] = 'UK'
        output["city"] = response.meta["city"]

        output["make"] = response.url.split('/')[3]
        output["model"] = response.url.split('/')[4]
        
        title = response.xpath('//h1[@class="main-heading__title "]/text()').get()
        output["year"] = int(re.search(r'\((.*?)\)', title).group(1))

        trim = response.xpath('//div[@class="main-heading__sub-title "]/text()').get()
        if trim is not None:
            output["trim"] = trim

        output["body_type"] = response.url.split('/')[5]

        pictures_list = response.xpath('//li[@class="cfs-used-details-page__thumbnails__item"]/a/img/@src').getall()
        if pictures_list:
            output["picture_list"] = json.dumps(pictures_list)

        price = response.xpath('//li[@class="cfs-used-details-page__summary__price price"]/text()').get()
        if price is not None:
            output["price_retail"] = float(price.strip().replace("£", "").replace(",", ""))
            output["currency"] = "GBP"

        basic_details = response.xpath('//li[@class="cfs-used-details-page__summary__item bullet-item"]/text()').getall()
        output["exterior_color"] = basic_details[1]
        output["odometer_value"] = int(basic_details[-1].split(" ")[0].replace(",", ""))
        output["odometer_units"] = basic_details[-1].split(" ")[-1]
        output["fuel"] = basic_details[0].split()[0]
        output["transmission"] = basic_details[0].split()[-1]

        view_more_link = response.xpath('//table[@class="cfs-used-details-page__specifications"]//a[@class="panel__view-link"]/@href').get()
        yield scrapy.Request(url=f'https://www.parkers.co.uk{view_more_link}', callback=self.specs, meta={"output": output})

    def specs(self, response):
        output = response.meta["output"]
        engine_specs_keys = response.xpath('//section[@class="specs-detail-page__engine specs-detail-page__section specs-detail-page__section--background-blue"]//th/text()').getall()
        engine_specs_values = response.xpath(
            '//section[@class="specs-detail-page__engine specs-detail-page__section specs-detail-page__section--background-blue"]//td/text()').getall()

        for k in range(len(engine_specs_keys)):
            key = engine_specs_keys[k]
            value = engine_specs_values[k]
            if value != "-":
                if key == "Engine Size":
                    output["engine_displacement_value"] = re.findall(r'\d+', value)[0]
                    output["engine_displacement_units"] = value.replace(output["engine_displacement_value"], "")
                elif key == "Cylinders":
                    output["engine_cylinders"] = int(value)
                elif key == "Drivetrain":
                    output["drive_train"] = value

        cabin_specs_keys = response.xpath(
            '//section[@class="specs-detail-page__cabin-luggage specs-detail-page__section specs-detail-page__section--pale-cyan"]//th/text()').getall()
        cabin_specs_values = response.xpath(
            '//section[@class="specs-detail-page__cabin-luggage specs-detail-page__section specs-detail-page__section--pale-cyan"]//td/text()').getall()

        for k in range(len(cabin_specs_keys)):
            key = cabin_specs_keys[k]
            value = cabin_specs_values[k]
            if key == "Doors":
                output["doors"] = int(value)
            elif key == "Seats":
                output["seats"] = int(value)

        apify.pushData(output)
