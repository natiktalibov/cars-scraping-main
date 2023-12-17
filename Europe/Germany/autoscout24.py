import datetime
import json
import scrapy
from scrapy import Selector
import apify

class Autoscout24Spider(scrapy.Spider):
    name = 'AutoScout24'

    def start_requests(self):
        user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
        urls = [
            'https://www.autoscout24.de/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.get_makes_links)

    def get_makes_links(self, response):
        makes = response.xpath("//select[@id='make']//option/text()").getall()
        # years = response.xpath("//select[@id='firstRegistration']//option/text()").getall()
        for make in makes[1:]:
            formatted_make = make.lower().replace(" ", "-")
            # for year in years[1:]:
            link = f'https://www.autoscout24.com/lst/{formatted_make}?atype=C&cy=D&desc=0&sort=standard&source=homepage_search-mask&ustate=N%2CU'
            yield scrapy.Request(url=link, callback=self.parse, meta={"make": formatted_make, "page": 1})

    def parse(self, response):
        links = response.xpath("//a[@class='ListItem_title__znV2I ListItem_title_new_design__lYiAv Link_link__pjU1l']/@href").getall()
        for link in links:
            url = f'https://www.autoscout24.com{link}'
            yield scrapy.Request(url=url, callback=self.detail)

        # pagination
        if len(links) > 0:
            page = response.meta["page"]
            make = response.meta["make"]
            link = f'https://www.autoscout24.com/lst/{make}?atype=C&cy=D&desc=0&sort=standard&source=homepage_search-mask&ustate=N%2CU&page={page+1}'
            yield scrapy.Request(url=link, callback=self.parse,  meta={"make": make, "page": page})

    def detail(self, response):
        output = dict()

        output['ac_installed'] = 0
        output['tpms_installed'] = 0

        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'AutoScout24'
        output["vehicle_url"] = response.url

        details_first_section = response.xpath("//dl[@class='DataGrid_defaultDlStyle__969Qm'][1]").get()
        details_first_section_keys = Selector(text=details_first_section).xpath("//dt/text()").getall()
        details_first_section_values = Selector(text=details_first_section).xpath("//dd/text()").getall()
        for k in range(len(details_first_section_keys)):
            key = details_first_section_keys[k]
            value = details_first_section_values[k]
            if key == "Body Type":
                output["body_type"] = value
            elif key == "Type":
                if value == "Used":
                    output["is_used"] = 1
                else:
                    output["is_used"] = 0
            elif key == "Drivetrain":
                output["drive_train"] = value
            elif key == "Seats":
                output["seats"] = int(value)
            elif key == "Doors":
                output["doors"] = int(value)

        details_second_section = response.xpath("//div[@data-cy='listing-history-section']//dl").get()
        details_second_section_keys = Selector(text=details_second_section).xpath("//dt//text()").getall()
        details_second_section_values = Selector(text=details_second_section).xpath("//dd//text()").getall()
        for k in range(len(details_second_section_keys)):
            key = details_second_section_keys[k]
            value = details_second_section_values[k]
            if key == "Mileage":
                o_value = value.split(" ")[0].replace(",", "")
                if o_value.isnumeric():
                    output["odometer_value"] = int(o_value)
                output["odometer_unit"] = value.split(" ")[1]
            elif key == "First registration":
                output["registration_year"] = int(value.split("/")[1])

        details_third_section = response.xpath("//div[@data-cy='technical-details-section']//dl").get()
        details_third_section_keys = Selector(text=details_third_section).xpath("//dt//text()").getall()
        details_third_section_values = Selector(text=details_third_section).xpath("//dd//text()").getall()
        for k in range(len(details_third_section_keys)):
            key = details_third_section_keys[k]
            value = details_third_section_values[k]
            if key == "Cylinders":
                if value.isnumeric():
                    output["engine_cylinders"] = int(value)
            elif key == "Gearbox":
                output["transmission"] = value
            elif key == "Engine Size":
                ed_value = value.split(" ")[0].replace(",", "")
                if ed_value.isnumeric():
                    output["engine_displacement_value"] = ed_value
                output["engine_displacement_unit"] = value.split(" ")[1]

        details_color_section = response.xpath("//div[@data-cy='color-section']//dl").get()
        details_color_section_keys = Selector(text=details_color_section).xpath("//dt//text()").getall()
        details_color_section_values = Selector(text=details_color_section).xpath("//dd//text()").getall()
        for k in range(len(details_color_section_keys)):
            key = details_color_section_keys[k]
            value = details_color_section_values[k]
            if key == "Colour":
                output["exterior_color"] = value
            elif key == "Upholstery colour":
                output["interior_color"] = value
            elif key == "Upholstery":
                output["upholstery"] = value

        make = response.xpath("//div[@class='StageTitle_makeModelContainer__WPHjg']/span[1]/text()").get()
        model = response.xpath("//div[@class='StageTitle_makeModelContainer__WPHjg']/span[2]/text()").get()
        output["make"] = make
        output["model"] = model

        city = response.xpath("//a[@class='scr-link LocationWithPin_locationItem__pHhCa']//text()").get()
        output["country"] = "DE"
        output["city"] = city.split(",")[0]

        price = response.xpath("//span[@class='PriceInfo_price__JPzpT']//text()").get()
        if price is not None:
            output["price_retail"] = float(price.replace("-", "").replace("€", "").replace(",", ".").replace(".", "").strip())
            output["currency"] = 'EUR'

        pictures_list = response.xpath("//img[@class='image-gallery-thumbnail-image']/@src").getall()
        if pictures_list is not None:
            output['picture_list'] = json.dumps(pictures_list)

        apify.pushData(output)

