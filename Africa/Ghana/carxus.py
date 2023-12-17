import re
import json
import datetime
import scrapy
from scrapy import Request, Selector
import apify


class CarxusSpider(scrapy.Spider):
    name = "carxus"
    allowed_domains = ["www.carxus.com"]
    start_urls = ["https://www.carxus.com/en/Inventory/Search?country=78"]

    def start_requests(self):
        for url in self.start_urls:
            country = ""
            if url == "https://www.carxus.com/en/Inventory/Search?country=1":
                country = "US"
            elif url == "https://www.carxus.com/en/Inventory/Search?country=78":
                country = "GH"
            elif url == "https://www.carxus.com/en/Inventory/Search?country=151":
                country = "NG"
            yield Request(url=url, meta={"country": country}, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        # get data
        div_list = sel.css("div#id_FoundVehiclesList div.c_SearchCar")
        for div in div_list:
            url = (
                "https://"
                + self.allowed_domains[0]
                + div.css("div.c_PhotoContainer a::attr(href)").get()
            )
            yield Request(
                url=url,
                meta={"vehicle_url": url, "country": response.meta["country"]},
                callback=self.get_data,
            )

        # next page
        next_url = sel.css(
            "div.c_PagerBottom table td.c_Pager_Next a::attr(href)"
        ).get()
        if next_url is not None:
            url = "https://" + self.allowed_domains[0] + next_url
            yield Request(
                url=url, meta={"country": response.meta["country"]}, callback=self.parse
            )

    def get_data(self, response):
        sel = Selector(response)
        output = {}

        # scraping info
        output["vehicle_url"] = str(response.meta["vehicle_url"])
        output["scraped_listing_id"] = (
            response.meta["vehicle_url"].split("/")[-1].split("-")[-1]
        )
        output["scraped_from"] = "carxus.com"
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())

        output["country"] = response.meta["country"]
        output["ac_installed"] = 0
        output["tpms_installed"] = 0

        # get image
        image_list = sel.css("div.veh-images-small-container a::attr(href)").getall()
        if image_list != []:
            output["picture_list"] = json.dumps(
                [
                    "https://" + self.allowed_domains[0] + image_path
                    for image_path in image_list
                ]
            )

        titles = sel.css("div.veh-main-header span::text").getall()
        front_title = titles[0].split(" -")[0].split(" ")
        output["year"] = int(front_title[0])
        output["make"] = front_title[1]
        output["model"] = " ".join(front_title[2:]).strip()

        # pricing details
        price_field = sel.xpath('//div[@class="veh-details-price-container"]/div')
        price = price_field.xpath("./span[2]/text()").get().replace(",", "")
        currency = price_field.xpath("./span[1]/text()").get()
        if price is not None:
            output["price_retail"] = float(price)
            output["currency"] = currency

        # dealer name
        seller_info = (
            sel.xpath('//div[@class="veh-tech-data-header"]/text()').get().strip()
        )
        if seller_info is not None:
            if "dealer" in seller_info.strip().lower():
                dealer_info = sel.xpath(
                    '//div[@class="veh-details-text-data-container"]//table//tr[1]/td/span/text()'
                ).get()
                output["dealer_name"] = dealer_info

        # vehicle specifications
        keys = [
            x.replace(":", "").lower()
            for x in response.css(
                "div.veh-details-container div.veh-details-text-data-container span.veh-details-title::text"
            ).getall()
        ]

        values = [
            x.replace("\r", "").replace("\n", "")
            for x in sel.css(
                "div.veh-details-container div.veh-details-text-data-container span.veh-details-value"
            ).getall()
        ]
        values = [re.findall(">(.*)<", x)[0].strip() for x in values]

        for key, value in zip(keys, values):
            if (
                value != "No Data"
                and value != ""
                and value != "Unspecified"
                and value != "Unknown"
                and value != "Optional"
                and "Other" not in value
            ):
                if key == "location":
                    location_value = value.split(",")
                    if len(location_value) == 2:
                        if output["country"].lower() == "us":
                            output["state_or_province"] = location_value[1].strip()
                        else:
                            output["city"] = location_value[1].strip()

                elif key == "mileage":
                    output["odometer_value"] = int(
                        re.findall(r"\d*", value)[0].replace(",", "")
                    )
                    output["odometer_unit"] = "".join(re.findall(r"[a-zA-Z]", value))

                elif key == "vin":
                    output["vin"] = value

                elif key == "fuel":
                    output["fuel"] = value

                elif key == "body style":
                    output["body_type"] = value

                elif key == "transmission":
                    output["transmission"] = value

                elif key == "other ext. color":
                    output["exterior_color"] = value

                elif key == "color":
                    output["exterior_color"] = value

                elif key == "interior color":
                    output["interior_color"] = value

                elif key == "interior type":
                    output["upholstery"] = value

                elif key == "drivetrain":
                    output["drive_train"] = value

                elif key == "engine":
                    engine_value = value
                    contains_engine_value = re.search(
                        "([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*))(?:[eE]([+-]?\d+))?L",
                        engine_value,
                    )
                    if contains_engine_value:
                        output[
                            "engine_displacement_value"
                        ] = contains_engine_value.group(1)
                        output["engine_displacement_units"] = "L"

                        engine_cylinders = engine_value.replace(
                            f"{contains_engine_value.group(1)}L", ""
                        ).strip()

                        if "cylinder" in engine_cylinders.lower():
                            engine_cylinders_value = filter(
                                lambda char: char.isnumeric(), list(engine_cylinders)
                            )
                            output["engine_cylinders"] = int(
                                list(engine_cylinders_value)[0]
                            )
                    else:
                        contains_engine_value_cc = re.search(
                            "([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*))(?:[eE]([+-]?\d+))?CC",
                            engine_value,
                        )
                        if contains_engine_value_cc:
                            output[
                                "engine_displacement_value"
                            ] = contains_engine_value_cc.group(1)
                            output["engine_displacement_units"] = "CC"

                        if "cylinder" in engine_value.lower():
                            engine_cylinders_value = filter(
                                lambda char: char.isnumeric(), list(engine_value)
                            )
                            output["engine_cylinders"] = int(
                                list(engine_cylinders_value)[0]
                            )

        apify.pushData(output)