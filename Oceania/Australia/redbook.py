import datetime
import re
import json
import scrapy
from scrapy.selector import Selector
import apify


class RedbookSpider(scrapy.Spider):
    name = "Redbook"
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    start_urls = [
        "https://www.redbook.com.au/cars/results/?q=(And.Service.redbook._.RecordType.car._.CountryCode.AU.)&offset=0",
    ]

    def parse(self, response):
        # Traverse product links
        product_links = response.xpath(
            '//a[@class="btn btn-primary js-encode-search"]/@href'
        ).getall()
        yield from response.follow_all(
            product_links,
            callback=self.detail,
        )

        # pagination
        page_link = response.xpath('//a[@class="page-link next "]/@href').get()
        if page_link is not None:
            yield response.follow(
                "https://www.redbook.com.au" + page_link,
                callback=self.parse,
            )

    def detail(self, response):
        output = {}

        output["ac_installed"] = 0
        output["tpms_installed"] = 0

        # scraping info
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Redbook"
        output["scraped_listing_id"] = response.url.split("/")[-1].split("?")[0]
        output["vehicle_url"] = response.url

        # location
        output["country"] = "AU"

        pictures = []
        main_img = response.xpath("//img[@class='img-fluid thumb-big']/@src").get()
        if main_img != "?height=600&padcolor=ffffff&aspect=FitWithin&width=900":
            pictures.append(main_img)

        additional_imgs = response.xpath("//div[@class='thumb-small']/@style").getall()
        for image in additional_imgs:
            res = re.findall(r"\(.*?\)", image)
            pictures.append(res[0].replace("(", "").replace(")", ""))

        if len(pictures) > 0:
            output["picture_list"] = json.dumps(pictures)

        # basic info (make, model, year)
        script = response.xpath("//section[@class='gallery']//script/text()").get()
        meta_data = (
            script.replace("var", "")
            .replace("gallery_meta", "")
            .replace("=", "")
            .strip()
        )
        if meta_data is not None:
            meta_data = json.loads(meta_data[:-1])
            if "make" in meta_data:
                output["make"] = meta_data["make"]
            if "model" in meta_data:
                output["model"] = meta_data["model"]
            if "year" in meta_data:
                output["year"] = meta_data["year"]

        # exterior color
        colors = response.xpath("//span[@class='style-description']//text()").getall()
        filtered_colors = []
        for color in colors:
            filtered_colors.append(color.strip().split("-")[0].strip())

        output["exterior_color"] = ", ".join(filtered_colors)

        # car details (fuel, transmission, engine, seats, doors, etc.)
        vehicle_details = response.xpath(
            "//div[contains(@class, 'row') and contains(@class, 'features-item')]"
        ).getall()

        for k in range(len(vehicle_details)):
            key = (
                Selector(text=vehicle_details[k])
                .xpath("//div[@class='col-4 features-item-name']/span//text()")
                .get()
            )
            value = (
                Selector(text=vehicle_details[k])
                .xpath(
                    "//div[contains(@class, 'col') and contains(@class, 'features-item-value')]/text()"
                )
                .get()
            )
            if key is not None and value is not None:
                key = key.strip().lower()
                value = value.strip().lower()

                if key == "price when new":
                    output["price_retail"] = float(
                        value.replace("$", "").replace(",", "")
                    )
                    output["currency"] = "AUD"
                elif key == "body":
                    output["body_type"] = value
                elif key == "badge":
                    if value != "(no badge)":
                        output["trim"] = value
                elif key == "no. doors":
                    output["doors"] = int(value)
                elif key == "seat capacity":
                    output["seats"] = int(value)
                elif key == "transmission":
                    output["transmission"] = value
                elif key == "drive":
                    output["drive_train"] = value
                elif key == "country of origin":
                    output["country_of_manufacture"] = value
                elif key == "fuel type":
                    output["fuel"] = value
                elif key == "engine type":
                    output["engine_block_type"] = value
                elif key == "engine size (cc)":
                    output["engine_displacement_value"] = value.replace(
                        "cc", ""
                    ).strip()
                    output["engine_displacement_units"] = "cc"
                elif key == "cylinders":
                    output["engine_cylinders"] = int(value)

        apify.pushData(output)
