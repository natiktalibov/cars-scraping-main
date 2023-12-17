import json
import scrapy
import datetime
import html
import apify


class Kavak(scrapy.Spider):
    name = "kavak"
    download_timeout = 120
    url = "https://www.kavak.com/tr/page-1/satilik-araba"

    def start_requests(self):
        yield scrapy.Request(self.url, callback=self.parse_main_page)

    def parse_main_page(self, response):
        initial_url = "https://www.kavak.com/"
        country = self.url.split("/")[3]
        colors = response.xpath("//app-facet-color/a/text()").getall()
        body_types = response.xpath("//app-facet-type/a/text()").getall()
        page_links = []

        for color in colors:
            for body_type in body_types:
                page_links.append(
                    initial_url
                    + country
                    + "/kasa-"
                    + body_type.lower()
                    + "/renk-"
                    + color.replace("ü", "u")
                    .replace("ş", "s")
                    .replace("ğ", "g")
                    .replace(" ", "_")
                    .lower()
                    + f"/page-1/"
                    + self.url.split("/")[-1]
                )

        for page_link in page_links:
            yield scrapy.Request(page_link, callback=self.parse)

    def parse(self, response):
        url = response.url
        country = url.split("/")[3]
        body_type = url.split("/")[4].split("-")[1]
        color = url.split("/")[5].split("-")[1]

        # traverse vehicle links
        product_links = response.xpath("//a[@class='card-inner']/@href").getall()
        for link in product_links:
            if link.split("-")[-1]:
                href = (
                    "https://api.kavak.services/services-common/inventory/"
                    + link.split("-")[-1]
                    + "/static"
                )
                yield response.follow(
                    href,
                    callback=self.detail,
                    cb_kwargs={
                        "country": country,
                        "color": color,
                        "body_type": body_type,
                    },
                )

        current_page = int(url.split("/")[-2].split("-")[1])
        next_page = response.xpath("//div[@class='results']/span[3]/text()").get()
        if next_page is not None and current_page < int(next_page):
            next_page = url.replace(f"page-{current_page}", f"page-{current_page+1}")
            yield response.follow(next_page, callback=self.parse)

    def detail(self, response, country, color, body_type):
        output = {}

        # country map
        ctry_map = {
            "mx": "MX",
            "br": "BR",
            "ar": "AR",
            "tr": "TR",
            "co": "CO",
            "cl": "CL",
            "pe": "PE",
        }

        # currency codes map
        curr_map = {
            "mx": "MXN",
            "br": "BRL",
            "ar": "ARS",
            "tr": "TRY",
            "co": "COP",
            "cl": "CLP",
            "pe": "PEN",
        }
        jsn = json.loads(html.unescape(response.body.decode()))
        data = jsn["data"]

        # body type, exterior color
        if data["exteriorColor"] is not None:
            output["exterior_color"] = color.replace("_", " ")
        if data["bodyType"] is not None:
            output["body_type"] = data["bodyType"]

        if "exterior_color" not in output:
            output["exterior_color"] = color
        if "body_type" not in output:
            output["body_type"] = body_type

        # engine details
        if "mainAccessories" in data["features"]:
            for feature in data["features"]["mainAccessories"]["items"]:
                if feature["name"] == "Litros":
                    output["engine_displacement_value"] = feature["value"]
                    output["engine_displacement_units"] = "L"

        # vehicle basic information
        output["make"] = data["make"]
        output["model"] = data["model"]
        output["year"] = data["carYear"]
        output["trim"] = data["trim"]
        output["transmission"] = data["transmission"]
        output["tpms_installed"] = 0
        output["ac_installed"] = 0

        # scraping details
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Kavak"
        output["scraped_listing_id"] = str(data["id"])
        output["vehicle_url"] = "https://www.kavak.com/" + country + data["carUrl"]

        # odometer details
        output["odometer_value"] = int(data["km"])
        if output.get("odometer_value"):
            output["odometer_unit"] = "km"

        # number of doors, seats, upholstery, ac_installed
        other_accessories = data["features"]["otherAccessories"]
        for feature in other_accessories:
            categories = feature["categories"]
            if "Dış" in feature["name"]:
                for category in categories:
                    if category["name"] == "Kapılar":
                        for item in category["items"]:
                            if item["code"] == "number_doors":
                                output["doors"] = int(item["value"])

            if feature["name"] == "İç":
                for category in categories:
                    if category["name"] == "Koltuk":
                        for item in category["items"]:
                            if item["code"] == "seats_material":
                                output["upholstery"] = item["value"]

        # pictures list
        medias = [media["media"] for media in data["media"]["inventoryMedia"]]
        medias.extend([media["media"] for media in data["media"]["internalDimples"]])
        medias.extend([media["media"] for media in data["media"]["externalDimples"]])
        output["picture_list"] = json.dumps(
            ["https://images.kavak.services/" + media for media in medias]
        )

        # location details
        output["city"] = data["region"]["name"]
        output["country"] = ctry_map[country]

        # price details
        output["price_retail"] = float(data["price"])
        output["currency"] = curr_map[country]

        apify.pushData(output)


"""
word "equipment" mapping
    equipment_map = {
        "mx": "Equipamiento y Confort",
        "br": "Equipamento e Conforto",
        "ar": "Equipamiento",
        "tr": "-",
        "co": "Equipamiento",
        "cl": "Equipamiento",
        "pe": "Equipamiento",
    }

word "seating" mapping
    seating_map = {
        "mx": "Asientos",
        "br": "Assentos",
        "ar": "Tapizado",
        "tr": "Koltuk",
        "co": "Asientos",
        "cl": "Asientos",
        "pe": "Asientos",
    }

word "doors" mapping
    doors_map = {
        "mx": "Puertas",
        "br": "Portas",
        "ar": "Puertas",
        "tr": "Kapılar",
        "co": "Puertas",
        "cl": "Puertas",
        "pe": "Puertas",
    }

word "color" mapping
    doors_map = {
        "mx": "color",
        "br": "cor",
        "ar": "color",
        "tr": "renk",
        "co": "color",
        "cl": "color",
        "pe": "color",
    }
"""
