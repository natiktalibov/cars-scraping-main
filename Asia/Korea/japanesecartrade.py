import datetime
import json
import scrapy
import apify


class JpcartradeSpider(scrapy.Spider):
    name = "jpcartrade"
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def start_requests(self):
        urls = [
            "https://www.japanesecartrade.com/stock_list.php?make_id=&maker_id=&mfg_from=&month_from=&mfg_to=&month_to=&fuel_id=&seat_capacity=&transmission_id=&type_id=&subtype_id=&drive=&mileage_from=&mileage_to=&price_from=&price_to=&cc_from=&cc_to=&wheel_drive=&color_id=&stock_country=south+korea&search_keyword=&SA=make&isSearched=1&sort=&desksearch=desksearch&seq="
        ]

        for url in urls:
            country = ""
            country_name = url.split("stock_country=")[1]
            if "uae" in country_name:
                country = "AE"
            elif "united" in country_name:
                country = "GB"
            elif "korea" in country_name:
                country = "KR"
            elif "japan" in country_name:
                country = "JP"
            elif "thailand" in country_name:
                country = "TH"
            elif "singapore" in country_name:
                country = "SG"
            elif "kenya" in country_name:
                country = "KE"

            yield scrapy.Request(
                url=url,
                meta={"country": country},
                callback=self.parse,
            )

    def parse(self, response):
        # pagination
        total_records = int(
            response.xpath('//strong[@class="ttlNowRecords"]/text()').get().replace(",", "")
        )
        total_pages = int(total_records / 10)

        for page_number in range(0, total_pages + 1):
            link = response.url + "&page=" + str(page_number)
            yield response.follow(
                link,
                meta={"country": response.meta["country"]},
                callback=self.traverse_product_links,
            )

    def traverse_product_links(self, response):
        # Traverse product links
        product_links = response.xpath('//h2[@class="list_head"]/a/@href').getall()
        yield from response.follow_all(
            product_links,
            meta={"country": response.meta["country"]},
            callback=self.detail,
        )

    def detail(self, response):
        output = dict()

        output["ac_installed"] = 0
        output["tpms_installed"] = 0

        # scraping info
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "JapaneseCarTrade"
        output["vehicle_url"] = response.url

        # pictures list
        picture_list = response.xpath("//img[@id='main_photo']/@src").get()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        # location details
        output["country"] = response.meta["country"]
        location = response.xpath(
            "//div[@class='inner_common_box']/h4/text()"
        ).get()

        if len(location.split(",")) > 1:
            output["city"] = location.split(",")[1]

        vehicle_details = response.xpath("//div[@class='listing-detail-box specification']/ul/li")
        for detail in vehicle_details:
            key = detail.xpath("./span/text()").get()
            value = detail.xpath("./strong/text()").get()

            if key and value != "--":
                if "Mfg. Year/Month" in key:
                    year = value.split("/")[0]
                    if year.isdigit():
                        output["year"] = year
                elif "Reg. Year/Month" in key:
                    year = value.split("/")[0]
                    if year.isdigit():
                        output["registration_year"] = year
                elif "JCT Ref. ID" in key:
                    output["scraped_listing_id"] = value.split("-")[1]
                elif "Transmission" in key:
                    output["transmission"] = value
                elif "Make" in key:
                    output["make"] = value
                elif "Model" in key:
                    output["model"] = value
                elif "Chassis Number" in key:
                    output["chassis_number"] = value
                elif "Engine" in key:
                    output["engine_displacement_value"] = value.split(" ")[0]
                    output["engine_displacement_units"] = value.split(" ")[1]
                elif "Fuel" in key:
                    output["fuel"] = value
                elif "Mileage" in key:
                    output["odometer_value"] = int(value.split(" ")[0].replace(",", ""))
                    output["odometer_unit"] = value.split(" ")[1]
                elif "Drive" == key:
                    output["steering_position"] = value
                elif "Type" == key:
                    output["body_type"] = value
                elif "Seat Capacity" in key:
                    if value.isnumeric():
                        output["seats"] = int(value)
                elif "Doors" in key:
                    if value.isnumeric():
                        output["doors"] = int(value)
                elif "Wheel Drive" in key:
                    output["drive_train"] = value
                elif "Exterior Color" in key:
                    output["exterior_color"] = value
                elif key == "Version/Class":
                    output["trim"] = value

        # price details
        price = response.xpath("//div[@class='fob_price']/span/strong/text()").get()
        price = "".join([i for i in list(price) if i.isdigit()])
        if price:
            output["price_retail"] = float(price)
            output["currency"] = "USD"

        # vehicle options
        vehicle_options = response.xpath(
            "//ul[@class='detail_acc_fea']/li/text()"
        ).getall()
        rest_vehicle_options = []
        for option in vehicle_options:
            option = option.lower()
            if (
                    option == "air condition"
                    or option == "auto-air condition"
                    or option == "ac"
                    or option == "aac"
                    or option == "a/c-front"
                    or option == "a/c-rear"
            ):
                output["ac_installed"] = 1
            elif option == "leather seats" or option == "leather seat":
                output["upholstery"] = "leather"
            else:
                rest_vehicle_options.append(option)

        ##### By SK : DM-811 : data quality : get all unique available options
        vehicle_options_unique_list = []
        for option in rest_vehicle_options:
            # We get same option twice if we directly dumps the "vehicle_options" so need to add below condition
            if option not in vehicle_options_unique_list:
                vehicle_options_unique_list.append(option)

        output["vehicle_options"] = json.dumps(vehicle_options_unique_list)
        ##### END

        if len(vehicle_options_unique_list) > 0:
            output["vehicle_options"] = ", ".join(vehicle_options_unique_list)

        description = response.xpath(
            "//h2[@class='detail_fault']/parent::div[@class='common_box']/div/text()"
        ).get()
        if description is not None:
            output["vehicle_disclosure"] = description

        apify.pushData(output)
