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
            "https://www.japanesecartrade.com/stock_list.php?make_id=&maker_id=&mfg_from=&month_from=&mfg_to=&month_to=&fuel_id=&seat_capacity=&transmission_id=&type_id=&subtype_id=&drive=&mileage_from=&mileage_to=&price_from=&price_to=&cc_from=&cc_to=&wheel_drive=&color_id=&stock_country=japan&search_keyword=&SA=make&isSearched=1&sort=&desksearch=desksearch&seq="
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
            response.xpath('//strong[@class="ttlNowRecords"]/text()')
            .get()
            .replace(",", "")
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
        output = {}
        data_keys = response.xpath('//div[@class="listing-detail-box specification"]/ul/li/span/text()').getall()
        data_values = response.xpath('//div[@class="listing-detail-box specification"]/ul/li/strong/text()').getall()
        for i in range(len(data_keys)):
            key = data_keys[i].lower()
            if key == "make":
                output["make"] = data_values[i]
            if key == "model":
                output["model"] = data_values[i]
            ##### By SK: DM-811 : Data quality
            if key == "version/class":
                output["trim"] = data_values[i]
            #### END
            if "chassis" in key:
                output["chassis_number"] = data_values[i]
            if key == "type":
                output["body_type"] = data_values[i]
            if key == "transmission":
                output["transmission"] = data_values[i]
            if key == "color":
                output["exterior_color"] = data_values[i]
            if "engine" in key:
                output["engine_displacement_value"] = data_values[i].split(" ")[0]
                try:
                    output["engine_displacement_units"] = data_values[i].split(" ")[1]
                except:
                    pass
            if "fuel" in key:
                output["fuel"] = data_values[i]
            if "door" in key:
                try:
                    output["doors"] = int(data_values[i])
                except ValueError:
                    pass
            if "seat" in key:
                if "[" in data_values[i]:
                    output["seats"] = int(data_values[i].split("[")[0])
                if "(" in data_values[i]:
                    output["seats"] = int(data_values[i].split("(")[0])
                if data_values[i].isdigit():
                    output["seats"] = int(data_values[i])
            if key == "wheel drive":
                output["drive_train"] = data_values[i]
            if "jct" in key:
                output["scraped_listing_id"] = data_values[i].split("-")[1]
            if "mfg" in key:
                year = data_values[i].split("/")[0]
                if year.isdigit():
                    output["year"] = year
            if "reg" in key:
                year = data_values[i].split("/")[0]
                if year.isdigit():
                    output["registration_year"] = year

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
            "//div[@class='dtl_sec_head']/div[@class='hidden-xs']/text()"
        ).get()

        try:
            if len(location.split(",")) > 1:
                output["city"] = location.split(",")[1]
        except AttributeError:
            pass

        vehicle_details = response.xpath("//div[contains(@class, 'dtl_main_spec')]/div")
        for detail in vehicle_details:
            key = detail.xpath("./span/@aria-label").get()
            value = detail.xpath("./text()").get()
            if key:
                if "Year" in key:
                    year = value.split("/")[0]
                    if year.isdigit():
                        output["registration_year"] = year
                elif "Transmission" in key:
                    output["transmission"] = value
                elif "Engine" in key:
                    try:
                        output["engine_displacement_value"] = value.split(" ")[0]
                        output["engine_displacement_units"] = value.split(" ")[1]
                    except IndexError:
                        # Handle the IndexError exception
                        output["engine_displacement_units"] = ""  # Set a default value
                elif "Fuel" in key:
                    output["fuel"] = value
                elif "Mileage" in key:
                    try:
                        output["odometer_value"] = int(value.split(" ")[0].replace(",", ""))
                    except ValueError:
                        pass
                    try:
                        output["odometer_unit"] = value.split(" ")[1]
                    except IndexError:
                        pass
                elif "Steering" in key:
                    output["steering_position"] = value

        # price details
        # price details
        price = response.xpath("//div[@class='fob_price']//strong/text()").getall()
        price = "\n".join(price[1:])
        if "ASK" in price:
            # Handle the "ASK" price case
            output["price_retail"] = "N/A"  # Set a default value
        else:
            # Convert the price string to an integer
            try:
                price = int(price.replace(',', ''))
                output["price_retail"] = float(price)
            except ValueError:
                pass
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

        # if len(rest_vehicle_options) > 0:
        # output["vehicle_options"] = ", ".join(rest_vehicle_options)

        description = response.xpath(
            "//h2[@class='detail_fault']/parent::div[@class='common_box']/div/text()"
        ).get()
        if description is not None:
            output["vehicle_disclosure"] = description

        ##### By SK : DM-811 : data quality : get all unique available options 
        vehicle_options_unique_list = [] 
        for option in vehicle_options:
            # We get same option twice if we directly dumps the "vehicle_options" so need to add below condition
            if option not in vehicle_options_unique_list:
                vehicle_options_unique_list.append(option)

        output["vehicle_options"] = json.dumps(vehicle_options_unique_list)
        ##### END

        apify.pushData(output)
