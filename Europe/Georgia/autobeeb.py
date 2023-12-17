import re
import json
import apify
import scrapy
import datetime


class AutobeebSpider(scrapy.Spider):
    name = 'autobeeb'
    start_urls = ['https://autobeeb.com/en-ge/sl/cars-in-georgia/1-1?pageNum=1']
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def parse(self, response):

        link_list = response.xpath("//div[@id='result-cards']/div[1]//div[@class='image']/a/@href").getall()
        link_list = ["https://autobeeb.com" + i for i in link_list]

        # yield from response.follow_all(link_list, self.product_detail)
        for link in link_list:
            yield scrapy.Request(link, self.product_detail)

        next_button = response.xpath("//li[@class='next  mobile-last']/a/@onclick").get()
        if next_button:
            next_num = re.findall("SetPage\\((.*?)\\)", next_button, re.S)[0]
            next_link = response.url.split("pagenum=")[0] + "pagenum=" + next_num
            yield response.follow(next_link, self.parse)

    def product_detail(self, response):
        output = {}

        form_data = response.xpath("//div[@class='col-sm-4 col-md-4 col-xs-6 no-padding']")
        for data in form_data:
            key = data.xpath("./span[1]/text()").get()
            value = data.xpath("./span[2]/text()").get()
            if "Make" in key:
                output["make"] = value.strip()
            elif "Model" in key:
                output["model"] = value.strip()
            elif "Year" in key:
                output["year"] = int(value.strip())
            elif "Gearbox" in key:
                output["transmission"] = value.strip()
            elif "Fuel Type" in key:
                output["fuel"] = value.strip()
            elif "Mileage" in key:
                output["odometer_value"] = int(value.strip().split(" ")[0].replace(",", ""))
                output["odometer_unit"] = value.strip().split(" ")[1]
            elif "City" in key:
                output["city"] = value.strip()
            elif "Status" in key: ## by NT
                output["is_used"] = value.strip() ## by NT
            elif "Category" in key:
                output["body_type"] = value.strip() ## by NT

        output["ac_installed"] = 0
        output["tpms_installed"] = 0
        output["scraped_date"] = datetime.datetime.isoformat(datetime.datetime.today())
        output["scraped_from"] = "Autobeeb"
        output["scraped_listing_id"] = response.url.split("/")[5]
        output["vehicle_url"] = response.url
        output["country"] = "GE"
        price = response.xpath("//span[@class='c-red']/span/span/text()").get()
        if price:
            price_retail = "".join([i for i in list(price.strip()) if i.isdigit() or i == "."])
            if price_retail:
                output["price_retail"] = float(price_retail)
        output["currency"] = "USD"

        picture_list = response.xpath("//div[@id='single-slider']/ul/li/a/img/@src").getall()
        if picture_list:
            output["picture_list"] = json.dumps(picture_list)

        # process empty fields
        list1 = []
        list2 = []
        for k, v in output.items():
            if v or v == 0:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))
        yield apify.pushData(output)
        # yield output
