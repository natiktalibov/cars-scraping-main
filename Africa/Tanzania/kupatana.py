import re
import json
import scrapy
import datetime

import apify


class KupatanaSpider(scrapy.Spider):
    name = 'kupatana'
    download_timeout = 120
    start_urls = ['https://kupatana.com/tz/search/cars?page=1']
    detail_link = []

    def parse(self, response):
        link_list = response.xpath('//div[@class="product-list__item "]/a/@href').getall()
        link_list = ["https://kupatana.com" + i for i in link_list]

        if link_list and link_list != self.detail_link:
            yield from response.follow_all(link_list, self.product_detail)
            self.detail_link = link_list
            next_link = response.url.split("page=")[0] + "page=" + str(int(response.url.split("page=")[1]) + 1)
            yield response.follow(next_link, self.parse)

    def product_detail(self, response):
        output = {}

        # Vehicle information may be in the form
        form_data = response.xpath("//div[@class='product-details__attributes']/div")
        for data in form_data:
            key = data.xpath('./div[1]/text()').get()
            value = data.xpath('./div[2]/text()').get()
            if "null" in value:
                continue
            if "Make" in key:
                output['make'] = value.strip()
            elif "Model" in key:
                output['model'] = value.strip()
            elif "Year" in key:
                output['year'] = int(value.strip())
            elif "Transmission" in key:
                output['transmission'] = value.strip()
            elif "Mileage" in key:
                output['odometer_value'] = int(value.split(" ")[0])
                output['odometer_unit'] = value.split(" ")[1]

        # Vehicle information may be on the label
        detail_tag = response.xpath('//div[@class="ant-tag custom-green-tag product-details__tag"]')
        for tag in detail_tag:
            key = "".join(tag.xpath('./p[1]//text()').getall())
            value = tag.xpath('./p[2]/text()').get()
            if "null" in value:
                continue
            if "Make" in key or "Brand" in key and output.get("make", '') == '':
                output['make'] = value.strip()
            elif "Model" in key and output.get("model", '') == '':
                output['model'] = value.strip()
            elif "Year" in key and output.get("year", '') == '':
                output['year'] = int(value.strip())
            elif "Transmission" in key and output.get("transmission", '') == '':
                output['transmission'] = value.strip()
            elif "Mileage" in key and output.get("odometer_value", '') == '':
                output['odometer_value'] = int(value.split(" ")[0])
                output['odometer_unit'] = "km"

        # If the above two methods do not get data, Parse XXX and xxx from the title
        if not output.get('make') or not output.get('model'):
            # Get the brand of all vehicles
            cars_brand_data = re.findall("__NEXT_DATA__ = (.*?)</script>", response.text, re.S)[0].split("module={}")[0]
            cars_brand = ''.join(cars_brand_data.split())
            cars_brand = json.loads(cars_brand)
            all_brand = cars_brand["props"]["initialState"]["categories"]["topCategories"]["entities"]["byId"]["05"][
                "subCategories"][0].get("brands")
            # Parse the required data in the title
            title = response.xpath("//h1[@class='product-details__title']/text()").get()

            # Match "make" and "model"
            for i in all_brand:
                if len(i["name"]) >= 3 and i["name"].lower() in title.lower():
                    if not output.get("make"):
                        output["make"] = i["name"]
                    for j in i["models"]:
                        if j.lower() in title.lower() and not output.get("model"):
                            output["model"] = j

        output['ac_installed'] = 0
        output['tpms_installed'] = 0
        output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
        output['scraped_from'] = 'kupatana'
        output['scraped_listing_id'] = response.url.split("/")[-1]
        output['vehicle_url'] = response.url
        output['country'] = "TZ"
        city = response.xpath('//span[@class="product-details__location"]/text()').get()
        if city:
            output['city'] = city.split(",")[0]
        price_data = response.xpath('//h2[@class="product-details__price"]/text()').get()
        price = "".join([i for i in list(price_data) if i.isdigit()])
        if price:
            output['price_retail'] = float(price)
            output['currency'] = "".join([i for i in list(price_data) if i.isalpha()])
        picture_list = response.xpath('//div[@class="image-gallery-slides"]/div/img/@src').getall()
        if picture_list:
            output['picture_list'] = json.dumps(picture_list)

        # process empty fields
        list1 = []
        list2 = []
        for k, v in output.items():
            if v or v == 0:
                list1.append(k)
                list2.append(v)
        output = dict(zip(list1, list2))

        apify.pushData(output)
