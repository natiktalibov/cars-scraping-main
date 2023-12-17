import datetime
import json
import scrapy
from urllib.parse import urlencode
import apify


class CarnationcanadaSpider(scrapy.Spider):
    name = 'Carnationcanada'
    download_timeout = 120
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    q_params = {
        "action": "vms_data",
        "endpoint": "https://vms.prod.convertus.rocks/api/filtering/?cp=539&ln=en&pg=1&pc=15&dc=true&qs=&im=&svs=&sc=all&v1=Passenger%20Vehicles&st=make%2Casc&ai=true&oem=&dp=&in_transit=true&in_stock=true&on_order=true&mk=&md=&tr=&bs=&tm=&dt=&cy=&ec=&mc=&ic=&pa=&ft=&eg=&v2=&v3=&fp=&fc=&fn=&tg=",
    }
    encode_q_params = urlencode(q_params)
    url = f'https://www.carnationcanada.com/wp-content/plugins/convertus-vms/include/php/ajax-vehicles.php?{encode_q_params}'

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, headers={'Content-Type': 'application/json'}, cb_kwargs={'page': 1})

    def parse(self, response, page):
        json_data = response.json()
        results = json_data["results"]
        if len(results) > 0:
            for item in results:
                output = dict()

                output['ac_installed'] = 0
                output['tpms_installed'] = 0

                output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
                output['scraped_from'] = 'Carnationcanada'
                output['scraped_listing_id'] = item["ad_id"]
                output["country"] = "CA"
                output["make"] = item["make"]
                output["model"] = item["model"]

                if "trim" in item and item["trim"] is not None and item["trim"] != "":
                    output["trim"] = item["trim"]
                if "asking_price" in item and item["asking_price"] is not None and isinstance(item["asking_price"], int):
                    output["price_retail"] = float(item["asking_price"])
                    output["currency"] = "CAD"
                if "body_style" in item and item["body_style"] is not None and item["body_style"] != "":
                    output["body_type"] = item["body_style"]
                if "cylinder" in item and item["cylinder"] is not None:
                    output["engine_cylinders"] = int(item["cylinder"])
                if "doors" in item and item["doors"] is not None:
                    output["doors"] = int(item["doors"])
                if "drive_train" in item and item["drive_train"] is not None and item["drive_train"] != "":
                    output["drive_train"] = item["drive_train"]
                if "engine_displacement" in item and item["engine_displacement"] is not None and item["engine_displacement"] != "":
                    output["engine_displacement_value"] = item["engine_displacement"]
                if "exterior_color" in item and item["exterior_color"] is not None and item["exterior_color"] != "":
                    output["exterior_color"] = item["exterior_color"]
                if "interior_color" in item and item["interior_color"] is not None and item["interior_color"] != "":
                    output["interior_color"] = item["interior_color"]
                if "fuel_type" in item and item["fuel_type"] is not None and item["fuel_type"] != "":
                    output["fuel"] = item["fuel_type"]
                if "sale_class" in item and item["sale_class"] is not None and item["sale_class"] != "":
                    if item["sale_class"] == "Used":
                        output["sale_class"] = 1
                    else:
                        output["sale_class"] = 0
                if "transmission" in item and item["transmission"] is not None and item["transmission"] != "":
                    output["transmission"] = item["transmission"]
                if "vin" in item and item["vin"] is not None and item["vin"] != "":
                    output["vin"] = item["vin"]
                if "year" in item and item["year"] is not None:
                    output["year"] = int(item["year"])
                if "passenger" in item and item["passenger"] is not None:
                    output["seats"] = int(item["passenger"])
                if "odometer" in item and item["odometer"] is not None and item["odometer"] != "":
                    output["odometer_value"] = item["odometer"]
                    output["odometer_unit"] = "KM"
                if "image" in item:
                    pictures_list = []
                    for image in item["image"]:
                        pictures_list.append((image["image_lg"]))
                    output["picture_list"] = json.dumps(pictures_list)
                if "vdp_url" in item and item["vdp_url"] is not None and item["vdp_url"] != "":
                    output["vehicle_url"] = item["vdp_url"].replace('\/','/').replace('www.northwayford.ca','www.carnationcanada.com')
                if "company_data" in item and item["company_data"] is not None:
                    output["city"] = item["company_data"]["company_city"]
                    output["state_or_province"] = item["company_data"]["company_province"]
                apify.pushData(output)

            page += 1
            q_params = {
                "action": "vms_data",
                "endpoint": f'https://vms.prod.convertus.rocks/api/filtering/?cp=539&ln=en&pg={page}&pc=15&dc=true&qs=&im=&svs=&sc=all&v1=Passenger%20Vehicles&st=make%2Casc&ai=true&oem=&dp=&in_transit=true&in_stock=true&on_order=true&mk=&md=&tr=&bs=&tm=&dt=&cy=&ec=&mc=&ic=&pa=&ft=&eg=&v2=&v3=&fp=&fc=&fn=&tg=',
            }
            encode_q_params = urlencode(q_params)
            url = f'https://www.carnationcanada.com/wp-content/plugins/convertus-vms/include/php/ajax-vehicles.php?{encode_q_params}'
            yield scrapy.Request(url=url, callback=self.parse, headers={'Content-Type': 'application/json'}, cb_kwargs={'page': page})

