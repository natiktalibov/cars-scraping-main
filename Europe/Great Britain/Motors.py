import datetime
import json
import scrapy
import apify


class BuyACarSpider(scrapy.Spider):
    name = 'MotorsUK'
    download_timeout = 120

    def start_requests(self):
        urls = [
            'https://www.motors.co.uk/search/car/results',
        ]
        payload = {
          "isNewSearch": True,
          "pagination": {
            "TotalPages": 0,
            "BasicResultCount": 238985,
            "TotalRecords": 238985,
            "FirstRecord": 1,
            "LastRecord": 22,
            "CurrentPage": 2,
            "LastPage": 10863,
            "PageSize": 22,
            "PageLinksPerPage": 5,
            "PageLinks": [
              {
                "Name": "1",
                "Link": "1"
              },
              {
                "Name": "2",
                "Link": "2"
              },
              {
                "Name": "3",
                "Link": "3"
              },
              {
                "Name": "4",
                "Link": "4"
              },
              {
                "Name": "5",
                "Link": "5"
              }
            ],
            "FirstPageLink": {
              "Name": "1",
              "Link": "1"
            },
            "Level": None,
            "Variants": 0
          },
          "searchPanelParameters": {
            "Doors": [],
            "Seats": [],
            "SafetyRatings": [],
            "SelectedTopSpeed": None,
            "SelectedPower": None,
            "SelectedAcceleration": None,
            "MinPower": -1,
            "MaxPower": -1,
            "MinEngineSize": -1,
            "MaxEngineSize": -1,
            "BodyStyles": [],
            "DriveTrains": [],
            "MakeModels": [],
            "FuelTypes": [],
            "Transmissions": [],
            "Colours": [],
            "IsPaymentSearch": False,
            "IsReduced": False,
            "IsHot": False,
            "IsRecentlyAdded": False,
            "IsGroupStock": False,
            "PartExAvailable": False,
            "IsPriceAndGo": False,
            "IsPriceExcludeVATSearch": False,
            "IncludeOnlineOnlySearch": False,
            "IsYearSearch": False,
            "IsPreReg": False,
            "IsExDemo": False,
            "ExcludeExFleet": False,
            "ExcludeExHire": False,
            "Keywords": [],
            "SelectedInsuranceGroup": None,
            "SelectedFuelEfficiency": None,
            "SelectedCostAnnualTax": None,
            "SelectedCO2Emission": None,
            "SelectedTowingBrakedMax": None,
            "SelectedTowingUnbrakedMax": None,
            "SelectedTankRange": None,
            "DealerId": 0,
            "Age": -1,
            "MinAge": -1,
            "MaxAge": -1,
            "MinYear": -1,
            "MaxYear": -1,
            "Mileage": -1,
            "MinMileage": -1,
            "MaxMileage": -1,
            "MinPrice": -1,
            "MaxPrice": -1,
            "MinPaymentMonthlyCost": -1,
            "MaxPaymentMonthlyCost": -1,
            "PaymentTerm": 60,
            "PaymentMileage": 10000,
            "PaymentDeposit": 1000,
            "SelectedSoldStatusV2": "notsold",
            "SelectedBatteryRangeMiles": None,
            "SelectedBatteryFastChargeMinutes": None,
            "BatteryIsLeased": False,
            "BatteryIsWarrantyWhenNew": False,
            "ExcludeImports": False,
            "ExcludeHistoryCatNCatD": False,
            "ExcludeHistoryCatSCatC": False,
            "Type": 1,
            "PostCode": "SW1W0NY",
            "Distance": 1000,
            "SortOrder": 0,
            "DealerGroupId": 0,
            "MinImageCountActive": False,
            "PaginationCurrentPage": 2
          }
        }

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, method="POST", headers={'Content-Type': 'application/json'}, body=json.dumps(payload), meta={"payload": payload})

    def parse(self, response):

        results = response.json()["Results"]
        for vehicle in results:
            if vehicle["ObjectType"] == "UsedVehicleResult":
                output = dict()

                output['ac_installed'] = 0
                output['tpms_installed'] = 0

                output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
                output['scraped_from'] = 'MotorsUK'
                output['scraped_listing_id'] = vehicle["VehicleId"]
                output["vehicle_url"] = f'https://www.motors.co.uk{vehicle["DetailsPageUrl"]}'

                output['country'] = 'UK'
                output["make"] = vehicle["Manufacturer"]
                output["model"] = vehicle["Model"]
                if "Variant" in vehicle:
                    output["trim"] = vehicle["Variant"]
                if "MileageInt" in vehicle:
                    output["odometer_value"] = int(vehicle["MileageInt"])
                    output["odometer_units"] = "miles"
                if "Price" in vehicle:
                    output["price_retail"] = float(vehicle["Price"])
                    output["currency"] = "GBP"
                if "FuelType" in vehicle:
                    output["fuel"] = vehicle["FuelType"]
                if "Transmission" in vehicle:
                    output["transmission"] = vehicle["Transmission"]
                if "RegistrationYear" in vehicle and vehicle["RegistrationYear"].isnumeric():
                    output["registration_year"] = int(vehicle["RegistrationYear"])
                if "EngineSizeLitres" in vehicle:
                    output["engine_displacement_value"] = vehicle["EngineSizeLitres"]
                    output["engine_displacement_units"] = "L"
                if "Colour" in vehicle and vehicle["Colour"] != "Not Supplied":
                    output["exterior_color"] = vehicle["Colour"]
                if "BodyStyle" in vehicle:
                    output["body_type"] = vehicle["BodyStyle"]
                if "MainImage" in vehicle:
                    output["picture_list"] = json.dumps([vehicle["MainImage"]["Thumbnail"]])
                
                apify.pushData(output)

        payload = response.meta["payload"]
        if payload["pagination"]["CurrentPage"] != payload["pagination"]["LastPage"]:
            payload["pagination"]["CurrentPage"] += 1
            payload["pagination"]["FirstRecord"] += payload["pagination"]["PageSize"]
            payload["pagination"]["LastRecord"] += payload["pagination"]["PageSize"]
            yield scrapy.Request(url='https://www.motors.co.uk/search/car/results', callback=self.parse, method="POST",
                                 headers={'Content-Type': 'application/json'}, body=json.dumps(payload),
                                 meta={"payload": payload})



