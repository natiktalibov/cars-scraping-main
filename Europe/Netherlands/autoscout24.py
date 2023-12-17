from urllib.parse import urljoin
import asyncio
import aiohttp
import json

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
import apify
import requests
import datetime
from bs4 import BeautifulSoup


# To run this Actor locally, you need to have the Selenium Chromedriver installed.
# https://www.selenium.dev/documentation/webdriver/getting_started/install_drivers/
# When running on the Apify platform, it is already included in the Actor's Docker image.

class element_contains_text(object):
    """An expectation for checking that an element has a particular css class.

    locator - used to find the element
    returns the WebElement once it has the particular css class
    """

    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = driver.find_element(*self.locator)  # Finding the referenced element
        if "resultaten" in element.text:
            return True
        else:
            return False


async def fetch_url(session, url):
    retries = 3  # Maximum number of retries
    for i in range(retries):
        try:
            async with session.get(url) as response:
                return await response.text()
        except aiohttp.ClientConnectionError:
            print(f"Attempt {i + 1}/{retries} failed. Retrying...")
    raise Exception("Failed to fetch URL after multiple attempts.")


async def main():
    chrome_options = ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    wait = WebDriverWait(driver, 500)

    driver.get('https://www.autoscout24.nl/?genlnk=navi&genlnkorigin=com-all-all-home')

    cookies_btn = driver.find_element(By.XPATH, "//button[@class='_consent-accept_1i5cd_111']")
    cookies_btn.click()

    make_dropdown = Select(driver.find_element(By.ID, "make"))
    make_options = driver.find_elements(By.XPATH, "//select[@id='make']/optgroup/option")

    session_timeout = aiohttp.ClientTimeout(total=500)

    for make_option in make_options:
        if make_option.text != "Make":
            value = make_option.get_attribute("value")
            make_dropdown.select_by_value(value)
            wait.until(element_contains_text((By.XPATH,
                                              "//a[@class='hf-searchmask-form__filter__search-button sc-btn-bob sc-absolute-center']/span")))

            model_dropdown = Select(driver.find_element(By.ID, "model"))
            wait.until(EC.element_to_be_clickable((By.ID, "model")))
            search_btn = driver.find_element(By.XPATH,
                                             "//a[@class='hf-searchmask-form__filter__search-button sc-btn-bob sc-absolute-center']")
            wait.until(element_contains_text((By.XPATH,
                                              "//a[@class='hf-searchmask-form__filter__search-button sc-btn-bob sc-absolute-center']/span")))
            results_after_make_selection = search_btn.text.strip().split(" ")[0].replace(",", "").replace(".", "")

            if results_after_make_selection.isnumeric() and int(results_after_make_selection) > 400:
                is_enabled = driver.find_element(By.ID, "model").is_enabled()
                if is_enabled:
                    model_options = model_dropdown.options
                    for model_option in model_options[1:]:
                        if "all" not in model_option.text:
                            model_value = model_option.get_attribute("value")
                            model_dropdown.select_by_value(model_value)
                            wait.until(element_contains_text((By.XPATH,
                                                              "//a[@class='hf-searchmask-form__filter__search-button sc-btn-bob sc-absolute-center']/span")))
                            results_after_model_selection = search_btn.text.strip().split(" ")[0].replace(",",
                                                                                                          "").replace(
                                ".", "")
                            if results_after_model_selection.isnumeric() and int(results_after_model_selection) != 0:
                                async with aiohttp.ClientSession(timeout=session_timeout) as session:
                                    await parse_page(session, search_btn.get_attribute("href"), 1)


            else:
                if results_after_make_selection.isnumeric() and int(results_after_make_selection) != 0:
                    async with aiohttp.ClientSession(timeout=session_timeout) as session:
                        await parse_page(session, search_btn.get_attribute("href"), 1)


async def parse_page(session, url, page):
    result = await fetch_url(session, f'https://www.autoscout24.com/{url.split("nl/")[1]}')
    soup = BeautifulSoup(result, 'html.parser')
    links = soup.find_all("a", {"class": "ListItem_title__znV2I ListItem_title_new_design__lYiAv Link_link__pjU1l"})

    tasks = []
    for link in links:
        href = link['href']
        listing_url = f'https://www.autoscout24.com{href}'
        tasks.append(listing_details(session, listing_url))

    if len(links) > 0:
        next_page = page + 1
        next_url = '&'.join(url.split("&")[:-1]) + f'&page={next_page}'
        tasks.append(parse_page(session, next_url, next_page))

    await asyncio.gather(*tasks)


async def listing_details(session, url):
    result = await fetch_url(session, url)
    soup = BeautifulSoup(result, 'html.parser')

    output = dict()

    output['ac_installed'] = 0
    output['tpms_installed'] = 0

    output['scraped_date'] = datetime.datetime.isoformat(datetime.datetime.today())
    output['scraped_from'] = 'AutoScout24'
    output["vehicle_url"] = url

    make = soup.select_one(".StageTitle_makeModelContainer__WPHjg > span:nth-child(1)")
    model = soup.select_one(".StageTitle_makeModelContainer__WPHjg > span:nth-child(2)")
    if make is not None:
        output["make"] = make.get_text()
    if model is not None:
        output["model"] = model.get_text()

    city_element = soup.select_one("a.scr-link.LocationWithPin_locationItem__pHhCa")
    city = city_element.get_text() if city_element else None
    output["country"] = "NL"
    if city_element is not None:
        output["city"] = city.split(",")[0]

    price_element = soup.select_one("span.PriceInfo_price__JPzpT")
    price = price_element.get_text() if price_element else None
    if price is not None:
        output["price_retail"] = float(
            price.replace("-", "").replace("€", "").replace(",", ".").replace(".", "").strip())
        output["currency"] = 'EUR'

    pictures = soup.select("img.image-gallery-thumbnail-image")
    pictures_list = [picture["src"] for picture in pictures]
    if pictures_list is not None:
        output['picture_list'] = json.dumps(pictures_list)

    details_first_section = soup.select_one("dl.DataGrid_defaultDlStyle__969Qm:first-child")
    if details_first_section is not None:
        details_first_section_keys = [dt.get_text() for dt in details_first_section.find_all("dt")]
        details_first_section_values = [dd.get_text() for dd in details_first_section.find_all("dd")]
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

    details_second_section = soup.select_one("div[data-cy='listing-history-section'] dl")
    if details_second_section is not None:
        details_second_section_keys = [dt.get_text() for dt in details_second_section.find_all("dt")]
        details_second_section_values = [dd.get_text() for dd in details_second_section.find_all("dd")]
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

    details_third_section = soup.select_one("div[data-cy='technical-details-section'] dl")
    if details_third_section is not None:
        details_third_section_keys = [dt.get_text() for dt in details_third_section.find_all("dt")]
        details_third_section_values = [dd.get_text() for dd in details_third_section.find_all("dd")]
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

    details_color_section = soup.select_one("div[data-cy='color-section'] dl")
    if details_color_section is not None:
        details_color_section_keys = [dt.get_text() for dt in details_color_section.find_all("dt")]
        details_color_section_values = [dd.get_text() for dd in details_color_section.find_all("dd")]
        for k in range(len(details_color_section_keys)):
            key = details_color_section_keys[k]
            value = details_color_section_values[k]
            if key == "Colour":
                output["exterior_color"] = value
            elif key == "Upholstery colour":
                output["interior_color"] = value
            elif key == "Upholstery":
                output["upholstery"] = value

    apify.pushData(output)
