import csv
import json
import logging
import pandas as pd
import pickle
import re
import time
import scrapy
import undetected_chromedriver as uc

from scrapy_selenium import SeleniumRequest
from scrapy.utils.log import configure_logging
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait


class MetroSpider(scrapy.Spider):
    def __init__(self):
        self.driver = uc.Chrome()

    name = "metro"
    allowed_domains = ["online.metro-cc.ru"]
    start_urls = ["https://online.metro-cc.ru"]

    # configure_logging(install_root_handler=False)
    # logging.basicConfig(
    #     filename='log.txt',
    #     format='%(levelname)s: %(message)s',
    #     level=logging.INFO
    # )

    def parse(self, path_template):
        name_value = WebDriverWait(self.driver, 140).until(
            ec.visibility_of_any_elements_located(
                (
                    By.XPATH,
                    path_template
                )))[0]

        stringify_name_value = str(name_value.get_attribute("innerHTML"))
        return stringify_name_value

    def make_product_url_list(self):
        self.driver.get('https://online.metro-cc.ru/')
        time.sleep(10)
        for page in range(1, 4):
            if page == 1:
                url = f'https://online.metro-cc.ru/category/sladosti-chipsy-sneki/zefir-marmelad-vostochnye-sladosti'
            else:
                url = f'https://online.metro-cc.ru/category/sladosti-chipsy-sneki/zefir-marmelad-vostochnye-sladosti?page={page}'
            self.driver.get(url)
            time.sleep(10)
            block = self.driver.find_element(By.ID, 'products-inner')
            href_list = block.find_elements(By.TAG_NAME, 'a')
            filename = 'urls.csv'
            with open(filename, 'a') as f:
                for href in href_list:
                    stringify_href = str(href.get_attribute('href'))
                    f.write(stringify_href)
                    f.write('\n')
                    time.sleep(2)

    def drop_dupe_urls(self):
        titles = ['url']
        url_list = pd.read_csv("urls.csv", names=titles, header=None)
        urls = url_list.drop_duplicates().reset_index(drop=True)
        with open('sweet_urls.csv', 'a') as f:
            urls.to_csv(f, index=False, header=False)

    def get_sweet_description(self):
        result_file = open('sweet_list.csv', 'a', encoding='utf-8')
        result_dict ={}
        with open('sweet_urls.csv', 'r', encoding='utf-8') as f:
            line = f.readline()
            while line:
                self.driver.get(line)
                time.sleep(40)
                try:
                    template_path = (
                        "//p[@class='product-page-content__article']"
                    )
                    stringify_id_value = self.parse(template_path)
                    stringify_id_value = str(stringify_id_value.split()[1])
                except TimeoutException:
                    stringify_id_value = '1000001'

                try:
                    template_path = (
                        "//div[@class='catalog-breadcrumbs__list-item-text']//span"
                    )
                    stringify_name_value = self.parse(template_path)
                except TimeoutException:
                    stringify_name_value = 'None'

                try:
                    template_path = (
                        f"//div[@class='product-unit-prices__actual-wrapper']"
                        f"//span[@class='product-price__sum-rubles']"
                    )
                    stringify_price_rub = self.parse(template_path)
                    if '&nbsp;' in stringify_price_rub:
                        stringify_price_rub = stringify_price_rub.replace('&nbsp;', '')
                except TimeoutException:
                    stringify_price_rub = '0'

                try:
                    template_path = (
                        f"//div[@class='product-unit-prices__trigger']"
                        f"//span[@class='product-price__sum-penny']"
                    )
                    stringify_price_kop = self.parse(template_path)
                except TimeoutException:
                    stringify_price_kop = '.0'

                try:
                    template_path = (
                        f"//div[@class='product-page-content__offline-bmpl-prices']"
                        f"//span[@class='product-price__sum-rubles']"
                    )
                    stringify_discounted_rub = self.parse(template_path)
                    if '&nbsp;' in stringify_discounted_rub:
                        stringify_discounted_rub = stringify_discounted_rub.replace('&nbsp;', '')
                except TimeoutException:
                    stringify_discounted_rub = '0'

                try:
                    template_path = (
                        f"//div[@class='product-page-content__offline-bmpl-prices']"
                        f"//span[@class='product-price__sum-penny']"
                    )
                    stringify_discounted_kop = self.parse(template_path)
                except TimeoutException:
                    stringify_discounted_kop = '.0'

                try:
                    template_path = (
                        "//li[./span/span[contains(text(),'Бренд')]]//a"
                    )
                    stringify_brand_name = self.parse(template_path)
                except TimeoutException:
                    stringify_brand_name = '-'

                stringify_price_value = (
                        stringify_price_rub.strip()
                        + stringify_price_kop.strip()
                )
                stringify_discounted_value = (
                        stringify_discounted_rub.strip()
                        + stringify_discounted_kop.strip()
                )

                result_dict['id'] = stringify_id_value.strip()
                result_dict['name'] = stringify_name_value.strip()
                result_dict['url'] = line
                result_dict['price'] = stringify_price_value
                result_dict['price_disc'] = stringify_discounted_value
                result_dict['brand'] = stringify_brand_name.strip()
                csv.DictWriter(result_file, fieldnames=[
                    "id",
                    "name",
                    "url",
                    "price",
                    "price_disc",
                    "brand"
                ]).writerow(result_dict)
                time.sleep(20)
                line = f.readline()

    def start_requests(self):
        self.make_product_url_list()
        time.sleep(2)
        self.drop_dupe_urls()
        time.sleep(2)
        self.get_sweet_description()
        time.sleep(2)
        self.driver.quit()
        url = "https://online.metro-cc.ru"
        yield SeleniumRequest(
            url=url,
            callback=self.parse
        )
