import scrapy
import re
from faker import Faker
import random
import time

from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.RMModel import Raw_Material

client = MongoClient("Scrapy", "Raw_Material")
fake = Faker()


class SciSpider(scrapy.Spider):
    name = "sel_broker"
    allowed_domains = [
        "moneydj.emega.com.tw",
        "5850web.moneydj.com/",
        "stock.capital.com.tw",
        "newjust.masterlink.com.tw",
        "just2.entrust.com.tw",
        "sjmain.esunsec.com.tw",
        "jsjustweb.jihsun.com.tw/",
    ]

    custom_settings = {"RETRY_MAX_TIME": 0}
