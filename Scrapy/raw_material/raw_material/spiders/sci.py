import scrapy
import re
from faker import Faker
import random
import time

from Financial_data_crawler.db.clients import MongoClient
from Financial_data_crawler.db.RMModel import Raw_Material
from Financial_data_crawler.Scrapy.raw_material.raw_material.middlewares import MaxRetriesExceeded

client = MongoClient("Scrapy", "Raw_Material")
fake = Faker()


class SciSpider(scrapy.Spider):
    name = "sci"
    allowed_domains = ["www.sci99.com/"]

    custom_settings = {"RETRY_TIMES": 3}

    def start_requests(self):
        user_agent = fake.user_agent()

        urls = [
            "https://www.sci99.com/monitor-1-0.html",
            "https://www.sci99.com/monitor-9180-0.html",
            "https://www.sci99.com/monitor-9179-0.html",
            "https://www.sci99.com/monitor-439-0.html",
            "https://www.sci99.com/monitor-429-0.html",
            "https://www.sci99.com/monitor-750-0.html",
            "https://www.sci99.com/monitor-1084-0.html",
            "https://www.sci99.com/monitor-752-0.html",
            "https://www.sci99.com/monitor-428-0.html",
            "https://www.sci99.com/monitor-430-0.html",
            "https://www.sci99.com/monitor-613-0.html",
            "https://www.sci99.com/monitor-747-0.html",
            "https://www.sci99.com/monitor-746-0.html",
            "https://www.sci99.com/monitor-751-0.html",
            "https://www.sci99.com/monitor-745-0.html",
            "https://www.sci99.com/monitor-57236214-0.html",
            "https://www.sci99.com/monitor-70079214-0.html",
            "https://www.sci99.com/monitor-57237214-0.html",
            "https://www.sci99.com/monitor-107997214-0.html",
            "https://www.sci99.com/monitor-94877214-0.html",
            "https://www.sci99.com/monitor-114779214-0.html",
            "https://www.sci99.com/monitor-114833214-0.html",
            "https://www.sci99.com/monitor-114827214-0.html",
            "https://www.sci99.com/monitor-4118537-0.html",
            "https://www.sci99.com/monitor-114857214-0.html",
            "https://www.sci99.com/monitor-113688214-0.html",
            "https://www.sci99.com/monitor-94974214-0.html",
            "https://www.sci99.com/monitor-94888214-0.html",
            "https://www.sci99.com/monitor-114842214-0.html",
            "https://www.sci99.com/monitor-107943214-0.html",
            "https://www.sci99.com/monitor-108010214-0.html",
            "https://www.sci99.com/monitor-94897214-0.html",
            "https://www.sci99.com/monitor-11117637-0.html",
            "https://www.sci99.com/monitor-377-0.html",
            "https://www.sci99.com/monitor-374-0.html",
            "https://www.sci99.com/monitor-445-0.html",
            "https://www.sci99.com/monitor-407-0.html",
            "https://www.sci99.com/monitor-366-0.html",
            "https://www.sci99.com/monitor-447-0.html",
            "https://www.sci99.com/monitor-367-0.html",
            "https://www.sci99.com/monitor-433-0.html",
            "https://www.sci99.com/monitor-453-0.html",
            "https://www.sci99.com/monitor-1481-0.html",
            "https://www.sci99.com/monitor-1482-0.html",
            "https://www.sci99.com/monitor-449-0.html",
            "https://www.sci99.com/monitor-448-0.html",
            "https://www.sci99.com/monitor-433-0.html",
            "https://www.sci99.com/monitor-766-0.html",
            "https://www.sci99.com/monitor-536-0.html",
            "https://www.sci99.com/monitor-500-0.html",
            "https://www.sci99.com/monitor-507-0.html",
            "https://www.sci99.com/monitor-835-0.html",
            "https://www.sci99.com/monitor-1483-0.html",
            "https://www.sci99.com/monitor-1089-0.html",
            "https://www.sci99.com/monitor-937-0.html",
            "https://www.sci99.com/monitor-502-0.html",
            "https://www.sci99.com/monitor-804-0.html",
            "https://www.sci99.com/monitor-1511-0.html",
            "https://www.sci99.com/monitor-384-0.html",
            "https://www.sci99.com/monitor-379-0.html",
            "https://www.sci99.com/monitor-378-0.html",
            "https://www.sci99.com/monitor-759-0.html",
            "https://www.sci99.com/monitor-1508-0.html",
            "https://www.sci99.com/monitor-1505-0.html",
            "https://www.sci99.com/monitor-1509-0.html",
            "https://www.sci99.com/monitor-1657-0.html",
            "https://www.sci99.com/monitor-1656-0.html",
            "https://www.sci99.com/monitor-1658-0.html",
            "https://www.sci99.com/monitor-1659-0.html",
            "https://www.sci99.com/monitor-1660-0.html",
            "https://www.sci99.com/monitor-1661-0.html",
            "https://www.sci99.com/monitor-1662-0.html",
            "https://www.sci99.com/monitor-1571-0.html",
            "https://www.sci99.com/monitor-1572-0.html",
            "https://www.sci99.com/monitor-1575-0.html",
            "https://www.sci99.com/monitor-908-0.html",
            "https://www.sci99.com/monitor-504-0.html",
            "https://www.sci99.com/monitor-1674-0.html",
            "https://www.sci99.com/monitor-450-0.html",
            "https://www.sci99.com/monitor-505-0.html",
            "https://www.sci99.com/monitor-499-0.html",
            "https://www.sci99.com/monitor-444-0.html",
            "https://www.sci99.com/monitor-529-0.html",
            "https://www.sci99.com/monitor-501-0.html",
            "https://www.sci99.com/monitor-368-0.html",
            "https://www.sci99.com/monitor-369-0.html",
            "https://www.sci99.com/monitor-675-0.html",
            "https://www.sci99.com/monitor-528-0.html",
            "https://www.sci99.com/monitor-370-0.html",
            "https://www.sci99.com/monitor-372-0.html",
            "https://www.sci99.com/monitor-375-0.html",
            "https://www.sci99.com/monitor-384-0.html",
            "https://www.sci99.com/monitor-399-0.html",
            "https://www.sci99.com/monitor-506-0.html",
            "https://www.sci99.com/monitor-491-0.html",
            "https://www.sci99.com/monitor-385-0.html",
            "https://www.sci99.com/monitor-386-0.html",
            "https://www.sci99.com/monitor-94693214-0.html",
            "https://www.sci99.com/monitor-94768214-0.html",
            "https://www.sci99.com/monitor-94968214-0.html",
            "https://www.sci99.com/monitor-94767214-0.html",
            "https://www.sci99.com/monitor-113883214-0.html",
            "https://www.sci99.com/monitor-107946214-0.html",
            "https://www.sci99.com/monitor-94715214-0.html",
            "https://www.sci99.com/monitor-94714214-0.html",
            "https://www.sci99.com/monitor-94749214-0.html",
            "https://www.sci99.com/monitor-94732214-0.html",
            "https://www.sci99.com/monitor-94898214-0.html",
            "https://www.sci99.com/monitor-94709214-0.html",
            "https://www.sci99.com/monitor-114856214-0.html",
            "https://www.sci99.com/monitor-94738214-0.html",
            "https://www.sci99.com/monitor-94773214-0.html",
            "https://www.sci99.com/monitor-94734214-0.html",
            "https://www.sci99.com/monitor-94736214-0.html",
            "https://www.sci99.com/monitor-94731214-0.html",
            "https://www.sci99.com/monitor-94712214-0.html",
            "https://www.sci99.com/monitor-107942214-0.html",
            "https://www.sci99.com/monitor-94814214-0.html",
            "https://www.sci99.com/monitor-114848214-0.html",
            "https://www.sci99.com/monitor-94706214-0.html",
            "https://www.sci99.com/monitor-94758214-0.html",
            "https://www.sci99.com/monitor-94713214-0.html",
            "https://www.sci99.com/monitor-94721214-0.html",
            "https://www.sci99.com/monitor-94774214-0.html",
            "https://www.sci99.com/monitor-94717214-0.html",
            "https://www.sci99.com/monitor-94911214-0.html",
            "https://www.sci99.com/monitor-94909214-0.html",
            "https://www.sci99.com/monitor-94910214-0.html",
            "https://www.sci99.com/monitor-114787214-0.html",
            "https://www.sci99.com/monitor-94823214-0.html",
            "https://www.sci99.com/monitor-114836214-0.html",
            "https://www.sci99.com/monitor-114793214-0.html",
            "https://www.sci99.com/monitor-114812214-0.html",
            "https://www.sci99.com/monitor-114813214-0.html",
            "https://www.sci99.com/monitor-94824214-0.html",
            "https://www.sci99.com/monitor-114777214-0.html",
            "https://www.sci99.com/monitor-94752214-0.html",
            "https://www.sci99.com/monitor-94720214-0.html",
            "https://www.sci99.com/monitor-94728214-0.html",
            "https://www.sci99.com/monitor-114858214-0.html",
            "https://www.sci99.com/monitor-114859214-0.html",
            "https://www.sci99.com/monitor-114860214-0.html",
            "https://www.sci99.com/monitor-23287330-0.html",
            "https://www.sci99.com/monitor-61361214-0.html",
            "https://www.sci99.com/monitor-108000214-0.html",
            "https://www.sci99.com/monitor-107999214-0.html",
            "https://www.sci99.com/monitor-94878214-0.html",
            "https://www.sci99.com/monitor-113475214-0.html",
            "https://www.sci99.com/monitor-107971214-0.html",
            "https://www.sci99.com/monitor-107944214-0.html",
            "https://www.sci99.com/monitor-94703214-0.html",
            "https://www.sci99.com/monitor-107985214-0.html",
            "https://www.sci99.com/monitor-94852214-0.html",
            "https://www.sci99.com/monitor-114763214-0.html",
            "https://www.sci99.com/monitor-9-0.html",
            "https://www.sci99.com/monitor-33-0.html",
            "https://www.sci99.com/monitor-894-0.html",
            "https://www.sci99.com/monitor-898-0.html",
            "https://www.sci99.com/monitor-45-0.html",
            "https://www.sci99.com/monitor-53-0.html",
            "https://www.sci99.com/monitor-124-0.html",
            "https://www.sci99.com/monitor-87-0.html",
            "https://www.sci99.com/monitor-102-0.html",
            "https://www.sci99.com/monitor-1744-0.html",
            "https://www.sci99.com/monitor-68-0.html",
            "https://www.sci99.com/monitor-120-0.html",
            "https://www.sci99.com/monitor-121-0.html",
            "https://www.sci99.com/monitor-123-0.html",
            "https://www.sci99.com/monitor-65-0.html",
            "https://www.sci99.com/monitor-737-0.html",
            "https://www.sci99.com/monitor-740-0.html",
            "https://www.sci99.com/monitor-241-0.html",
            "https://www.sci99.com/monitor-235-0.html",
            "https://www.sci99.com/monitor-238-0.html",
            "https://www.sci99.com/monitor-114790214-0.html",
            "https://www.sci99.com/monitor-114788214-0.html",
            "https://www.sci99.com/monitor-107969214-0.html",
            "https://www.sci99.com/monitor-113477214-0.html",
            "https://www.sci99.com/monitor-114699214-0.html",
            "https://www.sci99.com/monitor-108001214-0.html",
            "https://www.sci99.com/monitor-107995214-0.html",
            "https://www.sci99.com/monitor-114741214-0.html",
            "https://www.sci99.com/monitor-114712214-0.html",
            "https://www.sci99.com/monitor-114743214-0.html",
            "https://www.sci99.com/monitor-114742214-0.html",
            "https://www.sci99.com/monitor-114773214-0.html",
            "https://www.sci99.com/monitor-114684214-0.html",
            "https://www.sci99.com/monitor-900-0.html",
            "https://www.sci99.com/monitor-74-0.html",
            "https://www.sci99.com/monitor-107-0.html",
            "https://www.sci99.com/monitor-113-0.html",
            "https://www.sci99.com/monitor-633-0.html",
            "https://www.sci99.com/monitor-697-0.html",
            "https://www.sci99.com/monitor-1664-0.html",
            "https://www.sci99.com/monitor-1665-0.html",
            "https://www.sci99.com/monitor-1670-0.html",
            "https://www.sci99.com/monitor-1671-0.html",
            "https://www.sci99.com/monitor-1663-0.html",
            "https://www.sci99.com/monitor-1672-0.html",
            "https://www.sci99.com/monitor-1669-0.html",
            "https://www.sci99.com/monitor-463-0.html",
            "https://www.sci99.com/monitor-114843214-0.html",
            "https://www.sci99.com/monitor-114803214-0.html",
            "https://www.sci99.com/monitor-114844214-0.html",
            "https://www.sci99.com/monitor-94969214-0.html",
            "https://www.sci99.com/monitor-94725214-0.html",
            "https://www.sci99.com/monitor-114837214-0.html",
            "https://www.sci99.com/monitor-113482214-0.html",
            "https://www.sci99.com/monitor-649-0.html",
            "https://www.sci99.com/monitor-650-0.html",
            "https://www.sci99.com/monitor-651-0.html",
            "https://www.sci99.com/monitor-673-0.html",
            "https://www.sci99.com/monitor-8172-0.html",
            "https://www.sci99.com/monitor-665-0.html",
            "https://www.sci99.com/monitor-652-0.html",
            "https://www.sci99.com/monitor-659-0.html",
            "https://www.sci99.com/monitor-114704214-0.html",
            "https://www.sci99.com/monitor-51502214-0.html",
            "https://www.sci99.com/monitor-94796214-0.html",
            "https://www.sci99.com/monitor-114761214-0.html",
            "https://www.sci99.com/monitor-94795214-0.html",
            "https://www.sci99.com/monitor-114781214-0.html",
            "https://www.sci99.com/monitor-114782214-0.html",
            "https://www.sci99.com/monitor-94793214-0.html",
            "https://www.sci99.com/monitor-114747214-0.html",
            "https://www.sci99.com/monitor-51474214-0.html",
            "https://www.sci99.com/monitor-94790214-0.html",
            "https://www.sci99.com/monitor-114762214-0.html",
            "https://www.sci99.com/monitor-56194214-0.html",
            "https://www.sci99.com/monitor-94857214-0.html",
            "https://www.sci99.com/monitor-94815214-0.html",
            "https://www.sci99.com/monitor-114748214-0.html",
            "https://www.sci99.com/monitor-107961214-0.html",
            "https://www.sci99.com/monitor-108002214-0.html",
            "https://www.sci99.com/monitor-113818214-0.html",
            "https://www.sci99.com/monitor-108004214-0.html",
            "https://www.sci99.com/monitor-107959214-0.html",
            "https://www.sci99.com/monitor-113887214-0.html",
            "https://www.sci99.com/monitor-113886214-0.html",
            "https://www.sci99.com/monitor-108003214-0.html",
            "https://www.sci99.com/monitor-644-0.html",
            "https://www.sci99.com/monitor-643-0.html",
            "https://www.sci99.com/monitor-646-0.html",
            "https://www.sci99.com/monitor-645-0.html",
            "https://www.sci99.com/monitor-647-0.html",
            "https://www.sci99.com/monitor-648-0.html",
            "https://www.sci99.com/monitor-688-0.html",
            "https://www.sci99.com/monitor-181-0.html",
            "https://www.sci99.com/monitor-56420214-0.html",
            "https://www.sci99.com/monitor-94841214-0.html",
            "https://www.sci99.com/monitor-56421214-0.html",
            "https://www.sci99.com/monitor-114789214-0.html",
            "https://www.sci99.com/monitor-114678214-0.html",
            "https://www.sci99.com/monitor-555-0.html",
            "https://www.sci99.com/monitor-629-0.html",
            "https://www.sci99.com/monitor-532-0.html",
            "https://www.sci99.com/monitor-531-0.html",
            "https://www.sci99.com/monitor-681-0.html",
            "https://www.sci99.com/monitor-626-0.html",
            "https://www.sci99.com/monitor-559-0.html",
            "https://www.sci99.com/monitor-621-0.html",
            "https://www.sci99.com/monitor-485-0.html",
            "https://www.sci99.com/monitor-623-0.html",
            "https://www.sci99.com/monitor-560-0.html",
            "https://www.sci99.com/monitor-622-0.html",
            "https://www.sci99.com/monitor-487-0.html",
            "https://www.sci99.com/monitor-690-0.html",
            "https://www.sci99.com/monitor-496-0.html",
            "https://www.sci99.com/monitor-689-0.html",
            "https://www.sci99.com/monitor-678-0.html",
            "https://www.sci99.com/monitor-679-0.html",
            "https://www.sci99.com/monitor-94873214-0.html",
            "https://www.sci99.com/monitor-94691214-0.html",
            "https://www.sci99.com/monitor-107993214-0.html",
            "https://www.sci99.com/monitor-113501214-0.html",
            "https://www.sci99.com/monitor-107992214-0.html",
            "https://www.sci99.com/monitor-107991214-0.html",
            "https://www.sci99.com/monitor-94872214-0.html",
            "https://www.sci99.com/monitor-94922214-0.html",
            "https://www.sci99.com/monitor-94921214-0.html",
            "https://www.sci99.com/monitor-94915214-0.html",
            "https://www.sci99.com/monitor-51520214-0.html",
            "https://www.sci99.com/monitor-56454214-0.html",
            "https://www.sci99.com/monitor-55591214-0.html",
            "https://www.sci99.com/monitor-94906214-0.html",
            "https://www.sci99.com/monitor-94834214-0.html",
            "https://www.sci99.com/monitor-94903214-0.html",
            "https://www.sci99.com/monitor-94835214-0.html",
            "https://www.sci99.com/monitor-55621214-0.html",
            "https://www.sci99.com/monitor-52066214-0.html",
            "https://www.sci99.com/monitor-107981214-0.html",
            "https://www.sci99.com/monitor-52106214-0.html",
            "https://www.sci99.com/monitor-52373214-0.html",
            "https://www.sci99.com/monitor-94858214-0.html",
            "https://www.sci99.com/monitor-97663214-0.html",
            "https://www.sci99.com/monitor-94811214-0.html",
            "https://www.sci99.com/monitor-94809214-0.html",
            "https://www.sci99.com/monitor-52004214-0.html",
            "https://www.sci99.com/monitor-94828214-0.html",
            "https://www.sci99.com/monitor-54752214-0.html",
            "https://www.sci99.com/monitor-54795214-0.html",
            "https://www.sci99.com/monitor-54709214-0.html",
            "https://www.sci99.com/monitor-53774214-0.html",
            "https://www.sci99.com/monitor-107949214-0.html",
            "https://www.sci99.com/monitor-54710214-0.html",
            "https://www.sci99.com/monitor-53731214-0.html",
            "https://www.sci99.com/monitor-114689214-0.html",
            "https://www.sci99.com/monitor-51652214-0.html",
            "https://www.sci99.com/monitor-94971214-0.html",
            "https://www.sci99.com/monitor-51770214-0.html",
            "https://www.sci99.com/monitor-51665214-0.html",
            "https://www.sci99.com/monitor-56449214-0.html",
            "https://www.sci99.com/monitor-51772214-0.html",
            "https://www.sci99.com/monitor-94743214-0.html",
            "https://www.sci99.com/monitor-107974214-0.html",
            "https://www.sci99.com/monitor-52110214-0.html",
            "https://www.sci99.com/monitor-52097214-0.html",
            "https://www.sci99.com/monitor-113693214-0.html",
            "https://www.sci99.com/monitor-113898214-0.html",
            "https://www.sci99.com/monitor-94980214-0.html",
            "https://www.sci99.com/monitor-6155637-0.html",
            "https://www.sci99.com/monitor-109554214-0.html",
            "https://www.sci99.com/monitor-56465214-0.html",
            "https://www.sci99.com/monitor-51779214-0.html",
            "https://www.sci99.com/monitor-94853214-0.html",
            "https://www.sci99.com/monitor-51403214-0.html",
            "https://www.sci99.com/monitor-53799214-0.html",
            "https://www.sci99.com/monitor-94862214-0.html",
            "https://www.sci99.com/monitor-54358214-0.html",
            "https://www.sci99.com/monitor-94896214-0.html",
            "https://www.sci99.com/monitor-94836214-0.html",
            "https://www.sci99.com/monitor-94838214-0.html",
            "https://www.sci99.com/monitor-94840214-0.html",
            "https://www.sci99.com/monitor-94839214-0.html",
            "https://www.sci99.com/monitor-94902214-0.html",
            "https://www.sci99.com/monitor-51557214-0.html",
            "https://www.sci99.com/monitor-113466214-0.html",
            "https://www.sci99.com/monitor-94843214-0.html",
            "https://www.sci99.com/monitor-94844214-0.html",
            "https://www.sci99.com/monitor-94837214-0.html",
            "https://www.sci99.com/monitor-3656837-0.html",
            "https://www.sci99.com/monitor-94978214-0.html",
            "https://www.sci99.com/monitor-94979214-0.html",
            "https://www.sci99.com/monitor-108013214-0.html",
            "https://www.sci99.com/monitor-94976214-0.html",
            "https://www.sci99.com/monitor-107932214-0.html",
            "https://www.sci99.com/monitor-94895214-0.html",
            "https://www.sci99.com/monitor-94975214-0.html",
        ]

        for url in urls:
            time.sleep(round(random.uniform(1, 4), 2))
            yield scrapy.Request(
                url=url, callback=self.parse,
                headers={"User-Agent": fake.user_agent()},
                errback= self.handle_504_error)

    def parse(self, response):
        def clean(string):
            # Remove all whitespace characters
            string = re.sub(r"\s+", "", string)

            # Use the str.strip() method to remove leading and trailing whitespace
            return string.strip()

        # Material name
        table_name = clean(
            response.xpath('//div[@class="detect_title"]/h2/text()').get()
        )

        # Use more descriptive variable names
        table_rows = response.xpath(
            '//div[@id="Panel1"]/div[@class="div_content"]/div/table/tr'
        )

        # Use a list comprehension to extract the data from the table rows
        data = [clean(row.xpath("td/text()").get()) for row in table_rows]

        # Use a similar approach to extract the prices
        prices = [clean(row.xpath("td/a/text()").get()) for row in table_rows]

        date_all = []
        price_all = []

        for row in range(0, len(data)):
            if re.match(r"\d{4}-\d{1,2}-\d{1,2}", data[row]):
                date_all.append(clean(data[row]))
                price_all.append(clean(prices[row]))

        for date, price in zip(date_all, price_all):
            d = {"Date": date, "Material_Name": table_name, "Price": float(price)}
            Raw_Material.objects(Date=date, Material_Name=table_name).modify(
                upsert=True, **d
            )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SciSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        return spider

    def handle_504_error(self, failure):
        if failure.check(MaxRetriesExceeded):
            self.logger.warning("Maximum number of 504 errors exceeded. Pausing for 1 hour.")
            time.sleep(3600)
            self.logger.warning("Resuming spider.")
            return self.make_requests_from_url(failure.request.url)
        else:
            return failure

    def spider_closed(self, spider):
        client.close()
