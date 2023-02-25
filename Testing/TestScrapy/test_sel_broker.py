from bs4 import BeautifulSoup

from Financial_data_crawler.Scrapy.sel_broker.sel_broker.spiders import sel_broker


def test_normal_case():
    test_str = """
        <SCRIPT LANGUAGE=javascript>
        <!--
        GenLink2stk('AS1795','美時');
        //-->
        </SCRIPT>
        """
    soup_obj = BeautifulSoup(test_str, "html.parser")
    stockid, stockname = sel_broker._extract_code_and_name(soup_obj)

    assert stockid == "AS1795"
    assert stockname == "美時"


def test_etf_case():
    test_str = """
        <td class="t4t1" id="oAddCheckbox" nowrap="">
        <a href="javascript:Link2Stk('00878');">00878國泰永續高股息</a>
        </td>
        """

    soup_obj = BeautifulSoup(test_str, "html.parser")
    stockid, stockname = sel_broker._extract_code_and_name(soup_obj)

    assert stockid == "00878"
    assert stockname == "國泰永續高股息"


def test_db():
    from Financial_data_crawler.db.clients import MongoClient
    from Financial_data_crawler.db.ChipModels import Broker_Info

    client = MongoClient("Scrapy", "sel_broker")

    assert Broker_Info.objects().count() == 85
    client.close()


def test_error_broker():

    from faker import Faker
    import scrapy

    fake = Faker()

    url = 'https://just2.entrust.com.tw/z/zg/zgb/zgb0.djhtm?a=5920&b=5920&c=E'

    class test_sel_broker(sel_broker.SelBrokerSpider):

        def start_requests(self):

            meta = {
                "BrokerCode": '5920',
                "BrokerName": '元富',
                "BranchName": '5920',
                "BranchCode": '元富',
            }

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers={"User-Agent": fake.user_agent()},
                meta=url["meta"],
            )


def test_auto_date_increase():

    date_str = "2023-2-9"
    new_date_str = sel_broker._day_move_one_day(date_str)
    new_date_plus_str = sel_broker._day_move_one_day(date_str,plus=True)

    assert new_date_str  == '2023-2-8'
    assert new_date_plus_str == '2023-2-10'

def test_nontrading_date():

    new_date = sel_broker._get_api_nontrading_day('2023-2-10')

    assert new_date == '2023-2-10'

def test_auto_date_return():

    from Financial_data_crawler.db.clients import MongoClient
    from Financial_data_crawler.db.ChipModels import Broker_Info, Broker_Transaction
    client = MongoClient("Scrapy", "sel_broker")
    earliest_transaction = Broker_Transaction.objects().order_by('Date').first()
    assert earliest_transaction['Date'] == '2023-02-09'
    client.close()




