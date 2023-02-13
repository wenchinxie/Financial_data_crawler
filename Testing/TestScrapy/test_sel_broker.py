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
    soup_obj = BeautifulSoup(test_str,'html.parser')
    stockid,stockname = sel_broker._extract_code_and_name(soup_obj)

    assert stockid == 'AS1795'
    assert stockname == '美時'

def test_etf_case():
    test_str = '''
        <td class="t4t1" id="oAddCheckbox" nowrap="">
        <a href="javascript:Link2Stk('00878');">00878國泰永續高股息</a>
        </td>
        '''

    soup_obj = BeautifulSoup(test_str, 'html.parser')
    stockid, stockname = sel_broker._extract_code_and_name(soup_obj)

    assert stockid == '00878'
    assert stockname == '國泰永續高股息'

def test_db():
    from Financial_data_crawler.db.clients import MongoClient
    from Financial_data_crawler.db.ChipModels import Broker_Info
    client = MongoClient("Scrapy", "sel_broker")

    assert Broker_Info.objects().count()==85
    client.close()



