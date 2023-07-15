from typing import Dict
import datetime
import re
import chardet

import httpx
from faker import Faker
from bs4 import BeautifulSoup
import pandas as pd


def autodetect(content):
    return chardet.detect(content).get("encoding")

class BaseClient:
    def __init__(self):
        self.client = httpx.Client(default_encoding=autodetect)
        self.header = Faker().user_agent()

    def _parse_html(self,url):
        response = self.client.get(url, headers={"User-Agent": self.header})
        self.check_status_code(response)
        return BeautifulSoup(response.text, "html.parser")

    @staticmethod
    def check_status_code(response):
        if response.status_code != 200:
            raise ValueError("Fail to get the target url")


class HTTPClient(BaseClient):
    def __init__(self):
        super().__init__()

    def get_raw_data(self, api_url: str):
        if not isinstance(api_url, str) or not api_url.startswith("http"):
            raise ValueError(f"Not Valid URL format: {api_url}")

        response = self.client.get(api_url, headers={"User-Agent": self.header})
        self.check_status_code(response)

        return response

    def get_market_day(self):
        soup = self._parse_html(r"https://tw.stock.yahoo.com/quote/2330.TW")
        market_day = soup.find("span", {"class": "C(#6e7780) Fz(12px) Fw(b)"}).text
        market_day = market_day.split(" ")[2]

        if market_day == "-":
            return datetime.datetime.today().strftime("%Y-%m-%d")

        return market_day


class IndScraper(BaseClient):
    BASE_URL = "https://ic.tpex.org.tw/introduce.php"

    def __init__(self):
        super().__init__()
        self._industry_data = self._extract_industries()
        self._chains_data = None

    def scrape_data(self):
        all_chains_df, all_comps_df = self._get_all_chains_data()
        all_inds_comp_df = all_comps_df.merge(all_chains_df, on=['IndCode'], how='left')
        return all_inds_comp_df

    def _get_all_chains_data(self):
        # Get all industries data
        all_chains_data = []
        all_comps_data = []
        for industry_code in self._industry_data.keys():
            soup = self._parse_html(f'{self.BASE_URL}?ic={industry_code}')
            try:

                chains_data, comps_data = self._extract_chains_and_companies(soup, industry_code)
                all_chains_data.extend(chains_data)
                all_comps_data.extend(comps_data)
            except Exception as e:
                raise ValueError(e)

        return pd.DataFrame(all_chains_data), pd.DataFrame(all_comps_data)

    def _extract_chains_and_companies(self, soup, industry_code):
        # Set the data foramt as a list of dictionaries

        chain_data_list = []
        company_data_list = []

        chains = soup.find_all(class_='chain')
        chain_data = {}
        chain_data['MainIndCode'] = industry_code
        chain_data['MainIndName'] = self._industry_data[industry_code]

        if chains:
            for chain in chains:
                chain_data_list.extend(self._extract_chain_industries(chain_data, chain))
        else:
            chain_data_list.extend(self._extract_chain_industries(chain_data, soup))

        for chain_data in chain_data_list:
            company_data_list.extend(self._extract_companies(soup, chain_data))

        return chain_data_list, company_data_list

    @staticmethod
    def _extract_chain_industries(chain_data, chain, discrete=False):
        all_ind_data = []
        for comp_groups in chain.find_all(class_='chain-company'):
            for comp in comp_groups.find_all(class_='company-chain-panel'):
                ind_data = chain_data.copy()
                ind_data['IndCode'] = comp['id'].split('_')[-1]
                ind_data['IndName'] = comp.text.strip()
                all_ind_data.append(ind_data)
        return all_ind_data

    @staticmethod
    def _extract_stream(chain):
        # Extract which stream the subindustry is in
        panel_text = chain.find(class_='chain-title-panel')
        return panel_text.text.strip() if panel_text else ''

    def _extract_companies(self, soup, chain):
        """ There are 2 situations:
        1. There is no subindustry, we can just parse from its ordinary industry
        2. There is subindustry, we need to loop into the panel, and then parse from its subindusry
        """
        comps = []
        ind_code = chain['IndCode']
        sc_ind_pnl = soup.find(id=f'sc-ind-pnl_{ind_code}')

        if sc_ind_pnl:
            sub_inds = self._extract_sub_industries(sc_ind_pnl)
            for sub_ind in sub_inds.keys():
                comps.extend(
                    self._get_company_list(soup.find(id=f'sc_company_{sub_ind}'), ind_code, sub_inds[sub_ind], sub_ind))

        else:
            company_list = soup.find(id=f"companyList_{ind_code}")
            if not company_list:
                return []
            comps.extend(self._get_company_list(company_list, ind_code))

        return comps

    def _get_company_list(self, company_list_soup, ind_code, sub_ind=None, sub_ind_code=None):
        comps = []
        for comp in company_list_soup.find_all(class_='company-text-over'):
            comp_info = self._get_company_info(comp, ind_code, sub_ind, sub_ind_code)
            if comp_info:
                comps.append(comp_info)

        return comps

    @staticmethod
    def _get_company_info(comp, ind_code, sub_ind=None, sub_ind_code=None):
        """Helper function to create a dictionary with company information"""
        if comp.get('href') and re.search(r'\d{4}$', comp['href']):
            return {
                'IndCode': ind_code,
                'SubInd': sub_ind,
                'SubIndCode': sub_ind_code,
                'Code': comp['href'][-4:],
                'Name': comp.text.strip()
            }

    @staticmethod
    def _extract_sub_industries(sc_ind_pnl):
        return {subchain['id'].split('_')[-1]: subchain.text.split('(')[0].replace('\xa0', '') for subchain in
                sc_ind_pnl.find_all(class_=re.compile(r'^subchain'))}

    def _extract_industries(self):
        soup = self._parse_html(self.BASE_URL)
        return {ind['value']: ind.text.strip() for ind in soup.find(id='ic_option').find_all('option')}
