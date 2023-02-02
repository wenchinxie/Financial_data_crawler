# Deal with files
import os
import zipfile
import shutil

# For data cleaning
from bs4 import BeautifulSoup
from pathlib import Path
import multiprocessing

from loguru import logger

from Financial_data_crawler import config_setup

config = config_setup.config


class TWFinancialReport:

    def __init__(self):
        self.base_dir = config.get('BaseDir', 'Base')
        self.html_dir = os.path.join(self.base_dir, 'html_files')

    def get_data(self):
        """
        Users have to download xbrl files first into the assigned directory
        """
        self.unzip()
        directory = self.html_dir
        files = [os.path.join(self.html_dir, f) for f in os.listdir(directory)
                 if os.path.isfile(os.path.join(directory, f))]

        with multiprocessing.Pool() as pool:
            dataset = pool.map(TWFinancialReport.run_process, files)

        return dataset

    @staticmethod
    def run_process(file):
        with open(file, encoding="UTF-8") as f:
            data = f.read()
        soup = BeautifulSoup(data, 'html.parser')

        # Basic Info
        filename = Path(file).stem.split('-')  # Use Path to distinguish file name
        stockid = filename[-2]
        year = filename[-1][:4]
        quarter = filename[-1][4:]
        quarter_mapping = {
            'Q1': '03-31',
            'Q2': '05-15',
            'Q3': '08-15',
            'Q4': '11-15'
        }
        date = year + '-' + quarter_mapping[quarter]
        financial_report = {'StockID': stockid,
                            'Year': int(year),
                            'Quarter': quarter,
                            'Type': 'TW',
                            'Date': date}

        sht_mapping = {
            'Balance Sheet': 'BalanceSheet',
            'Statement of Comprehensive Income': 'IncomeStatement',
            'Statements of Cash Flows': 'CashFlowsStatement'
        }

        tables = soup.find_all('table')
        PROBLEM_TABLE = False
        for i, table in enumerate(tables[:3]):
            try:
                sht = table.find('tr').find('th').find(class_='en')

                if sht is not None:
                    sht = sht.text.strip()
                    sht = sht_mapping[sht]
                    res = TWFinancialReport.BS_PL_CF_Parse(table)
                else:
                    sht = list(sht_mapping.values())[i]
                    res = {}

            except Exception as e:
                logger.error(e)
                logger.info(file)

                sht = list(sht_mapping.values())[i]
                res = {}
                PROBLEM_TABLE = True

            finally:
                financial_report[sht] = res

        if PROBLEM_TABLE:
            TWFinancialReport.move_problem_file(file)
        else:
            TWFinancialReport.move_completed_file(file)

        return financial_report

    @staticmethod
    def BS_PL_CF_Parse(table, REQUIRE_COL=1) -> dict:
        """
        For Balance Sheet, Profit & Loss Statement and Cash Flows Statement Only
        Set Require_col=1 for future unique use if I want to dissolve something
        """

        d_list = [{} for _ in range(2, 2 + REQUIRE_COL)]
        res = [{} for _ in range(2, 2 + REQUIRE_COL)]

        infos = table.find_all('tr')
        periods = [time.find(class_='en').text for time in infos[1].find_all('th')[2:2 + REQUIRE_COL]]

        stack = []

        # Cos I need to detect if the next one is a subject(ex:Asset or Liability)
        # restrict the items to the last second one
        # Start from the second one because the first item is category and second one is period
        for i, n in enumerate(infos[2:-2]):
            code = n.find('td').text
            next_code = infos[i + 3].find('td').text
            if code == '' or code.count('\u3000') < next_code.count('\u3000'):
                # If there isn't any code, then use stack to record the category
                # Some doesn't have subjects, so compare the tab to record the category
                en_subject = n.find(class_='en').text.strip().lower()
                zh_subject = n.find(class_='zh').text.strip()
                stack.append([en_subject, zh_subject])
            else:

                # Add the subject information and amount to dictionary
                # To restrict the list length to the time window we want
                amts = [amt.get_text().strip() for amt in n.select('.amt')[:len(d_list)]]

                for d, amt in zip(d_list, amts):
                    en_subject = n.select(f'.en')[0].get_text().strip().lower()
                    zh_subject = n.select(f'.zh')[0].get_text().strip()
                    d[code] = {'English Subject': en_subject,
                               'Chinese Subject': zh_subject,
                               'Amt': amt}

            if infos[i + 3].find('td').text == '' and not d_list[0] == {}:

                # If the next item is a subject, which means the category ended, then record the category to current
                # dictionary
                for record, cur in zip(res, d_list):
                    TWFinancialReport.record_value(record, cur, stack)

                stack.pop()

                d_list = [{} for _ in range(2, 2 + REQUIRE_COL)]
                if infos[i + 3].find(class_='en').text.count('\u3000') == 0:
                    stack = []

        # Record each period
        for r, p in zip(res, periods):
            r['Period'] = p

        if REQUIRE_COL == 1:
            res = res[0]
        return res

    @staticmethod
    def record_value(record, cur, stack):
        if stack == []:
            key = list(cur.keys())[0]
            cur[key]['English Subject']
            stack.append((cur[key]['English Subject'], cur[key]['Chinese Subject']))

        for i in range(len(stack)):
            en_subject = stack[i][0]
            zh_subject = stack[i][1]
            if i == len(stack) - 1:
                cur['Chinese Subject'] = zh_subject
                record[en_subject] = cur

            else:
                if stack[i][0] not in record:
                    record[en_subject] = {'Chinese Subject': zh_subject}
                record = record[en_subject]

    @staticmethod
    def unzip():
        """
        Unzip files and move those zip files to assigned directory
        """
        base_dir = config.get('BaseDir', 'Base')
        html_dir = os.path.join(base_dir, 'html_files')
        todo_dir = os.path.join(base_dir, 'todo')
        completed_zipdir = os.path.join(base_dir, 'completedzip')

        for root, dirs, files in os.walk(todo_dir):
            for file in files:
                if file.endswith('.zip'):
                    zip_path = os.path.join(root, file)
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(html_dir)

                    # move the completed zip files to the assigned folder instead of deleting
                    # will update some tables inside the html files in the future
                    shutil.move(zip_path, completed_zipdir)

    @staticmethod
    def move_completed_file(file):
        base_dir = config.get('BaseDir', 'Base')
        completed_dir = os.path.join(base_dir, 'completed')
        shutil.move(file, os.path.join(completed_dir, os.path.basename(file)))

    @staticmethod
    def move_problem_file(file):
        base_dir = config.get('BaseDir', 'Base')
        completed_dir = os.path.join(base_dir, 'problem')
        shutil.move(file, os.path.join(completed_dir, os.path.basename(file)))
