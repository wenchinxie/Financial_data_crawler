import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_csv(url,tag_name):
    driver.get(url)
    time.sleep(2)

    wait = WebDriverWait(driver, 600)
    xpath =f"//{tag_name}[@class='csv']"
    wait.until(EC.presence_of_element_located(
                (By.XPATH, xpath)))
    element = driver.find_element(By.XPATH, xpath)
    element.send_keys("\n")
    time.sleep(2)

chromedriver_path = '/home/wenchin/airflow/dags/chromedriver'
driver = webdriver.Chrome(executable_path=chromedriver_path)
try:
    url_sets =[
        'https://www.twse.com.tw/pcversion/zh/page/trading/fund/TWT38U.html',
        'https://www.twse.com.tw/zh/trading/foreign/twt44u.html',
        'https://www.twse.com.tw/zh/trading/foreign/twt43u.html'
    ]
    tag_names =['a','button','button']
    for url,tag in zip(url_sets,tag_names):
        get_csv(url,tag)

except Exception as e:
    print(e)

driver.close()

