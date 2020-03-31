from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from lxml import html
import requests
import time

lvr_url = 'https://plvr.land.moi.gov.tw/DownloadOpenData'
download_url = 'https://plvr.land.moi.gov.tw//DownloadSeason'
chrome_driver = '/Users/arthur/Documents/Lvr_land/lvrland_crawler/chromedriver'


def get_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome('../chromedriver', chrome_options=chrome_options)
    return driver


def get_history_seasons():
    driver = get_driver()
    driver.implicitly_wait(15)
    driver.get(lvr_url)
    driver.execute_script(
        '''$('.tabOpenDataCss').empty();loadAjaxUrl('DownloadHistory_ajax_list','tab_opendata_history_content');''')
    options = [i.get_attribute('value') for i in driver.find_elements_by_xpath('//*[@id="historySeason_id"]/option')]
    # 取出【103年第1季】~【108年第2季】
    seasons = sorted(options)[7:-2]
    driver.quit()
    return seasons


def download_csv(seasons, request_param):
    '''
    縣市代號 A:台北市, F:新北市, E:高雄市, B: 台中市, H:桃園市
    交易類別 A:不動產買賣, B:預售屋買賣
    '''
    # key為交易類別，value為縣市代號array
    for s in seasons:
        for trade_type, citys in request_param.items():
            for c in citys:
                req_url = f'{download_url}?season={s}&fileName={c}_lvr_land_{trade_type}.csv'
                print(f'request url: {req_url}')
                file = requests.get(req_url)
                year, season = s.split('S')
                # print(f'year: {year}, season: {season}')
                filename = f'./lvr_src/{year}_{season}_{c}_{trade_type}.csv'
                open(filename, 'wb').write(file.content)
                print(f'{filename} downloaded')


def run():
    seasons = get_history_seasons()
    request_param = {
        'A': ['A', 'E', 'F'],
        'B': ['B', 'H'],
    }
    download_csv(seasons, request_param)


if __name__ == '__main__':
    run()
    # pass