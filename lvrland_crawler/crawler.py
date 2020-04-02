from lxml import html
import asyncio
import aiohttp
import requests
import logging
import time
import json
import os.path
import os

lvr_url = 'https://plvr.land.moi.gov.tw/DownloadOpenData'
download_url = 'https://plvr.land.moi.gov.tw//DownloadSeason'
seasons_url = 'https://plvr.land.moi.gov.tw/DownloadSeason_ajax_list'
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format=FORMAT)


def get_request_param():
    with open('request.json') as f:
        return json.load(f)


class AsnycDownload(object):
    def __init__(self, request_obj, max_threads):
        self.requests_obj = request_obj
        self.results = []
        self.max_threads = max_threads
        self.work_queue = asyncio.Queue()

    def __download_csv(self, req, csv_content):
        try:
            filepath = req['filepath']
            filename = req['filename']
            url = req['url']
            with open(f'{filepath}{filename}', 'w') as f:
                f.write(csv_content)
            logging.debug(f'{url} downloaded')
        except Exception as e:
            raise e
        self.results.append(filename)

    async def get_body(self, req):
        async with aiohttp.ClientSession() as session:
            async with session.get(req.get('url'), timeout=30) as response:
                assert response.status == 200
                csv_content = await response.text()
                return csv_content

    async def get_results(self, req):
        csv_content = await self.get_body(req)
        if '系統發生錯誤，請洽系統管理人員' in csv_content:
            url = req['url']
            logging.error(f'{url} FILE not found')
            return 'File not found'
        self.__download_csv(req, csv_content)
        return 'Completed'

    async def handle_tasks(self, task_id):
        while not self.work_queue.empty():
            current_req = await self.work_queue.get()
            try:
                task_status = await self.get_results(current_req)
            except:
                filename = current_req['filename']
                logging.error(f'{filename} failed, start retry...')
                self.work_queue.put_nowait(current_req)

    def eventloop(self):
        [self.work_queue.put_nowait(req) for req in self.requests_obj]
        loop = asyncio.get_event_loop()
        tasks = [self.handle_tasks(task_id) for task_id in range(self.max_threads)]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()


def get_history_seasons():
    source = requests.get(seasons_url).content
    tree = html.fromstring(source)
    season_options = tree.xpath('//*[@id="historySeason_id"]/option/@value')
    return sorted(season_options)


def get_reuqest_urls(seasons, request_param):
    '''
    縣市代號 A:台北市, F:新北市, E:高雄市, B: 台中市, H:桃園市
    交易類別 A:不動產買賣, B:預售屋買賣
    '''
    for s in seasons:
        for r in request_param['params']:
            trade_type = r['trade_type']
            for c in r['citys']:
                req_url = f'{download_url}?season={s}&fileName={c}_lvr_land_{trade_type}.csv'
                year, season = s.split('S')
                filepath = f'./lvr_src/{year}/{season}/{trade_type}/{c}/'
                if not os.path.isdir(filepath):
                    os.makedirs(filepath)
                filename = f'{c}_lvr_land_{trade_type}.csv'
                yield {'filepath': filepath, 'filename': filename, 'url': req_url}


def get_target_sessons(season_options, request_param):
    start_seaion_idx = season_options.index(request_param['start_season'])
    end_season_idx = season_options.index(request_param['end_season'])
    return season_options[start_seaion_idx:end_season_idx + 1]


def run():
    season_options = get_history_seasons()
    request_param = get_request_param()
    seasons = get_target_sessons(season_options, request_param)
    logging.info(f'total request seasons: {len(seasons)}, {seasons}')
    request_obj = list(get_reuqest_urls(seasons, request_param))
    logging.info(f'total request objects: [{len(request_obj)}]')
    async_download = AsnycDownload(request_obj, 10)
    async_download.eventloop()
    logging.info(f'total downloaded: [{len(async_download.results)}]')


if __name__ == '__main__':
    start_time = time.time()
    run()
    logging.info("runtime --- %s seconds ---" % (time.time() - start_time))