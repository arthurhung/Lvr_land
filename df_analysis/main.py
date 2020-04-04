from elasticsearch import Elasticsearch, helpers
import pandas as pd
import numpy as np
import sys
import glob
import cn2an
import logging
import csv
import json

CSV_PATH = '../lvrland_crawler/lvr_src/*/*/*/*/*_lvr_land_*.csv'
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format=FORMAT)


class LvrLandAnalysis(object):
    def __init__(self):
        self.df_lvrland = self._concat_csv()

    def _convert_csv2df(self):
        '''
        將所有csv內容轉為dataframe，並將 "df_name" column 塞入
        ex: df_name = ''
        '''
        source_csv = glob.glob(CSV_PATH)
        logging.info(f'source csv: [{len(source_csv)}]')
        for s in source_csv:
            sp = s.split('/')
            year, season, city, trade_type = sp[3], sp[4], sp[5], sp[6]
            new_column = f'{year}_{season}_{city}_{trade_type}'
            temp_df_h1 = pd.read_csv(s, header=1)
            temp_df_h1['df_name'] = new_column
            yield temp_df_h1

    def _concat_csv(self):
        '''
        axis = 0 直向合併資料表
        ignore_index 忽略各自的index，重新建立index
        '''
        return pd.concat(self._convert_csv2df(), axis=0, ignore_index=True)

    def is_self_residence(self):
        '''
        主要用途為住家用
        '''
        return self.df_lvrland['main use'] == '住家用'

    def is_self_residence_building(self):
        '''
        建物型態為住宅大樓
        '''
        return self.df_lvrland['building state'].fillna('0').str.startswith('住宅大樓')

    def is_gte_floor_13(self):
        def convert_floor2int(x):
            '''
            樓層資料因包含有中文數字+層數, 數字, 地下層，將其轉為integer
            '''
            if type(x) == str:
                if x.isdigit():
                    return int(x)
                else:
                    if x != '地下層':
                        x = cn2an.cn2an(x.strip("層"))
                        return x
            elif type(x) == int:
                return x
            else:
                logging.error(f'convert_floor2int fail value: [{x}], type: [{type(x)}]')
            return 0

        return self.df_lvrland['total floor number'].fillna(0).map(convert_floor2int) >= 13

    def count_total_case(self, df):
        '''
        總件數
        '''
        return df.shape[0]

    def count_total_berth(self, df):
        '''
        總車位數(透過交易筆棟數)
        '''
        return df['transaction pen number'].map(lambda x: int(x.split('車位')[-1])).sum()

    def count_avg_total_price(self, df):
        '''
        平均總價元(四捨五入)
        '''
        return df['total price NTD'].mean(0).round()

    def count_avg_total_berth_price(self, df):
        '''
        平均車位總價元(有車位才算，價格大於0)
        '''
        return df[df['the berth total price NTD'] > 0]['the berth total price NTD'].mean().round()

    def df2csv(self, df, filename):
        try:
            pd.DataFrame(df).to_csv(
                f'{filename}',
                index=0,
                encoding='utf_8_sig',
            )
            logging.info(f'save succeed: {filename}')
            return f'save succeed: {filename}'
        except:
            logging.error(f'save FAIL: {filename}', exc_info=True)
            return f'save FAIL: {filename}'


def get_csv_mapping():
    with open('mapping.json') as f:
        return json.load(f)


def csv_to_es(filename):
    '''
    csv 傳送至 Elasticsearch
    '''
    es = Elasticsearch(['localhost'], port=9200)
    mapping = get_csv_mapping()
    es.indices.create(
        index="csv_data",
        body=mapping,
        ignore=400,  # ignore 400 already exists code
    )
    with open(filename) as f:
        reader = csv.DictReader(f)
        helpers.bulk(
            es,
            reader,
            index='csv_data',
            raise_on_error=False,
        )


if __name__ == '__main__':
    # filter.csv
    try:
        args = sys.argv[1]
    except:
        args = 'main'

    if args != 'update':
        lvr_land = LvrLandAnalysis()
        is_self_residence = lvr_land.is_self_residence()
        is_self_residence_building = lvr_land.is_self_residence_building()
        is_gte_floor_13 = lvr_land.is_gte_floor_13()
        df_residence_gte_13 = lvr_land.df_lvrland[(is_self_residence & is_self_residence_building & is_gte_floor_13)]
        logging.info(f'filer dataframe shape: {df_residence_gte_13.shape}')
        lvr_land.df2csv(df_residence_gte_13, 'filter.csv')

        # count.csv
        total_case = lvr_land.count_total_case(df_residence_gte_13)
        total_berth = lvr_land.count_total_berth(df_residence_gte_13)
        avg_total_price = lvr_land.count_avg_total_price(df_residence_gte_13)
        avg_total_berth_price = lvr_land.count_avg_total_berth_price(df_residence_gte_13)
        count_data = {
            '總件數': [total_case],
            '總車位數': [total_berth],
            '平均總價元': [avg_total_price],
            '平均車位總價元': [avg_total_berth_price],
        }
        lvr_land.df2csv(count_data, 'count.csv')

    # 'filter.csv' 傳送至 Elasticsearch
    logging.info('send csv to Elasticsearch...')
    target_file = 'filter.csv'
    csv_to_es(target_file)
    logging.info(f'{target_file} to Elasticsearch succeed')