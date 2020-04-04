# Lvr_land

1. 爬蟲(內政部不動產時價登錄網)
2. pandas分析

## Requirements

`pip install -r requirements.txt`

## Usage

### Crawler

`python lvrland_crawler/crawler.py`

#### request.json 為查詢範圍的參數

- start_season 查詢開始年度season

- end_season 查詢結束年度season

- params 查詢物件

- trade_type 
A：不動產買賣
B：預售屋買賣
C：不動產租賃

Example:

```json
{
    "start_season": "103S1",
    "end_season": "108S2",
    "params": [
        {
            "trade_type": "A",
            "citys": [
                "A",
                "E",
                "F"
            ]
        },
        {
            "trade_type": "B",
            "citys": [
                "B",
                "H"
            ]
        }
    ]
}
```

### Docker ELK

`docker compose -f docker_elk/docker-compose-elk.yml up`

### Analysis Data

產出 filter.csv, count.csv
並將 filter.csv 傳送至 Elasticsearch

執行完整流程
`python df_analysis/main.py`

只做傳送csv
`python df_analysis/main.py update`

filter.csv

- 主要用途為【住家用】 
- 建物型態為【住宅大樓】 
- 總樓層數需【大於等於十三層】

count.csv

- 計算【總件數】
- 計算【總車位數】(透過交易筆棟數) 
- 計算【平均總價元】
- 計算【平均車位總價元】

### Notes

- 刪除 Elasticsearch csv 檔案
`curl -XDELETE "localhost:9200/csv*"`