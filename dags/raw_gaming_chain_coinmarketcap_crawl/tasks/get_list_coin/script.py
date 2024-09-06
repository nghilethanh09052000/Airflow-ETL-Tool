from typing import List, Dict, Union, Any
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import time 
import uuid
import requests
from lxml import html
import logging

class GetListCoins(GCSDataUpload):
    def __init__(
            self, 
            gcs_bucket: str,
            gcs_prefix:str,
            ds: str,
            ti:str,
            timestamp: int,
            **kwargs
        ):

        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )

        self.ds = ds
        self.ti = ti
        self.timestamp = timestamp

        self.results = []

        self.start = 1
        self.limit = 100

    def run(self):

        while self.start:
            try:
                response = requests.get(
                    url=f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing',
                    params = {
                        "start": self.start,
                        "limit": self.limit,
                        "sortBy": "market_cap",
                        "sortType": "desc",
                        "convert": "USD,BTC,ETH",
                        "cryptoType": "all",
                        "tagType": "all",
                        "audited": False,
                        "aux": "ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,self_reported_circulating_supply,self_reported_market_cap,total_supply,volume_7d,volume_30d",
                        "tagSlugs": "gaming"
                    }
                )

                json_data = response.json()
                data = json_data.get('data')
                currencies = data.get('cryptoCurrencyList')

                if not currencies:
                    logging.info('Max API Called Request Exceed')
                    break

                self._get_data(currencies = currencies)
                self.start += self.limit

            except requests.exceptions.RequestException as e:
                raise ValueError(f"Error in API request: {e}")
            
    def _get_data(
            self,
            currencies: List[Dict[str, str]]
        ):
    
        for currency in currencies:

            cuid = currency.get('id')
            name = currency.get('name')
            symbol = currency.get('symbol')
            slug = currency.get('slug')
            tags = '-'.join(currency.get('tags'))
            cmc_rank = currency.get('cmcRank')
            market_pair_count = currency.get('marketPairCount')
            circulating_supply = currency.get('circulatingSupply')
            self_reported_circulating_supply = currency.get('selfReportedCirculatingSupply')
            total_supply = currency.get('totalSupply')
            max_supply = currency.get('maxSupply')
            ath = currency.get('ath')
            atl = currency.get('atl')
            high_24h = currency.get('high24h')
            low_24h = currency.get('low24h')
            is_active = currency.get('isActive')
            last_updated = currency.get('lastUpdated')
            date_added = currency.get('dateAdded')
            website = f"https://coinmarketcap.com/currencies/{currency.get('slug')}"
            last_quote = currency.get('quotes')[-1]

            price = last_quote.get('price')
            volume_24h = last_quote.get('volume24h')
            volume_7d = last_quote.get('volume7d')
            volume_30d = last_quote.get('volume30d')
            market_cap = last_quote.get('marketCap')
            self_reported_market_cap = last_quote.get('selfReportedMarketCap')
            percent_change_1h = last_quote.get('percentChange1h')
            percent_change_24h = last_quote.get('percentChange24h')
            percent_change_7d = last_quote.get('percentChange7d')
            percent_change_30d = last_quote.get('percentChange30d')
            percent_change_60d = last_quote.get('percentChange60d')
            percent_change_90d = last_quote.get('percentChange90d')
            fully_diluted_market_cap = last_quote.get('fullyDilluttedMarketCap')
            market_cap_by_total_supply = last_quote.get('marketCapByTotalSupply')
            dominance = last_quote.get('dominance')
            turnover = last_quote.get('turnover')
            ytd_price_change_percentage = last_quote.get('ytdPriceChangePercentage')
            percent_change_1y = last_quote.get('percentChange1y')

           

            results = {
                "cuid"                             : str(cuid),
                "name"                             : str(name),
                "symbol"                           : str(symbol),
                "slug"                             : str(slug),
                "tags"                             : str(tags),
                "cmc_rank"                         : str(cmc_rank),
                "market_pair_count"                : str(market_pair_count),
                "circulating_supply"               : str(circulating_supply),
                "self_reported_circulating_supply" : str(self_reported_circulating_supply),
                "total_supply"                     : str(total_supply),
                "max_supply"                       : str(max_supply),
                "ath"                              : str(ath),
                "atl"                              : str(atl),
                "high_24h"                         : str(high_24h),
                "low_24h"                          : str(low_24h),
                "is_active"                        : str(is_active),
                "last_updated"                     : str(last_updated),
                "date_added"                       : str(date_added),
                "website"                          : str(website),
                "price"                            : str(price),
                "volume_24h"                       : str(volume_24h),
                "volume_7d"                        : str(volume_7d),
                "volume_30d"                       : str(volume_30d),
                "market_cap"                       : str(market_cap),
                "self_reported_market_cap"         : str(self_reported_market_cap),
                "percent_change_1h"                : str(percent_change_1h),
                "percent_change_24h"               : str(percent_change_24h),
                "percent_change_7d"                : str(percent_change_7d),
                "percent_change_30d"               : str(percent_change_30d),
                "percent_change_60d"               : str(percent_change_60d),
                "percent_change_90d"               : str(percent_change_90d),
                "fully_diluted_market_cap"         : str(fully_diluted_market_cap),
                "market_cap_by_total_supply"       : str(market_cap_by_total_supply),
                "dominance"                        : str(dominance),
                "turnover"                         : str(turnover),
                "ytd_price_change_percentage"      : str(ytd_price_change_percentage),
                "percent_change_1y"                : str(percent_change_1y),
            }

            self._upload_data(results)

            logging.info(f'PAGE-------------------------------------------------------{self.start}')
            logging.info(f'RESULTS----------------------------------------------------{results}')

            self.results.append(results)

            self.ti.xcom_push(
                key = 'list_coins',
                value = self.results
            )
        

    
    def _upload_data(self, data):

        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_timestamp={self.timestamp}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
    