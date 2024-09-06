from typing import List, Dict, Union, Any
from enum import Enum
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import time 
import random
import uuid
import requests
from lxml import html
import json



class DateRangeParams(Enum):
    ONE_DAY   = '1D'
    SEVEN_DAY = '7D'
    ONE_MONTH = '1M'
    ONE_YEAR  = '1Y'
    ALL       = 'ALL'

class ExtractCoinMarketCapHtmlData:

    @staticmethod
    def extract(html_content: str) -> Dict[str, str]:

        tree = html.fromstring(html_content)

        cuid                             = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-role="chip-content-item"]/text()').strip()
        market_cap                       = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[1]/div/dd/text()')
        volume_24h                       = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[2]/div/dd/text()')
        volume_market_cap_24h            = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[3]/div/dd/text()')
        self_reported_circulating_supply = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[4]/div/dd/text()')
        total_supply                     = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[5]/div/dd/text()')
        max_supply                       = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[6]/div/dd/text()')
        fully_diluted_market_cap         = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[1]/div/dl/div[7]/div/dd/text()')
        contracts                        = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//script[@id="__NEXT_DATA__"]/text()')
        rating                           = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//div[@data-module-name="Coin-stats"]/div/section[2]/div/div[2]/div[4]/div[2]/span/text()')
        symbols                          = ExtractCoinMarketCapHtmlData._extract_single_value(tree, '//span[@data-role="coin-symbol"]/text()')
        
        return {
            'cuid'                             : str(cuid),
            'symbols'                          : str(symbols),
            'market_cap'                       : str(ExtractCoinMarketCapHtmlData._format_dolar_symbol_value(market_cap)),
            'volume_24h'                       : str(ExtractCoinMarketCapHtmlData._format_dolar_symbol_value(volume_24h)),
            'volume_market_cap_24h'            : str(ExtractCoinMarketCapHtmlData._format_percentage_symbol_value(volume_market_cap_24h)),
            'self_reported_circulating_supply' : str(ExtractCoinMarketCapHtmlData._format_currency_symbol_value(self_reported_circulating_supply)),
            'total_supply'                     : str(ExtractCoinMarketCapHtmlData._format_currency_symbol_value(total_supply)),
            'max_supply'                       : str(ExtractCoinMarketCapHtmlData._format_currency_symbol_value(max_supply)),
            'fully_diluted_market_cap'         : str(ExtractCoinMarketCapHtmlData._format_dolar_symbol_value(fully_diluted_market_cap)),
            'rating'                           : str(rating.strip()),
            'contracts'                        : str(ExtractCoinMarketCapHtmlData._get_contracts_data(contracts))
            
        }

    def _extract_single_value(tree, xpath_expression):
        result = tree.xpath(xpath_expression)
        if result:
            return result[0]
        else:
            return ''
    
    def _format_dolar_symbol_value(value: str) -> str:
        return str(value.replace('$', '') \
                            .replace(',', '')) \
                            if '$' in value else None
    
    def _format_percentage_symbol_value(value: str) -> str:
        return str(value.replace('%', '')) if '%' in value else None 
    
    def _format_currency_symbol_value(value: str) -> str:
        return ''.join(c for c in value if c.isdigit())
    
    def _get_contracts_data(data) -> List[Dict[str,str]]:

        json_data = json.loads(data)
        platforms = json_data.get('props', None) \
                            .get('pageProps', None) \
                                .get('detailRes', None) \
                                    .get('detail', None) \
                                        .get('platforms', None)
        
        return [
            {
                'contract_id'      : str(platform.get('contractId')) if platform.get('contractId') else None,
                'contract_address' : str(platform.get('contractAddress')) if platform.get('contractAddress') else None,
                'contract_platform': str(platform.get('contractPlatform')) if platform.get('contractPlatform') else None,
            } for platform in platforms
        ]
        
class ExtractCoinMarketCapApiData:
    
    @staticmethod
    def extract_one_day_price(
        api_data: Dict[str, Any]
    ):
        points = api_data.get('data', None).get('points', None)

        if not points:
            return {
                'price_1D': '',
                'volume_24h_1D': ''
            }

        last_key = list(points.keys())[-1]

        current_price: List[str] = points.get(last_key).get('v')

        return {
            'price_1D': str(current_price[0]),
            'volume_24h_1D': str(current_price[1])
        }
    
    @staticmethod
    def extract_all_price(
        api_data: Dict[str, Any]     
    ):
        points = api_data.get('data', None).get('points', None)

        if not points:
            return [
                {
                    'timestamp' : '',
                    'price'     : '',
                    'volume_24h': ''
                }
            ]
        
        return [
            {
                'timestamp' : str(point),
                'price'     : str(points.get(point).get('v')[0]),
                'volume_24h': str(points.get(point).get('v')[1])
            } for point in list(points.keys())
        ]

class CoinMarketCapApiBuilder:

    def __init__(
            self,
            cuid: str,
            date_range_param: DateRangeParams
        ) -> None:
        
        self.cuid = cuid
        self.date_range_param = date_range_param
        self.api = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart'
    
    @property
    def build(self) -> List[Union[str, Dict[str, str]]]:
        return [
            self.api, 
            {
                'id': self.cuid,
                'range': self.date_range_param
            }
        ]

class GetCoinMarketCapGamingData(
    GCSDataUpload
):

    def __init__(
        self, 
        website: str,
        day_range: str,
        ds:str,
        timestamp: int,
        gcs_bucket: str,
        gcs_prefix:str,
        **kwargs
    ):

        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )

        self.website = website
        self.day_range = day_range
        self.ds = ds
        self.timestamp = timestamp

    def execute_task(self):

        delay_seconds = random.uniform(1, 10)
        time.sleep(delay_seconds)

        """Page Source Data"""
        html_content = self._get_html_source_data()
        html_extractor = ExtractCoinMarketCapHtmlData()
        extracted_html_data = html_extractor.extract(html_content)

        followers = self._get_followers(
            cuid=extracted_html_data.get('cuid'),
        )

        if self.day_range == DateRangeParams.ONE_DAY.value:
            one_day_params_builder = CoinMarketCapApiBuilder(
                                cuid=extracted_html_data.get('cuid'),
                                date_range_param=DateRangeParams.ONE_DAY.value      
                            )
            one_day_api, one_day_params = one_day_params_builder.build
            one_day_api_data = self._get_api_data(api=one_day_api, params=one_day_params)
            one_day_api_extractor = ExtractCoinMarketCapApiData()
            extracted_one_day_api_data = one_day_api_extractor.extract_one_day_price(api_data=one_day_api_data)
            data = {
                **extracted_html_data,
                **extracted_one_day_api_data,
                'followers': followers,
                'timestamp': int(self.timestamp)
            }
        
        elif self.day_range == DateRangeParams.ALL.value or self.day_range == DateRangeParams.ONE_YEAR.value:
            all_day_params_builder = CoinMarketCapApiBuilder(
                                cuid=extracted_html_data.get('cuid'),
                                date_range_param=self.day_range
                            )
            all_day_api, all_day_params = all_day_params_builder.build
            all_day_api_data = self._get_api_data(api=all_day_api, params=all_day_params)
            all_day_api_extractor = ExtractCoinMarketCapApiData()
            prices = all_day_api_extractor.extract_all_price(api_data=all_day_api_data)
            data = []
            for price in prices:
                data.append(
                    {
                        'cuid'      : extracted_html_data.get('cuid'),
                        'timestamp' : price.get('timestamp'),
                        'price'     : price.get('price'),
                        'volume_24h': price.get('volume_24h')
                    }
                )
        self._upload_data(data=data)

    def _get_followers(self, cuid: str):

        session = requests.Session()

        first_response = session.post(
            url='https://api-gravity.coinmarketcap.com/live/v3/live/get-online',
            data={'cryptoId': int(cuid)},
            headers={
                'accept': 'application/json, text/plain, */*',
                'origin': 'https://coinmarketcap.com',
                'platform': 'web',
                'referer': 'https://coinmarketcap.com/'
            }
        )

        # Extract CSRF token from the first response
        csrf_token = first_response.cookies.get('x-csrf-token')

        # Perform the second request using the same session
        second_response = session.post(
            url='https://api-gravity.coinmarketcap.com/gravity/v3/gravity/announcement/query',
            json={'cryptoId': int(cuid)},
            headers={
                'authority': 'api-gravity.coinmarketcap.com',
                'method': 'POST',
                'path': '/gravity/v3/gravity/announcement/query',
                'scheme': 'https',
                'accept': 'application/json, text/plain, */*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'content-length': '18',
                'content-type': 'application/json',
                'languagecode': 'en',
                'origin': 'https://coinmarketcap.com',
                'platform': 'web',
                'referer': 'https://coinmarketcap.com/',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                'x-request-id': '211f03ace73c48f7a82ccee649add6ab',
                'x-csrf-token': csrf_token,
                'cookie': f'bnc-uuid=09924dca-9479-4eee-8ba9-e827c761401c; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2218ada5de3803be-0dcdb6893c548f8-4c6d127c-3686400-18ada5de3811d6d%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThhZGE1ZGUzODAzYmUtMGRjZGI2ODkzYzU0OGY4LTRjNmQxMjdjLTM2ODY0MDAtMThhZGE1ZGUzODExZDZkIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2218ada5de3803be-0dcdb6893c548f8-4c6d127c-3686400-18ada5de3811d6d%22%7D; cmc-theme=day; _gid=GA1.2.2049995729.1702263091; _ga=GA1.2.573874484.1695880702; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Dec+15+2023+17%3A55%3A23+GMT%2B0700+(Indochina+Time)&version=202303.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false&consentId=3c3a3be9-c220-4f35-8602-f9ce83612fc4&interactionCount=0; _dc_gtm_UA-40475998-1=1; x-csrf-token={csrf_token}; _ga_VZT5E68L14=GS1.1.1702637470.96.0.1702637724.0.0.0'
            }
        )
        session.close()
        
        data = second_response.json().get('data')

        owner = data.get('owner')
        return owner.get('followers') if owner else ''
   
    def _get_html_source_data(self):
        headers = { 
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding' :'gzip, deflate, br',
        }
        try:
            html_source_data = requests.get(self.website, headers=headers)
            return html_source_data.content
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Error in API request: {e}")
    
    def _get_api_data(
            self,
            api:str,
            params: Dict[str, str]
        ) -> Dict[str, Any]:
        try:
            data = requests.get(api, params=params)
            return data.json()
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Error in API request: {e}")
    
    def _upload_data(self, data):

        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_timestamp={str(self.timestamp)}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
    







