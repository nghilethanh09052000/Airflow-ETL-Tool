from typing import Dict, List, Sequence, Any, Union
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from sensor_tower.scripts.utils import (
    CATEGORY_IDS
)
from dataclasses import dataclass
from enum import Enum


COUNTRY_CODES = {
    "AE": "United Arab Emirates",
    "AR": "Argentina",
    "AT": "Austria",
    "AU": "Australia",
    "BE": "Belgium",
    "BR": "Brazil",
    "CA": "Canada",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CO": "Colombia",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "EC": "Ecuador",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "GR": "Greece",
    "HK": "Hong Kong",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IN": "India",
    "IT": "Italy",
    "JP": "Japan",
    "KR": "South Korea",
    "LU": "Luxembourg",
    "MT": "Malta",
    "MX": "Mexico",
    "MY": "Malaysia",
    "NL": "Netherlands",
    "NO": "Norway",
    "NZ": "New Zealand",
    "PA": "Panama",
    "PE": "Peru",
    "PH": "Philippines",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "RU": "Russia",
    "SA": "Saudi Arabia",
    "SE": "Sweden",
    "SG": "Singapore",
    "SK": "Slovakia",
    "TH": "Thailand",
    "TR": "Turkey",
    "TW": "Taiwan",
    "UA": "Ukraine",
    "US": "US",
    "VN": "Vietnam",
    "ZA": "South Africa"
}



NETWORKS = [
    "Adcolony",
    "Admob",
    "Applovin",
    # "BidMachine",
    "Chartboost",
    # "Digital_Turbine",
    #"Facebook",
    #"InMobi",
    #"Instagram",
   #"Meta_Audience_Network",
   # "Mintegral",
    "Mopub",
    #"Pinterest",
    #"Smaato",
    "Snapchat",
    "Supersonic",
    #"Tapjoy",
    "TikTok",
    #"Twitter",
    "Unity",
    #"Verve",
    "Vungle",
    #ß"Youtube"
]


AD_TYPES = [
    'banner',
    'full_screen',
    'video',
    'playable'
]

class TimePeriod(Enum):
    WEEK = 'week'
    MONTH = 'month'
    QUARTER = 'quarter'


@dataclass
class CreativesTopsEndpointParams:
    os: str
    date: str
    period: str
    category: str
    country: str
    network: str
    ad_types: str
    limit: int
    page: int
    # placement: str

    def __post_init__(self):
        
        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')

        if self.period not in [data.value for data in TimePeriod]:
                raise ValueError('Time Period Must Be In This List', [data.value for data in TimePeriod])
        
        if self.os in ['ios', 'unified'] and self.category not in list(CATEGORY_IDS.get('ios').keys()):
            raise ValueError(
                "Please Specify Category on this list:", 
                list(CATEGORY_IDS.get('ios').keys())
            )

        if self.os == 'android' and self.category not in list(CATEGORY_IDS.get('android').keys()):
            raise ValueError(
                "Please Specify Android Category on this list:", 
                list(CATEGORY_IDS.get('ios').keys())
            )

        if self.country not in list(COUNTRY_CODES.keys()):
            raise ValueError('Invalid Country. Please specify a country from the list of', list(COUNTRY_CODES.keys()))

        if self.network not in NETWORKS:
            raise ValueError('Your network is not in the list:', NETWORKS)
        
        if self.ad_types not in AD_TYPES:
            raise ValueError('Your ad types is not in the list:', AD_TYPES)
        
        if self.limit and (self.limit < 0 or self.limit > 250):
            raise ValueError('Invalid Limit number specified. Limit must be a specific number or not be exceed 250')

        if self.page and (self.page < 0):
            raise ValueError('Invalid Limit Page Specified. Page Number Must be a positive number')


@dataclass
class CreativesTopsEndpointData:
    network           : str
    ad_type           : str
    first_seen_at     : str
    last_seen_at      : str
    app_id            : str
    canonical_country : str
    name              : str
    publisher_name    : str
    publisher_id      : str
    humanized_name    : str
    icon_url          : str
    os                : str
    date              : str
    country           : str
    creative_id       : str
    creative_url      : str
    preview_url       : str
    thumb_url         : str
    html_url          : str
    video_duration    : str
    width             : str
    height            : str
    title             : str
    button_text       : str
    message           : str


class CreativesTopsEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds"
    )
     
    def __init__(
        self,
        ds: str,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        http_conn_id: str,
        os: List[str],
        period: str,
        categories: List[Union[str, int]],
        country: str,
        networks: List[str],
        ad_types: List[str],
        limit: int,
        page: int,
        placement: str,
        **kwargs
    ):
    
        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/ad_intelligence/top_creatives/creative_tops",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds

        self.os = os
        self.period = period
        self.categories = categories
        self.country = country
        self.networks = networks
        self.ad_types = ad_types
        self.limit = limit
        self.page = page
        self.placement = placement

    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)
    
    def fetch_data(self):
        results = []
        for os in self.os:
            categories = self._format_categories(os=os, categories=self.categories)
            for category in categories:
                for network in self.networks:
                    for ad_type in self.ad_types:
                        params = self._format_params(os=os, network=network, ad_type=ad_type, category = category)
                        data = self.api.get_creatives_tops_data(base_params=params)
                        results.extend(data)
        return self._format_response_result(results=results)

    def _format_categories(
        self,
        os: str,
        categories: List[Union[str, int]]
    ) -> List[str]:
        return [
            str(category) for category in categories
            if (
                    (os in ['ios','unified']) and isinstance(category, int)
                ) 
                or
                (
                    os == 'android' and isinstance(category, str)
                )
        ]
    
    def _format_params(
            self,
            os: str,
            network: str,
            ad_type: str,
            category:str
        ) -> CreativesTopsEndpointParams:
        return CreativesTopsEndpointParams(
                os=os,
                date=self.ds,
                period=self.period,
                category=category,
                country=self.country,
                network=network,
                ad_types=ad_type,
                limit=self.limit,
                page=self.page,
                # placement=self.placement
            ).__dict__

    def _format_response_result(
        self, 
        results: List
    ) -> List:
        
        if not results: return results

        data = []

        for result in results:
            creatives = result.get('creatives')
            for creative in creatives:
                data.append(
                    CreativesTopsEndpointData(
                        network           = str(result.get('network')),
                        ad_type           = str(result.get('ad_type')),
                        first_seen_at     = str(result.get('first_seen_at')),
                        last_seen_at      = str(result.get('last_seen_at')),
                        app_id            = str(result.get('app_info').get('app_id')),
                        canonical_country = str(result.get('app_info').get('canonical_country')),
                        name              = str(result.get('app_info').get('name')),
                        publisher_name    = str(result.get('app_info').get('publisher_name')),
                        publisher_id      = str(result.get('app_info').get('publisher_id')),
                        humanized_name    = str(result.get('app_info').get('humanized_name')),
                        icon_url          = str(result.get('app_info').get('icon_url')),
                        os                = str(result.get('app_info').get('os')),
                        date              = self.ds,
                        country           = self.country,
                        creative_id       = str(creative.get('id')),
                        creative_url      = str(creative.get('creative_url')),
                        preview_url       = str(creative.get('preview_url')),
                        thumb_url         = str(creative.get('thumb_url')),
                        html_url          = str(creative.get('html_url')),
                        video_duration    = str(creative.get('video_duration')),
                        width             = str(creative.get('width')),
                        height            = str(creative.get('height')),
                        title             = str(creative.get('title')),
                        button_text       = str(creative.get('button_text')),
                        message           = str(creative.get('message')),
                    ).__dict__
                )

        return data






