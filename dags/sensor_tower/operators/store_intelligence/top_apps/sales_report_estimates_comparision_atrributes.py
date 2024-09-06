from typing import List, Dict, Sequence, Union, Optional, Literal
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from dataclasses import dataclass, field
from airflow.utils.context import Context
from enum import Enum
from sensor_tower.scripts.utils import (
    CATEGORY_IDS,
    MobileOperatingSystem
)
from utils.constants import COUNTRY_CODE

class ComparisionAttributeData(Enum):
    ABSOLUTE = 'absolute'
    DELTA = 'delta'
    TRANSFORMED_DELTA = 'transformed_delta'


class TimePeriod(Enum):
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    QUARTER = 'quarter'


class Measures(Enum):
    UNITS = 'units'
    REVENUE = 'revenue'


class IosDeviceTypes(Enum):
    IPHONE = 'iphone'
    IPAD = 'ipad'
    TOTAL = 'total'


class CustomTagsMode(Enum):
    INCLUDE_UNIFIED_APPS = 'included_unified_apss'
    EXCLUDE_UNIFIED_APPS = 'excluded_unified_apss'


@dataclass
class SalesReportEstimatesCompareAttrParams:
    os: str
    comparison_attribute: ComparisionAttributeData
    time_range: TimePeriod
    measure: Measures
    device_type: str
    category: str
    date: str
    end_date: str
    country: str
    limit: Optional[int]
    offset: Optional[int]
    custom_fields_filter_id: Optional[str]
    custom_tags_mode: Optional[str]

    def __post_init__(self):
    
        if self.os not in [data.value for data in MobileOperatingSystem]:
            raise ValueError(
                'Invalid Operating System. Expected:', 
                [data.value for data in MobileOperatingSystem]
            )

        if self.comparison_attribute not in [data.value for data in ComparisionAttributeData]:
            raise ValueError(
                'Invalid Comparision Attribute Params, Please Specify on this List:',
                [data.value for data in ComparisionAttributeData]
            )

        if self.time_range not in [data.value for data in TimePeriod]:
            raise ValueError(
                'Invalid Time Period Params, Please Specify on this List:', 
                [data.value for data in TimePeriod]
            )
        
        if self.measure not in [data.value for data in Measures]:
            raise ValueError(
                'Invalid Measure Params, Please Specify on this List:', 
                [data.value for data in Measures]
            )
        
        if self.os == 'ios' and self.device_type not in [data.value for data in IosDeviceTypes]:
            raise ValueError(
                'Invalid IOS Device, Please Specify On This List:', 
                [data.value for data in IosDeviceTypes]
            )
        
        if self.os == 'android' and self.device_type:
            raise ValueError(
                "Please Leave Blank for Android Device"
            )
        
        if self.os == 'unified' and self.device_type != 'total':
            raise ValueError(
                "Unified Operating System must go for all devices. Expetec: total"
            )
        
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
              
        if self.country not in list(COUNTRY_CODE.keys()):
            raise ValueError('Invalid Country. Please specify a country from the list of', list(COUNTRY_CODE.keys()))
        
        if self.limit and (self.limit < 0 or self.limit > 2000):
            raise ValueError('Invalid Limit number specified. Limit must be a positive number or not be exceed 2000')

        if self.offset and (self.offset < 0):
            raise ValueError('Invalid Offset number specified. Offset must be a positive number')

        if self.custom_fields_filter_id and (
            self.custom_tags_mode and 
                self.custom_tags_mode not in [data.value for data in CustomTagsMode]
        ):
            raise ValueError(
                'Please Specify Custom Tag Mode On This List', CustomTagsMode
        )


@dataclass
class SalesReportEstimatesCompareAttrData:
    app_id: str
    current_units_value: int
    comparison_units_value: int
    units_absolute: int
    units_delta: int
    units_transformed_delta: float
    current_revenue_value: int
    comparison_revenue_value: int
    revenue_absolute: int
    revenue_delta: int
    revenue_transformed_delta: float
    absolute: int
    delta: int
    transformed_delta: float
    custom_tags: Dict[str, str]
    date: str
    country: str
    publisher_country: str
    primary_category: str
    game_class: str
    game_genre: str
    game_sub_genre: str
    game_art_style: str
    game_theme: str
    game_setting: str
    release_date_ww: str
    current_US_rating: str
    most_popular_country_by_revenue: str
    most_popular_country_by_downloads: str
    most_popular_region_by_revenue: str
    most_popular_region_by_downloads: str
    all_time_publisher_downloads_ww: int
    all_time_publisher_revenue_ww: int
    RPD_all_time_ww: str
    all_time_downloads_ww: int
    all_time_revenue_ww: int
    downloads_first_30_days_ww: int
    revenue_first_30_days_ww: int
    last_30_days_downloads_ww: int
    last_30_days_revenue_ww: int
    last_180_days_downloads_ww: int
    last_180_days_revenue_ww: int
    ARPDAU_last_month_ww: str
    ARPDAU_last_month_US: str


class SalesReportEstimatesCompareAttrEndpointOperator(
    SensorTowerBaseOperator, 
    SensorTowerAbstractEndpoint
):

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
        comparison_attribute:str,
        time_range: str,
        measure: str,
        device_type: Dict[Literal['ios', 'android'], str ],
        categories: List[Union[str, int]],
        country: str,
        limit: int,
        offset: int,
        custom_fields_filter_id: str,
        custom_tags_mode: str,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/store_intelligence/top_apps/sales_report_estimates_comparision_attributes",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds

        self.os = os
        self.comparison_attribute = comparison_attribute
        self.time_range = time_range
        self.measure = measure
        self.device_type = device_type
        self.categories = categories
        self.country = country
        self.limit = limit
        self.offset = offset
        self.custom_fields_filter_id = custom_fields_filter_id
        self.custom_tags_mode = custom_tags_mode

    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)

    def fetch_data(self):
        results = []
        for os in self.os:
            categories = self._format_categories(os=os, categories=self.categories)
            for category in categories:
                params = self._format_params(os=os, category = category)
            items = self.api.get_sales_report_estimates_comparison_attributes(
                base_params=params)
            results.extend(items)
        return self._format_response_result(results=results)
    
    def _format_categories(
        self,
        os: str,
        categories: List[Union[str, int]]
    ) -> List[str]:
        return [
            str(category) for category in categories
            if (
                    (os == 'ios' or os == 'unified') 
                    and isinstance(category, int)
                ) 
                or
                (
                    os == 'android' and isinstance(category, str)
                )
        ]
    
    def _format_params(
        self,
        os: str,
        category: str
    ) -> SalesReportEstimatesCompareAttrParams:

        params = {
            'os': os,
            'comparison_attribute': self.comparison_attribute,
            'time_range': self.time_range,
            'measure': self.measure,
            'device_type': self.device_type.get(os) if os != 'unified' else 'total',
            'category': category,
            'date': self.ds,
            'end_date': self.ds,
            'country': self.country,
            'limit': self.limit,
            'offset': self.offset,
            'custom_fields_filter_id': self.custom_fields_filter_id,
            'custom_tags_mode': self.custom_fields_filter_id
        }

        return SalesReportEstimatesCompareAttrParams(**params).__dict__

    def _format_response_result(
        self, 
        results: List
    ) -> List[SalesReportEstimatesCompareAttrData]:
        
        if not results: return results

        formatted_results = []
        for result in results:

            custom_tags = result.get("custom_tags", {})
            custom_tags["ARPDAU_last_month_ww"] = result.get(
                "custom_tags", {}).get("ARPDAU (Last Month, WW)", "")
            custom_tags["ARPDAU_last_month_US"] = result.get(
                "custom_tags", {}).get("ARPDAU (Last Month, US)", "")

            data = {
                "app_id"                           : str(result.get("app_id", "")),
                "current_units_value"              : result.get("current_units_value", 0),
                "comparison_units_value"           : result.get("comparison_units_value", 0),
                "units_absolute"                   : result.get("units_absolute", 0),
                "units_delta"                      : result.get("units_delta", 0),
                "units_transformed_delta"          : result.get("units_transformed_delta", 0.0),
                "current_revenue_value"            : result.get("current_revenue_value", 0),
                "comparison_revenue_value"         : result.get("comparison_revenue_value", 0),
                "revenue_absolute"                 : result.get("revenue_absolute", 0),
                "revenue_delta"                    : result.get("revenue_delta", 0),
                "revenue_transformed_delta"        : result.get("revenue_transformed_delta", 0.0),
                "absolute"                         : result.get("absolute", 0),
                "delta"                            : result.get("delta", 0),
                "transformed_delta"                : result.get("transformed_delta", 0.0),
                "custom_tags"                      : custom_tags,
                "date"                             : result.get("date", ""),
                "country"                          : result.get("country", ""),
                "publisher_country"                : custom_tags.get("Publisher Country", ""),
                "primary_category"                 : custom_tags.get("Primary Category", ""),
                "game_class"                       : custom_tags.get("Is a Game", ""),
                "game_genre"                       : custom_tags.get("Primary Category", ""),
                "game_sub_genre"                   : custom_tags.get("Primary Category", ""),
                "game_art_style"                   : custom_tags.get("Primary Category", ""),
                "game_theme"                       : custom_tags.get("Primary Category", ""),
                "game_setting"                     : custom_tags.get("Primary Category", ""),
                "release_date_ww"                  : custom_tags.get("Release Date (WW)", ""),
                "current_US_rating"                : str(custom_tags.get("Current US Rating", '')),
                "most_popular_country_by_revenue"  : custom_tags.get("Most Popular Country by Revenue", ""),
                "most_popular_country_by_downloads": custom_tags.get("Most Popular Country by Downloads", ""),
                "most_popular_region_by_revenue"   : custom_tags.get("Most Popular Region by Revenue", ""),
                "most_popular_region_by_downloads" : custom_tags.get("Most Popular Region by Downloads", ""),
                "all_time_publisher_downloads_ww"  : str(custom_tags.get("All Time Publisher Downloads (WW)", '')),
                "all_time_publisher_revenue_ww"    : str(custom_tags.get("All Time Publisher Revenue (WW)", '')),
                "RPD_all_time_ww"                  : str(custom_tags.get("RPD (All Time, WW)", '')),
                "all_time_downloads_ww"            : str(custom_tags.get("All Time Downloads (WW)", '')),
                "all_time_revenue_ww"              : str(custom_tags.get("All Time Revenue (WW)", '')),
                "downloads_first_30_days_ww"       : str(custom_tags.get("Downloads First 30 Days (WW)", '')),
                "revenue_first_30_days_ww"         : str(custom_tags.get("Revenue First 30 Days (WW)", '')),
                "last_30_days_downloads_ww"        : str(custom_tags.get("Last 30 Days Downloads (WW)", '')),
                "last_30_days_revenue_ww"          : str(custom_tags.get("Last 30 Days Revenue (WW)", '')),
                "last_180_days_downloads_ww"       : str(custom_tags.get("Last 180 Days Downloads (WW)", '')),
                "last_180_days_revenue_ww"         : str(custom_tags.get("Last 180 Days Revenue (WW)", '')),
                "ARPDAU_last_month_ww"             : str(custom_tags.get("ARPDAU (Last Month, WW)", '')),
                "ARPDAU_last_month_US"             : str(custom_tags.get("ARPDAU (Last Month, US)", ''))
            }

            formatted_results.append(data)

        return formatted_results

    
