from enum import Enum


class Scale(Enum):
    ALL = 'all'
    HOUR = 'hour'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    QUARTER = 'quarter'
    YEAR = 'year'

class SplitBy(Enum):
    CAMPAIGN_SET = 'campaignSet'
    CREATIVE_PACK = 'creativePack'
    AD_TYPE = 'adType'
    CAMPAIGN = 'campaign'
    TARGET = 'target'
    SOURCE_APP_ID = 'sourceAppId'
    STORE = 'store'
    COUNTRY = 'country'
    PLATFORM = 'platform'
    OS_VERSION = 'osVersion'
    SKAD_CONVERSION_VALUE = 'skadConversionValue'

class Fields(Enum):
    ALL = 'all'
    TIMESTAMP = 'timestamp'
    CAMPAIGN_SET = 'campaignSet'
    CREATIVE_PACK = 'creativePack'
    AD_TYPE = 'adType'
    CAMPAIGN = 'campaign'
    TARGET = 'target'
    SOURCE_APP_ID = 'sourceAppId'
    STORE = 'store'
    COUNTRY = 'country'
    PLATFORM = 'platform'
    OS_VERSION = 'osVersion'
    STARTS = 'starts'
    VIEWS = 'views'
    CLICKS = 'clicks'
    INSTALLS = 'installs'
    SPEND = 'spend'
    SKAD_INSTALLS = 'skadInstalls'
    SKAD_CPI = 'skadCpi'
    SKAD_CONVERSION = 'skadConversion'

class AdTypes(Enum):
    VIDEO = 'video'
    PLAYABLE = 'playable'
    VIDEO_PLAYABLE = 'video+playable'

class AppStores(Enum):
    APPLE = 'apple'
    GOOGLE = 'google'

class PlatForm(Enum):
    IOS = 'ios'
    ANDROID = 'android'

