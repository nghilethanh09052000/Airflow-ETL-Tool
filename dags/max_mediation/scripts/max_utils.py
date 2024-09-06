class Params:

    AD_REVENUE_COLUMNS = [
        "day,country,application,package_name,platform,network,network_placement,max_ad_unit,max_ad_unit_id,ad_unit_waterfall_name,device_type,ad_format,attempts,responses,impressions,ecpm,estimated_revenue,store_id"
    ]

    USER_AD_REVENUE_COLUMNS = [
        'Date','Ad Unit ID','Ad Unit Name','Waterfall','Ad Format','Placement','Country','Device Type','IDFA','IDFV','User ID','Network','Revenue'
    ]

    COHORT_REQUIRED_X_NUMBER = ['0', '1', '2', '3', '7', '14', '30']

    COHORT_AD_REVENUE_PERPORMANCE_COLUMNS = [
        "day","application","package_name","platform","country","installs",
        "pub_revenue_X",
        "iap_pub_revenue_X",
        "ads_pub_revenue_X",
        "reward_pub_revenue_X",
        "inter_pub_revenue_X",
        "banner_pub_revenue_X",
        "mrec_pub_revenue_X",
        "rpi_X",
        "iap_rpi_X",
        "ads_rpi_X",
        "reward_rpi_X",
        "inter_rpi_X",
        "banner_rpi_X",
        "mrec_rpi_X"
    ]
     
    COHORT_AD_IMPRESSIONS_INFO_COLUMNS = [
        "day","application","package_name","platform","country","installs",
        "user_count_X",
        "imp_X",
        "imp_per_user_X",
        "inter_imp_X",
        "inter_imp_per_user_X",
        "reward_imp_X",
        "reward_imp_per_user_X",
        "banner_imp_X",
        "banner_imp_per_user_X",
        "mrec_imp_X",
        "mrec_imp_per_user_X"
    ]

    COHORT_SESSION_INFO_COLUMNS = [
        "day","application","package_name","platform","country","installs",
        "daily_usage_X",
        "session_count_X",
        "user_count_X",
        "session_length_X",
        "retention_X"
    ]

    APP_META_QUERY = "SELECT DISTINCT package_name, platform FROM `{{bq_project}}.raw_max_mediation.raw_ad_revenue` WHERE snapshot_date = '{{ ds }}'"

    QUERY_JOB_CONFIG = '''{"jobType": "QUERY","query": {"query": "{{ query }}"}}'''