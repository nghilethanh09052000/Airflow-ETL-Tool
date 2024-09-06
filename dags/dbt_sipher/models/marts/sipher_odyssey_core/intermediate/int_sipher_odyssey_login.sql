{{- config(
  	materialized='incremental',
  	unique_key=[
			'event_date',
			'user_id',
			'ather_id',
			'user_pseudo_id',
			'ga_session_id'
			],
  	merge_update_columns = [
						'event_timestamp',
						'session_duration',
						'country',
						'version',
						'build_number',
						'operating_system',
						'operating_system_version',
						'mobile_model_name',
						'mobile_brand_name',
						'mobile_marketing_name',
						'mobile_os_hardware_model',
						'gl_version',
						'gpu_family',
						'profile',
						'gpu_brand',
						'chipset',
						'device_model',
						'vulkan_version'
						],
	partition_by={
		'field': 'event_date',
		'data_type': 'DATE',
	},
	cluster_by=['country']
) -}}

SELECT  
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
	user_id,
	COALESCE({{ get_string_value_from_user_properties(key="ather_id")}}, 'unknown') AS ather_id,
	user_pseudo_id,
	COALESCE({{ get_int_value_from_event_params(key="ga_session_id")}},0) AS ga_session_id,
    COALESCE(SUM({{ get_int_value_from_event_params(key="engagement_time_msec")}})/1000 , 0) AS session_duration,
	--lay thoi gian dau tien user login cua 1 session
	MIN(event_timestamp) AS event_timestamp,

    COALESCE(MAX(geo.country), 'unknown') AS country,
    COALESCE(MAX(app_info.version), 'unknown') AS version,

	-- khong dung unknown doi voi buildnumber o day
    MAX(COALESCE({{ get_string_value_from_user_properties(key="build_number")}},
	{{ get_string_value_from_event_params(key="build_number")}})) AS build_number,

    COALESCE(MAX(device.operating_system), 'unknown') AS operating_system,
	COALESCE(MAX(device.operating_system_version), 'unknown') AS operating_system_version,
    COALESCE(MAX(device.mobile_model_name), 'unknown') AS mobile_model_name,
    COALESCE(MAX(device.mobile_brand_name), 'unknown') AS mobile_brand_name,
    COALESCE(MAX(device.mobile_marketing_name), 'unknown') AS mobile_marketing_name,
    COALESCE(MAX(device.mobile_os_hardware_model), 'unknown') AS mobile_os_hardware_model,

	COALESCE(MAX({{ get_string_value_from_event_params(key="gl_version")}}), 'unknown') AS gl_version,
	COALESCE(MAX({{ get_string_value_from_event_params(key="gpu_family")}}), 'unknown') AS gpu_family,
	COALESCE(MAX({{ get_string_value_from_event_params(key="profile")}}), 'unknown') AS profile,
	COALESCE(MAX({{ get_string_value_from_event_params(key="gpu_brand")}}), 'unknown') AS gpu_brand, 
	COALESCE(MAX({{ get_string_value_from_event_params(key="chipset")}}), 'unknown') AS chipset,
	COALESCE(MAX({{ get_string_value_from_event_params(key="device_model")}}), 'unknown') AS device_model, 
	COALESCE(MAX({{ get_string_value_from_event_params(key="vulkan_version")}}), 'unknown') AS vulkan_version, 
	
	FROM {{ ref('stg_firebase__sipher_odyssey_events_3d') }}
	
	WHERE TRUE
		AND event_name IN ('login_start', 'devicespec', 'user_engagement')  
		AND user_id IS NOT NULL -- chi lay cac step sau login
	
	GROUP BY ALL
