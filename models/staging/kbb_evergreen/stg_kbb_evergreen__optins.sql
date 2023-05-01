

SELECT t.id as id_tracking_optins
	, analytics.fnEmail(t.email) AS email
		, NULLIF(TRIM(t.email), '') as orig_email
		, NULLIF(trim(t.first),'') as first_name
		, NULLIF(trim(t.last),'') as last_name
		, analytics.fnPhone(t.phone) AS phone
		, NULLIF(TRIM(t.phone), '') as orig_phone
		, NULLIF(trim(t.street),'') as address_1
		, NULLIF(trim(t.city),'') as city
		, NULLIF(trim(t.state),'') as state
		, NULLIF(trim(t.zip),'') as zip
		, NULLIF(trim(t.country),'') as country
		, t.dt
		, NULLIF(trim(t.funnel_id), '') as funnel_id
		, t.funnel_step_id
		, t.ip
		, t.optin_ip
		, NULLIF(analytics.fnJSON_tracking_optins_cart_affiliate_id(NULLIF(json, ''), 'domain_userid'), '""') AS domain_userid
		, NULLIF(REPLACE(JSON_EXTRACT(json, '$.contact.additional_info.utm_source'), '"', ''), '') AS utm_source
		, NULLIF(REPLACE(JSON_EXTRACT(json, '$.contact.additional_info.utm_medium'), '"', ''), '') AS utm_medium
		, NULLIF(REPLACE(JSON_EXTRACT(json, '$.contact.additional_info.utm_content'), '"', ''), '') AS utm_content
		, NULLIF(REPLACE(JSON_EXTRACT(json, '$.contact.additional_info.utm_campaign'), '"', ''), '') AS utm_campaign
		, NULLIF(REPLACE(JSON_EXTRACT(json, '$.contact.additional_info.utm_term'), '"', ''), '') AS utm_term
FROM {{ source('kbb_evergreen', 'tracking_optins') }} t