

SELECT t.id as id_webinarfuel_registrations
	, analytics.fnEmail(t.email) AS email
		, NULLIF(TRIM(t.email), '') as orig_email
		, NULLIF(trim(t.first_name),'') as first_name
		, NULLIF(trim(t.last_name),'') as last_name
		, analytics.fnPhone(t.phone) AS phone
		, NULLIF(TRIM(t.phone), '') as orig_phone
		, NULLIF(trim(t.city),'') as city
		, NULLIF(trim(t.state),'') as state
		, NULLIF(trim(t.country),'') as country
		, t.first_registration_date as dt
		, t.video_id
		, NULLIF(trim(t.video_name), '') as video_name
		, NULLIF(trim(t.ip), '') as ip
		, NULLIF(trim(t.utm_source), '') AS utm_source
		, NULLIF(trim(t.utm_medium), '') AS utm_medium
		, NULLIF(trim(t.utm_content), '') AS utm_content
		, NULLIF(trim(t.utm_campaign), '') AS utm_campaign
		, NULLIF(trim(t.utm_term), '') AS utm_term
		, concat('WF - ', t.webinar_name) as webinar_name
		, datetime(t.inserted_at, 'America/Phoenix') as inserted_at
FROM {{ source('kbb_evergreen', 'webinarfuel_registrations') }} t