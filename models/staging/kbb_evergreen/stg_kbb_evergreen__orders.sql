

SELECT analytics.fnEmail(t.email) AS email
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
        , NULLIF(t.funnel_id, '') as funnel_id
        , t.dt
        , t.id as source_id
        , 'google_cloud_mysql_kbb_evergreen.tracking_orders' as source_desc
FROM {{ source('kbb_evergreen', 'tracking_orders') }} t