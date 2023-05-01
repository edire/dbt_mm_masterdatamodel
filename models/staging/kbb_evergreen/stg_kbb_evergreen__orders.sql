

SELECT t.id as id_tracking_orders
        , left(t.id, 9) as id_tracking_orders_base
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
        , NULLIF(t.funnel_id, '') as funnel_id
        , t.funnel_step_id
        , t.ip
        , t.order_ip
        , NULLIF(analytics.fnJSON_tracking_orders_cart_affiliate_id(NULLIF(json, ''), 'domain_userid'), '""') AS domain_userid
        , NULLIF(REPLACE(JSON_EXTRACT(json, '$.purchase.contact.additional_info.utm_source'), '"', ''), '') AS utm_source
        , NULLIF(REPLACE(JSON_EXTRACT(json, '$.purchase.contact.additional_info.utm_medium'), '"', ''), '') AS utm_medium
        , NULLIF(REPLACE(JSON_EXTRACT(json, '$.purchase.contact.additional_info.utm_content'), '"', ''), '') AS utm_content
        , NULLIF(REPLACE(JSON_EXTRACT(json, '$.purchase.contact.additional_info.utm_campaign'), '"', ''), '') AS utm_campaign
        , NULLIF(REPLACE(JSON_EXTRACT(json, '$.purchase.contact.additional_info.utm_term'), '"', ''), '') AS utm_term
        , t.dt
        , t.amount
        , t.transaction_id
        , t.product
        , t.productid
        , case when t.id like '%-%' then 1 else 0 end as is_addon
FROM {{ source('kbb_evergreen', 'tracking_orders') }} t