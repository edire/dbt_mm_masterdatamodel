

select analytics.fnEmail(c.email) as email
  , nullif(trim(c.email), '') as orig_email
  , nullif(trim(json_extract(c.metadata, '$.first_name')), '') as first_name
  , nullif(trim(json_extract(c.metadata, '$.last_name')), '') as last_name
  , analytics.fnPhone(json_extract(c.metadata, '$.phone')) as phone
  , nullif(trim(json_extract(c.metadata, '$.phone')), '') as orig_phone
  , nullif(trim(coalesce(c.address_line_1, c.shipping_address_line_1)), '') as address_1
  , nullif(trim(coalesce(c.address_line_2, c.shipping_address_line_2)), '') as address_2
  , nullif(trim(coalesce(c.address_city, c.shipping_address_city)), '') as city
  , nullif(trim(coalesce(c.address_state, c.shipping_address_state)), '') as state
  , nullif(trim(coalesce(c.address_postal_code, c.shipping_address_postal_code)), '') as zip
  , nullif(trim(coalesce(c.address_country, c.shipping_address_country)), '') as country
  , c.created as dt
  , c.id as source_id
  , 'stripe_mastermind.customer' as source_desc
from {{ source('stripe_mastermind', 'customer') }} c