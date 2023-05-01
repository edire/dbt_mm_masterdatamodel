

SELECT c.id as id_contact
  , analytics.fnEmail(c.property_email) as email
  , nullif(trim(c.property_email), '') as orig_email
  , nullif(trim(c.property_firstname), '') as first_name
  , nullif(trim(c.property_lastname), '') as last_name
  , analytics.fnPhone(c.property_phone) as phone
  , nullif(trim(c.property_phone), '') as orig_phone
  , nullif(trim(c.property_address), '') as address_1
  , nullif(trim(c.property_city), '') as city
  , nullif(trim(c.property_state), '') as state
  , nullif(trim(c.property_zip), '') as zip
  , nullif(trim(c.property_country), '') as country
  , property_createdate as dt
FROM {{ source('hubspot', 'contact') }} c