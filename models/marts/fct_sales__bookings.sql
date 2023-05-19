
with bookings as (
    select b.id
        , b.starting_time
        , analytics.fnEmail(b.customer_email) as customer_email
        , b.rep_email
        , b.extractiontime
        , b.last_updated_time
    from {{ source('oncehub', 'bookings') }} b
    qualify row_number() over (partition by b.id, b.starting_time order by extractiontime) = 1
)

, recency as (
    select b.id
        , b.starting_time
        , b.customer_email
        , b.rep_email
        , b.last_updated_time
        , row_number() over (partition by b.id order by b.extractiontime desc) as recency
    from bookings b
)

, hubspot_deal as (
    select d.deal_id
        , d.property_oncehub_booking_id
        , case when d.deal_pipeline_id = 11280578 and d.deal_pipeline_stage_id = 63590844 then true else false end as is_no_show
    from {{ source('hubspot', 'deal') }} d
    qualify row_number() over (partition by d.property_oncehub_booking_id order by _fivetran_synced desc) = 1
)

    select {{ dbt_utils.generate_surrogate_key(['b.id', 'b.starting_time']) }} as pk
        , b.id as booking_id
        , d.deal_id
        , b.starting_time
        , b.customer_email
        , b.rep_email
        , case when b.recency > 1 then 'rescheduled'
            when d.is_no_show = true then 'no_show'
            when b.starting_time >= current_timestamp then 'pending'
            else 'complete' end as booking_status
        , b.last_updated_time
        , b.recency
from recency b
    join hubspot_deal d
        on b.id = d.property_oncehub_booking_id