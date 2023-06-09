{% snapshot snap_int_stripe_subscriptions__agg %}

{{
   config(
       target_database=env_var('project'),
       target_schema=env_var('dataset') + '_snapshots',
       unique_key='subscription_id',

       strategy='check',
       check_cols='all',
   )
}}

select *
from {{ ref('int_stripe_subscriptions__agg') }}

{% endsnapshot %}