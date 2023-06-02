{% snapshot snap_fct_mastermind__subscriptions %}

{{
   config(
       target_database=env_var('project'),
       target_schema=env_var('dataset') + '_snapshots',
       unique_key='pk',

       strategy='check',
       check_cols='all',
   )
}}

select *
from {{ ref('fct_mastermind__subscriptions') }}

{% endsnapshot %}