{% snapshot snap_dim_contacts %}

{{
   config(
       target_database=env_var('project'),
       target_schema=env_var('dataset') + '_snapshots',
       unique_key='email',

       strategy='check',
       check_cols=['dt_captured', 'funnel_id_captured', 'source_captured', 'source_id_captured'],
   )
}}

select email
    , dt_captured
    , funnel_id_captured
    , source_captured
    , source_id_captured
from {{ ref('dim_contacts') }}
where email is not null

{% endsnapshot %}