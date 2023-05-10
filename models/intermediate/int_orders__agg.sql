

{{ dbt_utils.union_relations(
    relations=[ref('int_orders__base'), ref('int_orders__missing')]
) }}