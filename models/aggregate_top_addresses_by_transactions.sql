-- currently not in use

{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

select distinct 
       count(*) over (partition by from_address_hash) as number_of_transactions, 
       from_address_hash,
       DATE(inserted_at) as `date` 
from {{ref('aggregate_addresses_transactions_by_day')}}

{% if is_incremental() %}
  WHERE (`date`) > (SELECT MAX(inserted_at) as max_date 
                            FROM {{ref('aggregate_addresses_transactions_by_day')}})
{% endif %}

limit 150