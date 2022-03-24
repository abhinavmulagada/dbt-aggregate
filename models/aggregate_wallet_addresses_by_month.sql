{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

select sum(new_wallet_addresses) as new_wallet_addresses_by_month,
       sum(returning_wallet_addresses) as returning_wallet_addresses_by_month,
       DATE_TRUNC(date, MONTH) as month,
       MAX(date) as most_recent_date
from {{ref('aggregate_wallet_addresses_by_day')}}
{% if is_incremental() %}
  WHERE `most_recent_date` < (SELECT MAX(date) as max_date 
                            FROM {{ref('aggregate_wallet_addresses_by_day')}})
{% endif %}
group by month