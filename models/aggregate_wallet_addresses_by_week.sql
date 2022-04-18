{{
    config(
        materialized='incremental',
        unique_key='week'
    )
}}

with address_data as (
  select sum(new_wallet_addresses) as new_wallet_addresses_by_month,
       sum(returning_wallet_addresses) as returning_wallet_addresses_by_month,
       DATE_TRUNC(date, WEEK) as week,
       MAX(date) as most_recent_date
  from {{ref('aggregate_wallet_addresses_by_day')}}
  where `date` is not null
  group by week
)
select * from address_data


{% if is_incremental() %}
  WHERE `most_recent_date` > (SELECT MAX(`most_recent_date`) as max_date FROM {{ this }})
{% endif %}
