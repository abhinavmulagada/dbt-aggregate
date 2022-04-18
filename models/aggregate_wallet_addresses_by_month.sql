{{
    config(
        materialized='incremental',
        unique_key='month'
    )
}}

with address_data as (
  select sum(new_wallet_addresses) as new_wallet_addresses_by_month,
       sum(returning_wallet_addresses) as returning_wallet_addresses_by_month,
       DATE_TRUNC(date, MONTH) as month,
       MAX(date) as most_recent_date
  from {{ref('aggregate_wallet_addresses_by_day')}}
  where `date` is not null
  group by month
)
select * from address_data


{% if is_incremental() %}
  WHERE `most_recent_date` > (SELECT MAX(`most_recent_date`) as max_date FROM {{ this }})
{% endif %}