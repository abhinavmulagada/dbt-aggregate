{{
    config(
        materialized='incremental',
        unique_key='week'
    )
}}

with stable_coin_weekly_data as (
    select name,
       sum(new_wallet_addresses) as new_wallet_addresses,
       sum(returning_wallet_addresses) as returning_wallet_addresses,
       DATE_TRUNC(date, WEEK) as week,
       MAX(date) as most_recent_date
    from {{ref('stable_coin_active_addresses_by_day')}}
    where date is not null
    group by week, name
)
select *
from stable_coin_weekly_data


{% if is_incremental() %}
  WHERE `most_recent_date` > (SELECT MAX(`most_recent_date`) as max_date 
                             FROM {{ this }})
{% endif %}
