{{
    config(
        materialized='incremental',
        unique_key='week'
    )
}}

select name,
       sum(count) as count,
       DATE_TRUNC(date, WEEK) as week,
       MAX(date) as most_recent_date
from {{ref('stable_coin_token_transfer_by_day')}}
where `date` is not null

{% if is_incremental() %}
  WHERE `most_recent_date` > (SELECT MAX(`most_recent_date`) as max_date FROM {{ this }})
{% endif %}

group by week, name
