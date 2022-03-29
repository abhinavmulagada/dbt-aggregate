{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with daily_count_data as (
SELECT count(*) as daily_count,
       DATE(b.timestamp) as date
FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` t
LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
ON t.block_hash = b.hash
where b.timestamp is not null
group by date
)
select date,
       daily_count,
       avg(daily_count) over (order by date rows between 29 preceding and current row) as thirty_day_avg
from daily_count_data

{% if is_incremental() %}
  WHERE `date` > (SELECT MAX(`date`) as max_date FROM {{ this }}) 
{% endif %}