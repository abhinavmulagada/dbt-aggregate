{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with addresses_by_month as (
    SELECT sum(total_number_of_addresses) 
           over (ORDER BY date ROWS BETWEEN 31 PRECEDING AND 1 PRECEDING) as sum, 
    DATE(`date`) as `date`,
    total_number_of_addresses
    FROM `celo-testnet-production.blockscout_data.addresses_by_day`
)
select date,
       sum,
       total_number_of_addresses,
       (total_number_of_addresses / sum) * 100 as percent_change_over_past_30_days
from addresses_by_month 

{% if is_incremental() %}
  where `date` >= (select max(`date`) from {{ this }})
{% endif %}