{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with addresses_sum as (
    SELECT 
    SUM(contract_addresses) over (ORDER BY date ROWS BETWEEN 31 PRECEDING AND 1 PRECEDING) as contract_sum,
    contract_addresses,
    SUM(wallet_addresses) over (ORDER BY date ROWS BETWEEN 31 PRECEDING AND 1 PRECEDING) as wallet_sum,
    wallet_addresses,
    date
    FROM {{ref('aggregate_addresses_by_day')}}
    group by date, contract_addresses, wallet_addresses
)
select date,
       contract_sum,
       contract_addresses,
       (contract_addresses / contract_sum) * 100 as contract_percent_change,
       wallet_sum,
       wallet_addresses,
       (wallet_addresses / wallet_sum) * 100 as wallet_percent_change,
from addresses_sum

{% if is_incremental() %}
  where `date` >= (select max(`date`) from {{ ref('aggregate_addresses_by_day') }} )
{% endif %}