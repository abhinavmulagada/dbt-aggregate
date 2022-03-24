/*
Need to figure out way to also get contract to contract transactions
*/

{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with transaction_data as(
      select DATE (t.inserted_at) as `date`,
            a.is_contract,
            CASE WHEN DATE(t.inserted_at) < DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) 
                 then 'new' else 'returning' end as address_status
      
      from `celo-testnet-production.blockscout_data.rpl_transactions` t
      inner join `celo-testnet-production.blockscout_data.rpl_addresses` a on a.`hash` = t.from_address_hash
),
transaction_data_2 as (
      select `date`,
            CASE WHEN is_contract and address_status = 'new' then 'new_contract_address' 
            WHEN is_contract and address_status = 'returning' then 'returning_contract_address'
            WHEN NOT is_contract and address_status = 'new' then 'new_wallet_address'
            WHEN NOT is_contract and address_status = 'returning' then 'returning_wallet_address' end as type,
      from transaction_data
)
select count(*) as count, type, date
from transaction_data_2 


{% if is_incremental() %}
  WHERE (date) > (SELECT MAX(DATE(inserted_at)) as max_date 
                            FROM `celo-testnet-production.blockscout_data.rpl_transactions`)
{% endif %}

group by type, date