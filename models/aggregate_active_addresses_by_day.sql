{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

select DATE (t.inserted_at) as `date`,
       count(case when CONTAINS_SUBSTR((a.is_contract), 'true') 
             and DATE(t.inserted_at) >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) then 1 end) as returning_contract_address,
       count(case when CONTAINS_SUBSTR((a.is_contract), 'false') 
             and DATE(t.inserted_at) >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) then 1 end) as returning_wallet_address,
       count(case when CONTAINS_SUBSTR((a.is_contract), 'true') 
             and DATE(t.inserted_at) < DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) then 1 end) as new_contract_address,
       count(case when CONTAINS_SUBSTR((a.is_contract), 'false') 
             and DATE(t.inserted_at) < DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY) then 1 end) as new_wallet_address,
from `celo-testnet-production.blockscout_data.rpl_celo_wallets` w
inner join `celo-testnet-production.blockscout_data.rpl_transactions` t on t.from_address_hash = w.wallet_address_hash 
inner join `celo-testnet-production.blockscout_data.rpl_addresses` a on a.`hash` = w.wallet_address_hash


{% if is_incremental() %}
  WHERE (t.inserted_at) > (SELECT MAX(inserted_at) as max_date 
                            FROM `celo-testnet-production.blockscout_data.rpl_transactions`)
{% endif %}

group by `date`