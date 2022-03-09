{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

SELECT SUM(CASE when CONTAINS_SUBSTR((is_contract), 'true') then 1 else 0 end) as contract_addresses, 
       SUM(CASE when CONTAINS_SUBSTR((is_contract), 'false') then 1 else 0 end) as wallet_addresses, 
       DATE(inserted_at) as `date` 
FROM `celo-testnet-production.blockscout_data.rpl_addresses` 

{% if is_incremental() %}
  WHERE DATE(`inserted_at`) > (SELECT MAX(`date`) as max_date 
                            FROM `celo-testnet-production.blockscout_data.addresses_by_day`)
{% endif %}

GROUP BY `date`