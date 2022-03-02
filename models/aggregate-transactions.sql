--DELETE FROM `celo-testnet-production.blockscout_data.transactions_by_day` WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY); 
--INSERT INTO `celo-testnet-production.dbt_aggregate.aggregate-transactions`
{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}


SELECT COUNT(1) as number_of_transactions, 
SUM(CAST(t.gas_used as BIGNUMERIC)) as total_gas_used, 
MIN(CAST(t.gas_used as BIGNUMERIC)) as min_gas_used, 
MAX(CAST(t.gas_used as BIGNUMERIC)) as max_gas_used, 
CAST(AVG(CAST(t.gas_used as BIGNUMERIC)) as BIGNUMERIC) as avg_gas_used, 
MIN(CAST(t.gas_price as BIGNUMERIC)) as min_gas_price, 
MAX(CAST(t.gas_price as BIGNUMERIC)) as max_gas_price, 
CAST(AVG(CAST(t.gas_price as BIGNUMERIC)) as BIGNUMERIC) as avg_gas_price,
SUM(CAST(t.value as BIGNUMERIC)) as total_value, 
MIN(CAST(t.value as BIGNUMERIC)) as min_value, 
MAX(CAST(t.value as BIGNUMERIC)) as max_value, 
CAST(AVG(CAST(t.value as BIGNUMERIC)) as BIGNUMERIC) as avg_value, 
DATE(b.timestamp) as date,
MAX(t.updated_at) as updated_at
FROM `celo-testnet-production.blockscout_data.rpl_transactions` t 
LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
ON t.block_hash = b.hash
--WHERE DATE(b.timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
--AND DATE(b.timestamp) < CURRENT_DATE() 

{% if is_incremental() %}
  where t.updated_at >= (select max(t.updated_at) from {{ this }} 
  WHERE DATE(b.timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AND DATE(b.timestamp) < CURRENT_DATE() )
{% endif %}

GROUP BY `date`