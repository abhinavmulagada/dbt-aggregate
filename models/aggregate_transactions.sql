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
DATE(b.timestamp) as date
FROM `celo-testnet-production.blockscout_data.rpl_transactions` t 
LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
ON t.block_hash = b.hash

{% if is_incremental() %}
  where t.updated_at >= (select max(t.updated_at) from {{ this }})
{% endif %}

GROUP BY `date`