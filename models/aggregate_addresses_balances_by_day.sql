{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with token_balances as (
    select * from 
    EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''SELECT CONCAT('0x', ENCODE(address_hash, 'hex')) as address_hash, 
              CONCAT('0x', ENCODE(token_contract_address_hash, 'hex')) AS token_contract_address_hash,
              value,
              DATE(inserted_at) 
              FROM address_current_token_balances''',
              '{"default_type_for_decimal_columns":"float64"}')
)
select distinct `date`, 
       SUM(value) over (partition by address_hash order by `date` rows 1 preceding) wallet_value, 
       address_hash 
from token_balances
where CONTAINS_SUBSTR(token_contract_address_hash, '0x471EcE3750Da237f93B8E339c536989b8978a438') -- CELO
    OR CONTAINS_SUBSTR(token_contract_address_hash, '0x765DE816845861e75A25fCA122bb6898B8B1282a') -- cUSD
    OR CONTAINS_SUBSTR(token_contract_address_hash, '0xD8763CBa276a3738E6DE85b4b3bF5FDed6D6cA73') -- cEUR
    OR CONTAINS_SUBSTR(token_contract_address_hash, '0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787') -- cREAL

{% if is_incremental() %}
  WHERE (`date`) > (SELECT MAX(inserted_at) as max_date 
                            FROM token_balances)
{% endif %}