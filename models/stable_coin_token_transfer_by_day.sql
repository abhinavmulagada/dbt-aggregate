{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

SELECT CASE
       WHEN token_contract_address_hash = '0x471ece3750da237f93b8e339c536989b8978a438' THEN 'CELO'        
       WHEN token_contract_address_hash = '0x765de816845861e75a25fca122bb6898b8b1282a' THEN 'cUSD'        
       WHEN token_contract_address_hash = '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' THEN 'cEUR'        
       WHEN token_contract_address_hash = '0xe8537a3d056da446677b9e9d6c5db704eaab4787' THEN 'cREAL'
       ELSE 'others' END as name,
       count(*) as count,
       DATE(b.timestamp) as date
FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` t
LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
ON t.block_hash = b.hash

{% if is_incremental() %}
  WHERE DATE(b.timestamp) > (SELECT DATE(MAX(date)) as max_date FROM {{ this }})
{% endif %}

group by name, date