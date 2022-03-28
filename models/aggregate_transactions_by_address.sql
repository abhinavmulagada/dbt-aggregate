{{
    config(
        materialized='incremental',
        unique_key='max_date'
    )
}}

SELECT CASE
        WHEN token_contract_address_hash = '0x471ece3750da237f93b8e339c536989b8978a438' THEN 'CELO'
        
        WHEN token_contract_address_hash = '0x765de816845861e75a25fca122bb6898b8b1282a' THEN 'cUSD'
        
        WHEN token_contract_address_hash = '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' THEN 'cEUR'
        
        WHEN token_contract_address_hash = '0xe8537a3d056da446677b9e9d6c5db704eaab4787' THEN 'cREAL'
        
        WHEN token_contract_address_hash = '0x73a210637f6f6b7005512677ba6b3c96bb4aa44b' THEN 'Mobius'
        
        WHEN token_contract_address_hash = '0x00be915b9dcf56a3cbe739d9b9c202ca692409ec' or 
             token_contract_address_hash = '0x1e593f1fe7b61c53874b54ec0c59fd0d5eb8621e' or 
             token_contract_address_hash = '0xe7b5ad135fa22678f426a381c7748f6a5f2c9e6c' THEN 'UBE'
        
        WHEN token_contract_address_hash = '0x17700282592d6917f6a73d0bf8accf4d578c131e' or 
             token_contract_address_hash = '0x7d00cd74ff385c955ea3d79e47bf06bd7386387d' or 
             token_contract_address_hash = '0x918146359264c492bd6934071c6bd31c854edbc3' or 
             token_contract_address_hash = '0x64defa3544c695db8c535d289d843a189aa26b98' or 
             token_contract_address_hash = '0xe273ad7ee11dcfaa87383ad5977ee1504ac07568' or 
             token_contract_address_hash = '0xa8d0e6799ff3fd19c6459bf02689ae09c4d78ba7' or 
             token_contract_address_hash = '0x7037f7296b2fc7908de7b57a89efaa8319f0c500' THEN 'Moola'
        
        WHEN token_contract_address_hash = '0x74c0c58b99b68cf16a717279ac2d056a34ba2bfe' THEN 'SOURCE'
        
        WHEN token_contract_address_hash = '0xbaab46e28388d2779e6e31fd00cf0e5ad95e327b' or 
             token_contract_address_hash = '0xd629eb00deced2a080b7ec630ef6ac117e614f1b' THEN 'wBTC'
       
        WHEN token_contract_address_hash = '0x122013fd7df1c6f636a5bb8f03108e876548b455' or 
             token_contract_address_hash = '0x2def4285787d58a2f811af24755a8150622f4361' or 
             token_contract_address_hash = '0xe919f65739c26a42616b7b8eedc6b5524d1e3ac4' THEN 'wETH'
        
        WHEN token_contract_address_hash = '0xa649325aa7c5093d12d6f98eb4378deae68ce23f' THEN 'Binance'
        
        ELSE 'others' END as name,
        count(*) as count,
        MAX(DATE(b.timestamp)) as max_date

FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` t
LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
ON t.block_hash = b.hash

{% if is_incremental() %}
  where max_date >= (select max(max_date) from {{ this }})
{% endif %}

group by name