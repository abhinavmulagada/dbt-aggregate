{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with token_data as(
    SELECT CASE
        WHEN token_contract_address_hash = '0x471ece3750da237f93b8e339c536989b8978a438' THEN 'CELO'        
        WHEN token_contract_address_hash = '0x765de816845861e75a25fca122bb6898b8b1282a' THEN 'cUSD'        
        WHEN token_contract_address_hash = '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' THEN 'cEUR'        
        WHEN token_contract_address_hash = '0xe8537a3d056da446677b9e9d6c5db704eaab4787' THEN 'cREAL'
        ELSE 'others' END as name,
        count(*) over (partition by from_address_hash) as number_of_transactions,
        from_address_hash,
        DATE(b.timestamp) as date

    FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` t
    LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b 
    ON t.block_hash = b.hash
    group by name, date, from_address_hash
),
wallet_by_day as (
 select  DATE(b.timestamp)  as `date`, 
        from_address_hash 
 from `celo-testnet-production.blockscout_data.rpl_transactions` t
 left join `celo-testnet-production.blockscout_data.rpl_blocks` b 
 on t.block_hash = b.hash 
 group by date, from_address_hash
),
final_data as (
    select w.`date`,
            t.name,
    count(case when t.number_of_transactions < 2 then 1 end) as new_wallet_addresses,
    count(case when t.number_of_transactions > 1 then 1 end) as returning_wallet_addresses
    from wallet_by_day w left join token_data t
    on w.from_address_hash = t.from_address_hash
    where name is not null
    group by date, name
)
select * from final_data

{% if is_incremental() %}
  WHERE (`date`) > (SELECT DATE(MAX(date)) as max_date FROM {{ this }})
{% endif %}