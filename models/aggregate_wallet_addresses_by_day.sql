{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

with transaction_count as(
    select distinct
       count(*) over (partition by from_address_hash) as number_of_wallet_transactions, 
       from_address_hash,
    from `celo-testnet-production.blockscout_data.rpl_transactions` 
),
token_transfer_count as(
    select distinct
       count(*) over (partition by from_address_hash) as number_of_token_transactions, 
       from_address_hash,
    from `celo-testnet-production.blockscout_data.rpl_token_transfers` 
),
total_transfer_count as (
    select from_address_hash, number_of_wallet_transactions as number_of_transactions from transaction_count
    union all
    select from_address_hash, number_of_token_transactions as number_of_transactions from token_transfer_count
),
wallet_by_day as (
 select  DATE(b.timestamp)  as `date`, 
        from_address_hash 
 from `celo-testnet-production.blockscout_data.rpl_transactions` t
 left join `celo-testnet-production.blockscout_data.rpl_blocks` b 
 on t.block_hash = b.hash 
 group by date, from_address_hash
)
select w.`date`,
count(case when t.number_of_transactions < 2 then 1 end) as new_wallet_addresses,
count(case when t.number_of_transactions > 1 then 1 end) as returning_wallet_addresses
from wallet_by_day w left join total_transfer_count t 
on w.from_address_hash = t.from_address_hash


{% if is_incremental() %}
  WHERE (`date`) > (SELECT DATE(MAX(date)) as max_date FROM {{ this }})
{% endif %}

group by `date`
