{{ config(
    materialized="table"
) }}

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
),
token_data as (
    select SUM(case when CONTAINS_SUBSTR(token_contract_address_hash, '0x471EcE3750Da237f93B8E339c536989b8978a438') then value else 0 end) as celo_balance, -- CELO)
           SUM(case when CONTAINS_SUBSTR(token_contract_address_hash, '0x765DE816845861e75A25fCA122bb6898B8B1282a') then value else 0 end) as cusd_balance, -- cUSD)
           SUM(case when CONTAINS_SUBSTR(token_contract_address_hash, '0xD8763CBa276a3738E6DE85b4b3bF5FDed6D6cA73') then value else 0 end) as ceur_balance, -- cEUR)
           SUM(case when CONTAINS_SUBSTR(token_contract_address_hash, '0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787') then value else 0 end) as creal_balance, -- cREAL)
        address_hash
    from token_balances
    group by address_hash
),
address_name_data as(
    select * from 
    EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''SELECT CONCAT('0x', ENCODE(address_hash, 'hex')) as address_hash,
              name from address_names''',
              '{"default_type_for_decimal_columns":"float64"}')
)
select token_data.*, case when a.name is not null then a.name else 'N/A' end as name 
from token_data
left join address_name_data a on token_data.address_hash = a.address_hash
order by celo_balance desc 
limit 10000