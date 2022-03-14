{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

select count(*) as wallet_addresses_count,
       DATE (inserted_at) as `date`
from `celo-testnet-production.blockscout_data.rpl_celo_wallets`

{% if is_incremental() %}
  WHERE DATE (inserted_at) > (SELECT MAX(`date`) as max_date 
                            FROM `celo-testnet-production.blockscout_data.addresses_by_day`)
{% endif %}

group by `date`