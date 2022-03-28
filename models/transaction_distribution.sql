{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

SELECT
  q1.to_address_hash,
  q1.total AS total,
  q2.total AS total_last_week,
  q1.date
FROM
  (select to_address_hash, 
          count(*) as total,
          date(max(b.timestamp)) as date
   from `celo-testnet-production.blockscout_data.rpl_transactions` t 
   left join `celo-testnet-production.blockscout_data.rpl_blocks` b 
   on t.block_hash = b.hash
   group by t.to_address_hash  
   order by total desc) AS q1
INNER JOIN(
  SELECT
  to_address_hash,
  COUNT(*) AS total
FROM
  `celo-testnet-production.blockscout_data.rpl_transactions`
WHERE
  inserted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY
  to_address_hash
ORDER BY
  total DESC) AS q2
ON
  q1.to_address_hash=q2.to_address_hash


{% if is_incremental() %}
  where `date` >= (select max(`date`) from {{ this }})
{% endif %}

ORDER BY
  q1.total DESC