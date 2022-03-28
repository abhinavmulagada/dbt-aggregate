with daily_data as (
select distinct to_address_hash, 
       date(b.timestamp) as date,
       count(*) as daily_total
   from `celo-testnet-production.blockscout_data.rpl_transactions` t 
   left join `celo-testnet-production.blockscout_data.rpl_blocks` b 
   on t.block_hash = b.hash
   group by t.to_address_hash, date
),
total_data as (
    select distinct sum(daily_total) as total, 
       to_address_hash
    from daily_data 
    group by to_address_hash
),
weekly_data as (
    select distinct sum(daily_total) as weekly_total,   
           to_address_hash,
           max(date) as max_date
    from daily_data 
    where date < DATE(CURRENT_TIMESTAMP())
    and date >= DATE_SUB(DATE(CURRENT_TIMESTAMP()), INTERVAL 7 DAY)
    group by to_address_hash
)
select t.total, 
       w.weekly_total, 
       w.to_address_hash, 
       w.max_date 
from weekly_data w 
inner join total_data t 
on t.to_address_hash = w.to_address_hash 
