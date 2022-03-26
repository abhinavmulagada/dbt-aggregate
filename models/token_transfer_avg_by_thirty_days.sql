/*
Each days shows the average number of transactions completed in the past 30 days
*/

{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

select date,
       avg(number_of_transactions) over (order by date rows between 29 preceding and current row) as prev_thirty_day_avg,
       number_of_transactions
from {{ ref('aggregate_transactions') }}

{% if is_incremental() %}
  WHERE `date` > (SELECT MAX(`date`) as max_date FROM {{ this }})
{% endif %}