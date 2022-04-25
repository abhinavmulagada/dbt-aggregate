WITH ordered_balances AS (
    SELECT user_address, 
        dollar_balance, 
        gold_balance,
        ROW_NUMBER() OVER (partition by user_address ORDER BY timestamp DESC) rn
    FROM `celo-testnet-production.mobile_wallet_production.fetch_balance` 
    WHERE DATE(_PARTITIONTIME) < '2021-03-04'
    AND user_address is not null
    AND context_app_name = "Valora"
),
pre_existing_addresses_with_balances as (
    SELECT 
        user_address,
        coalesce(cast(dollar_balance as float64), 0) as dollar_balance,
        coalesce(cast(gold_balance as float64), 0) as celo_balance
    FROM ordered_balances
    WHERE rn = 1
),
active_address_to_device_mapping as (
    SELECT 
        DATE(_PARTITIONTIME) as ds,
        user_address, 
        COUNT(1) as num_fetch_balance
    FROM `celo-testnet-production.mobile_wallet_production.fetch_balance` 
    WHERE DATE(_PARTITIONTIME) between '2021-03-04' and "2021-03-31"
        AND user_address is not null
        AND context_app_name = "Valora"
    GROUP BY 1, 2
),
filtered_active_addresses as (
    SELECT 
        A.*,
        B.dollar_balance as starting_dollar_balance,
        B.celo_balance  as starting_celo_balance
    FROM active_address_to_device_mapping A 
        JOIN pre_existing_addresses_with_balances B
        ON A.user_address = B.user_address
),
user_l28_status as (
    SELECT 
        user_address,
        starting_dollar_balance > 0 as had_dollar_balance,
        starting_celo_balance > 0 as had_celo_balance,
        COUNT(1) as l28,
        SUM(num_fetch_balance) as total_balance_fetch,
    FROM filtered_active_addresses 
    GROUP BY 1, 2, 3
)
SELECT 
l28,
COUNT(1) as num_users
FROM user_l28_status 
GROUP BY 1 order by 1 