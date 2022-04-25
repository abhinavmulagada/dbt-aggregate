WITH pre_existing_users as (
    SELECT user_address
    FROM `celo-testnet-production.mobile_wallet_production.fetch_balance`
    WHERE DATE(_PARTITIONTIME) < DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    GROUP BY 1
),
attestations as (
    SELECT 
        lower(B.walletAddress) as address,
        A.attestations,
        A.attestation_date
    FROM (
        (
            SELECT account, COUNT(1) as attestations, MAX(timestamp_trunc(timestamp, day)) as attestation_date, 
            FROM analytics_eksportisto.attestation_completed
            GROUP BY 1
        ) AS A
        JOIN (
            SELECT account, walletAddress
            FROM analytics.eksportisto_data
            WHERE eventname = 'AccountWalletAddressSet'
            GROUP BY 1, 2
        ) AS B
        ON A.account = B.account
    )
    WHERE A.attestations >= 3
),
user_transactions_count as (
    SELECT DATE(_PARTITIONTIME) as ds, user_address, COUNT(1) as num_outbound_transactions
    FROM `celo-testnet-production.mobile_wallet_production.transaction_confirmed` 
    WHERE DATE(_PARTITIONTIME) between DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and context_app_name = 'Valora'
    GROUP BY 1, 2
),
dates as (
    SELECT ds
    FROM user_transactions_count 
    GROUP BY 1
),
dates_x_users as (
    SELECT 
        A.user_address,
        B.ds
    FROM pre_existing_users A
    CROSS JOIN dates B
),
filtered_dates_x_users as (
    SELECT 
        A.user_address,
        A.ds
    FROM dates_x_users A
        JOIN attestations B
        ON A.user_address = B.address
),
filtered_users_tx_count as (
    SELECT
        B.user_address,
        B.ds,
        COALESCE(A.num_outbound_transactions , 0) as num_daily_sent_transactions
    FROM user_transactions_count A
        RIGHT OUTER JOIN filtered_dates_x_users B
        ON A.user_address = B.user_address
        AND A.ds = B.ds
),
per_address_summary as (
    SELECT 
        user_address,
        COUNTIF(num_daily_sent_transactions > 0) as l28,
        SUM(num_daily_sent_transactions) as monthly_sent_transactions
    FROM filtered_users_tx_count
    GROUP BY 1
)
SELECT 
    l28,
    COUNT(1) as num_users,
    SUM(monthly_sent_transactions) * 1.0 / COUNT(1) as avg_daily_sent_tx_per_active_day_of_use
FROM per_address_summary
GROUP by 1
order by 1