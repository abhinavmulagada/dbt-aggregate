with start_dates as (
select
    user_address,
    min(date(timestamp)) as join_date
FROM celo-testnet-production.mobile_wallet_production.fetch_balance
GROUP BY 1
),
start_dates_with_country as (
    SELECT
        A.user_address,
        A.join_date,
        COALESCE(C.country, 'Phone number not set') as country
    FROM start_dates A
    LEFT JOIN celo-testnet-production.mobile_wallet_production.phone_number_set AS B
    ON A.user_address = B.user_address
    left JOIN celo-testnet-production.mobile_wallet_production.countries AS C
    ON B.country_code = C.country_code
),
latest_balance_fetch as (
    SELECT
        user_address,
        max(timestamp) as latest_fetch_timestamp
    FROM celo-testnet-production.mobile_wallet_production.fetch_balance
    GROUP BY 1
),
latest_balance as (
    SELECT
        A.user_address,
        B.dollar_balance,
        B.gold_balance
    FROM latest_balance_fetch A
    JOIN celo-testnet-production.mobile_wallet_production.fetch_balance B
    ON A.user_address = B.user_address
        AND A.latest_fetch_timestamp = B.timestamp
),
per_user_summary as (
    SELECT
        A.user_address,
        A.join_date,
        A.country,
        safe_cast(B.dollar_balance as float64) as latest_dollar_balance,
        safe_cast(B.gold_balance as float64) as latest_celo_balance
    FROM start_dates_with_country A
        JOIN latest_balance B
        ON A.user_address = B.user_address
)
SELECT
    country,
    COUNTIF(DATE_DIFF(CURRENT_DATE(), join_date , DAY) <= 90) as new_addresses_last_90_days,
    COUNTIF(DATE_DIFF(CURRENT_DATE(), join_date , DAY) <= 30) as new_addresses_last_30_days,
    COUNTIF(DATE_DIFF(CURRENT_DATE(), join_date , DAY) <= 90 and coalesce(latest_dollar_balance, 0) + coalesce(latest_celo_balance, 0) > 0) as new_addresses_with_balance_last_90_days,
    COUNTIF(DATE_DIFF(CURRENT_DATE(), join_date , DAY) <= 30 and coalesce(latest_dollar_balance, 0) + coalesce(latest_celo_balance, 0) > 0) as new_addresses_with_balance_last_30_days
FROM per_user_summary
GROUP BY 1
order by 2 desc
