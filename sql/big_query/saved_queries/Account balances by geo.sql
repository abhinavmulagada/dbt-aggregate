WITH balances_country as 
(
    SELECT 
        address,
        country,
        cusd
    FROM `celo-testnet-production.mobile_wallet_production_mobile.valora_balances`
    WHERE date = '2021-06-03'
),
avg_balances as 
(   
    SELECT address, sum(eod_balance) / 7.0 as avg_cusd_balance, 
    FROM  `celo-testnet-production.analytics_eksportisto.daily_balances`
    WHERE ds between '2021-05-24' and '2021-05-30'
    GROUP BY 1
),
attestations as (
    SELECT 
        B.walletAddress,
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
    WHERE attestations >=3
),
confirmed_addresses_by_country as (
    SELECT 
        B.address,
        B.country,
        Coalesce(C.avg_cusd_balance, 0)   as avg_cusd_balance
    FROM attestations A
    JOIN balances_country B 
    ON lower(A.walletAddress) = lower(B.address)
    left JOIN avg_balances C
    ON lower(A.walletAddress) = lower(C.address)
)
SELECT 
    case
        when avg_cusd_balance = 0 then "0. 0 cusd" 
        when avg_cusd_balance < 1 then "1. <1 cusd"
        when avg_cusd_balance between 1 and 10 then "2. 1 to 10 cusd"
        when avg_cusd_balance > 10 then "3. >10 cusd"
    end as balance_tier,
    round(sum(IF(country IN ('US', 'GB', 'AU'), 1, 0)) * 100.0 / COUNT(1)) as pct_US_GB_AU_accounts,
    ROUND(SUM(IF(country = 'BR', 1, 0)) * 100.0 / COUNT(1)) as pct_BR_accounts,
    ROUND(SUM(IF(country IN ('ES', 'IT', 'FR', 'DE', 'NL', 'PT'), 1, 0)) * 100.0 / COUNT(1)) as pct_EU_accounts,
    ROUND(SUM(IF(country IN ('PH'), 1, 0)) * 100.0 / COUNT(1)) as pct_PH_accounts,
    COUNT(1) as num_addresses
FROM confirmed_addresses_by_country 
GROUp by 1
order by 1