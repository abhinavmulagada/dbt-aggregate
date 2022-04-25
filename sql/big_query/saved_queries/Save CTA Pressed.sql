WITH attestations as (
    SELECT 
        B.walletAddress,
        A.account
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
valora_users as (
    SELECT address
    FROM `celo-testnet-production.mobile_wallet_production.valora_balances`
    WHERE date = '2021-07-11'
    GROUP BY 1
),
visits_to_details_page as (
    SELECT user_address, COUNT(1) as num_rewards_details_visits
    FROM `celo-testnet-production.mobile_wallet_production.rewards_screen_cta_pressed` 
    WHERE DATE(_PARTITIONTIME) between "2021-06-29" and "2021-07-11"
    GROUP BY 1
),
full_address_details as (
    SELECT
        coalesce(B.account, A.address) as abtest_address, -- for exposure to in-app experience
        A.address, -- for rewards distribution
        B.account IS NOT NULL as has_verified_phone_number,
        coalesce(C.num_rewards_details_visits, 0) as num_rewards_details_visits
    FROM valora_users A
        LEFT JOIN attestations B
        ON lower(A.address) = lower(B.walletAddress)
        LEFT JOIN visits_to_details_page  C
            ON lower(A.address) = lower(C.user_address)
)
SELECT 
    CASE
        WHEN abtest_address <'0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000' then '0. test_group'
        WHEN abtest_address >= '0x8000000000000000000000000000000000000000' and address >= '0x8000000000000000000000000000000000000000' then '1. control_group'
        WHEN abtest_address >= '0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000' then 'no_in_app_but_earnings'
        else 'in_app_but_no_earnings'
    END as test_group,
    -- CASE
    --     WHEN pre_experiment_balance IS NULL THEN '0. New users'
    --     WHEN pre_experiment_balance = 0 THEN '1. No pre_experiment_balance'
    --     WHEN pre_experiment_balance <= 0.01 THEN '2. Less than 1c'
    --     WHEN pre_experiment_balance <= 1 THEN '3. Less than 1cUSD'
    --     WHEN pre_experiment_balance <= 10 THEN '4. Less than 10 cUSD'
    --     WHEN pre_experiment_balance <= 100 THEN '5. Less than 100 cUSD'
    --     ELSE 'More than 100cUSD'
    -- END as pre_experiment_balance,
    COUNT(1) as num_samples,
    COUNTIF(has_verified_phone_number) as verified_users_count,
    COUNTIF(num_rewards_details_visits>0) * 100.0 / COUNT(1) as pct_users_press_cta,
    avg(num_rewards_details_visits) as avg_rewards_details_visits,
    avg(num_rewards_details_visits) + 1.96 * (SELECT stddev(num_rewards_details_visits) from full_address_details) / Sqrt(count(1)) as upper_CI,
    avg(num_rewards_details_visits) - 1.96 * (SELECT stddev(num_rewards_details_visits) from full_address_details) / Sqrt(count(1)) as lower_CI
FROM full_address_details
-- WHERE has_verified_phone_number 
-- WHERE ((abtest_address <'0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000') OR (abtest_address >= '0x8000000000000000000000000000000000000000' and address >= '0x8000000000000000000000000000000000000000'))
GROUP BY 1