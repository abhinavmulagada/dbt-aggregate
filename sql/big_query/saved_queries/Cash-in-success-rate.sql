WITH transfers as (
    SELECT 
        timestamp as transfer_timestamp, 
        transferFrom, 
        transferTo, 
        value, 
        txHash
    FROM `celo-testnet-production.analytics.eksportisto_data`
    Where ((contract = 'StableToken'
            AND event = 'RECEIVED_EVENT_LOG'
            AND eventname = 'Transfer') 
        OR (event ='RECEIVED_TRANSFER'
            AND currencySymbol = 'cGLD')
        OR (event ='RECEIVED_EVENT_LOG'
            AND contract = 'GoldToken'
            AND eventname = 'Transfer'))
    GROUP BY 1, 2, 3, 4, 5
),
screen_cash_in as (
    SELECT 
        DATE(_PARTITIONTIME) as ds,
        anonymous_id, 
        current_screen,
        timestamp as visit_timestamp
    FROM `celo-testnet-production.mobile_wallet_production.screens`
    WHERE DATE(_PARTITIONTIME) >= '2021-03-10'
        AND context_app_name = 'Valora'
        and previous_screen = 'FiatExchangeOptions'
        and current_screen IN ('ExternalExchanges', 'FiatExchangeAmount')
),
anonymous_id_to_address_match as (
    SELECT 
        DATE(_PARTITIONTIME) as ds,
        user_address, 
        anonymous_id
    FROM `celo-testnet-production.mobile_wallet_production.fetch_balance` 
    WHERE user_address is not null
        AND context_app_name = "Valora"
    GROUP BY 1, 2, 3
),
screen_cash_in_by_address as (
    select
        A.ds,
        A.current_screen,
        A.visit_timestamp,
        B.user_address as address
    from screen_cash_in A
        JOIN anonymous_id_to_address_match B
        ON A.anonymous_id = B.anonymous_id
        AND A.ds = B.ds
),
cash_in_screen_with_actual_cash_in as (
    SELECT
        A.ds,
        A.current_screen,
        A.visit_timestamp,
        A.address,
        coalesce(B.transfer_timestamp, TIMESTAMP("2008-12-25 15:30:00+00")) as cash_in_timestamp
    FROM screen_cash_in_by_address A
        LEFT OUTER JOIN transfers B
        ON lower(A.address) = lower(B.transferTo)
),
filtered_cash_ins as (
    SELECT
        ds,
        current_screen,
        visit_timestamp,
        address,
        IF(cash_in_timestamp between visit_timestamp and TIMESTAMP_ADD(visit_timestamp, INTERVAL 1 HOUR), 1, 0) as matched_cash_in
    FROM cash_in_screen_with_actual_cash_in 
),
# SELECT 
#     ds,
#     COUNT(1) as rows_count
# FROM filtered_cash_ins 
# WHERE matched_cash_in = 1
# GROUP BY 1 order by 1 desc
summary_of_result as (
    SELECT 
        ds,
        current_screen,
        # visit_timestamp,
        address,
        SUM(matched_cash_in) as num_matched_cash_ins
    FROM filtered_cash_ins
    GROUP BY 1, 2, 3
)
SELECT 
    ds,
    current_screen, 
    COUNT(1) as num_visits,
    COUNTIF(num_matched_cash_ins > 0) as visits_with_matched_cash_in,
    COUNTIF(num_matched_cash_ins > 0) * 1.0 / COUNT(1) as pct_success
FROM summary_of_result 
GROUP BY 1, 2
order by 1, 2
