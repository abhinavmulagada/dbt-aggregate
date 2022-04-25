with deduped_transations as (
    SELECT
        timestamp,
        transferFrom,
        transferTo,
        value,
        event,
        currencySymbol,
        contract,
        eventname,
        txHash
    FROM `celo-testnet-production`.`analytics`.`eksportisto_data`
    WHERE ((
            event ='RECEIVED_TRANSFER'
            AND currencySymbol = 'cGLD')
        OR (
            event ='RECEIVED_EVENT_LOG'
            AND contract = 'GoldToken'
            AND eventname = 'Transfer'))
        and transferFrom IN (
            '0x71AdeE1b7aC4756dF017473E6beE668b59b503e7',
            '0x406aa797d88807ea9A02EaA6a37e0B20c0223272',
            '0x8AF1400045e713d2F833Ec6d1D76C2F0EA675418',
            '0xfbBCe3c212F7F03Bc055156211772bDa4F2522Dd',
            '0x1a0E08D4BC0Edb05036cE81D6B21278b5381c680',
            '0xD87cb8Ceb4Ff5100b7E17288B023DB4b30324964',
            '0x5BC2e7394FBD1074eAcb78923802952DeF4Ba8c2',
            '0xeF0b9226fC6A5acD320720A2fCEC6B31ed2ac862'
        )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
rewards_received as (
    SELECT
        transferTo,
        SUM(value) as celo_rewards_received,
        COUNT(1) as number_of_rewards_received
    FROM deduped_transations
    GROUP BY 1
),
balance_summary as (
    SELECT
        B.address,
        B.celo_balance,
        B.transactions_to_date,
        A.celo_rewards_received,
        A.number_of_rewards_received
    FROM rewards_received A
    JOIN `celo-testnet-production.analytics_eksportisto.daily_celo_balances` B
    ON lower(A.transferTo) = B.address and B.ds='2021-03-25'
)
SELECT
    COUNT(1) as num_addresses,
    COUNTIF(round(celo_balance, 2) = 0) * 1.0 / COUNT(1) as pct_with_zero_celo,
    COUNTIF(ROUND(celo_balance, 3) = ROUND(celo_rewards_received , 3)) * 1.0 / COUNT(1) as pct_with_rewards_celo_balance,
    COUNTIF(transactions_to_date = number_of_rewards_received) * 1.0 / COUNT(1) as pct_with_only_rewards_celo_transactions
FROM balance_summary
