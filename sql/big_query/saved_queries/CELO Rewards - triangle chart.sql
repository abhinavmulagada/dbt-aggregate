with deduped_transactions as (
    SELECT 
        timestamp, 
        timestamp_trunc(timestamp, day) as ds,
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
            '0x75498D5471ca24779af601371E958845b0B2df00',
            '0x44AA75d94B70aaF2fFcEA9d61081F2BE86DC8536',
            '0x47B9b9b88CCBCF580472C8E17184bE073B0E3a83',
            '0x7F501701024490714354d8D99014c465D991B407',
            '0xc0b2Ab5E740E55AB26678566038c47C60F005340',
            '0xfaA7649E51Ec69ABf7Ba21422a433896bBd9C3cA',
            '0x9bCE69a441FF5C4a1391385B45Cb36737D735874',
            '0x71AdeE1b7aC4756dF017473E6beE668b59b503e7',
            '0x406aa797d88807ea9A02EaA6a37e0B20c0223272',
            '0x8AF1400045e713d2F833Ec6d1D76C2F0EA675418',
            '0xfbBCe3c212F7F03Bc055156211772bDa4F2522Dd',
            '0x1a0E08D4BC0Edb05036cE81D6B21278b5381c680',
            '0xD87cb8Ceb4Ff5100b7E17288B023DB4b30324964',
            '0x5BC2e7394FBD1074eAcb78923802952DeF4Ba8c2',
            '0xeF0b9226fC6A5acD320720A2fCEC6B31ed2ac862'
        )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
join_dates as (
    select
        lower(transferTo) as address,
        min(ds) as date_joined
    from deduped_transactions
    GROUP BY 1
),
address_weekly_activity as (
    SELECT
        A.address,
        A.date_joined,
        B.ds,
        round(date_diff(date(ds), date(date_joined), DAY) / 7.0) as weeks_since_joined
    FROM join_dates A
        JOIN deduped_transactions B
        ON A.address = lower(B.transferTo)
),
weekly_new_users as (
    SELECT
        date_joined,
        COUNT(1) as addresses_count
    FROM join_dates 
    GROUP BY 1
),
weekly_actives_by_cohort as (
    select
        ds,
        weeks_since_joined,
        date_joined,
        COUNT(1) as weekly_actives
    FROM address_weekly_activity 
    GROUP BY 1, 2, 3
)
SELECT 
    B.date_joined,
    B.weeks_since_joined,
    B.weekly_actives,
    A.addresses_count 
FROM weekly_new_users A 
JOIN weekly_actives_by_cohort B
ON A.date_joined = B.date_joined
order by 1, 2