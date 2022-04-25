with deduped_transactions as (
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
    WHERE (
            event = 'RECEIVED_TRANSFER'
            AND currencySymbol = 'cGLD')
        OR (
            event = 'RECEIVED_EVENT_LOG'
            AND contract IN ('GoldToken', 'StableToken')
            AND eventname = 'Transfer')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
addresses_with_receiving_transactions as (
    SELECT 
        DISTINCT transferTo as address
    FROM deduped_transactions
),
wallet_addresses as (
    SELECT B.walletAddress
    FROM addresses_with_receiving_transactions A
        JOIN `celo-testnet-production.analytics_eksportisto.account_to_wallet_address` B
        ON A.address = B.account
),
all_addresses as (
    SELECT address 
    FROM addresses_with_receiving_transactions
    UNION DISTINCT 
    SELECT walletAddress as address
    FROM wallet_addresses
)
SELECT *
FROM all_addresses