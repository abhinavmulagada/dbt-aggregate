INSERT INTO `celo-testnet-production.blockscout_data.rpl_addresses` (fetched_coin_balance, fetched_coin_balance_block_number, `hash`, inserted_at, updated_at, nonce, decompiled, verified, is_contract)
WITH last_update_ts AS (
    SELECT MAX(updated_at) as last_timestamp
    FROM `celo-testnet-production.blockscout_data.rpl_addresses`
), updated_addresses AS (
    SELECT
        fetched_coin_balance,
        fetched_coin_balance_block_number,
        `hash`,
        TIMESTAMP(inserted_at) as inserted_at,
        TIMESTAMP(updated_at) as updated_at,
        nonce,
        decompiled,
        verified, 
        is_contract
    FROM EXTERNAL_QUERY(
        'us-west1.blockscout-rc13',
        '''
        SELECT 
            fetched_coin_balance,
            fetched_coin_balance_block_number,
            CONCAT('0x', ENCODE(hash, 'hex')) as hash,
            inserted_at,
            updated_at,
            nonce,
            decompiled,
            verified,
            CASE
                WHEN contract_code IS NOT NULL THEN True
                ELSE False
            END as is_contract
        FROM addresses
        '''
    ) as a
    WHERE TIMESTAMP(a.updated_at) > (
        SELECT last_timestamp 
        FROM last_update_ts
    )
)
SELECT * 
FROM updated_addresses
ORDER BY updated_at;