INSERT INTO
    `celo-testnet-production.blockscout_data.rpl_address_token_balances` (
        address_hash,
        block_number,
        token_contract_address_hash,
        value,
        value_fetched_at,
        inserted_at,
        updated_at
    )
WITH
    last_update_ts AS (
        SELECT MAX(updated_at) AS last_timestamp
        FROM `celo-testnet-production.blockscout_data.rpl_address_token_balances`
    ),
    updated_addresses AS (
        SELECT
            address_hash,
            block_number,
            token_contract_address_hash,
            value,
            TIMESTAMP(value_fetched_at) AS value_fetched_at,
            TIMESTAMP(inserted_at) AS inserted_at,
            TIMESTAMP(updated_at) AS updated_at
        FROM EXTERNAL_QUERY(
            'us-west1.blockscout-rc13-replica-2',
            '''
            SELECT 
                CONCAT('0x', ENCODE(address_hash, 'hex')) as address_hash,
                block_number,
                CONCAT('0x', ENCODE(token_contract_address_hash, 'hex')) as token_contract_address_hash,
                value::text,
                value_fetched_at,
                inserted_at,
                updated_at
            FROM address_current_token_balances
            '''
        ) AS a
        WHERE TIMESTAMP(a.updated_at) > (
            SELECT last_timestamp 
            FROM last_update_ts
        )
    )
SELECT *
FROM updated_addresses
ORDER BY updated_at;