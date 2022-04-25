DECLARE BQ_LAST_DATETIME TIMESTAMP;
DECLARE DSQL STRING;
SET BQ_LAST_DATETIME = (
    SELECT MAX(inserted_at)
    FROM celo-testnet-production.blockscout_data.rpl_token_transfers
);
SET DSQL = '"
    SELECT 
        CONCAT(\'0x\', ENCODE(transaction_hash, \'hex\')) AS transaction_hash,
        CONCAT(\'0x\', ENCODE(block_hash, \'hex\')) AS block_hash,
        log_index,
        CONCAT(\'0x\', ENCODE(from_address_hash, \'hex\')) AS from_address_hash,
        CONCAT(\'0x\', ENCODE(to_address_hash, \'hex\')) AS to_address_hash,
        amount::text,
        token_id::text,
        block_number,
        CONCAT(\'0x\', ENCODE(token_contract_address_hash, \'hex\')) AS token_contract_address_hash,
        comment,
        inserted_at,
        updated_at
    FROM token_transfers
    WHERE inserted_at > \'('|| BQ_LAST_DATETIME ||')\'
    "';
EXECUTE IMMEDIATE '
    INSERT INTO celo-testnet-production.blockscout_data.rpl_token_transfers (transaction_hash, block_hash, log_index, from_address_hash, to_address_hash, amount, token_id, token_contract_address_hash, inserted_at, updated_at, block_number, comment) (
        SELECT
            transaction_hash,
            block_hash,
            log_index,
            from_address_hash,
            to_address_hash,
            amount,
            token_id,
            token_contract_address_hash,
            TIMESTAMP(inserted_at) AS inserted_at,
            TIMESTAMP(updated_at) AS updated_at,
            block_number,
            comment
        FROM EXTERNAL_QUERY(
            "us-west1.blockscout-rc13",
            '|| DSQL || '
        )
    )
';