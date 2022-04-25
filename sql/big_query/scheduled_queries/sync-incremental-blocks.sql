DECLARE BQ_LAST_BLOCK NUMERIC;
DECLARE DSQL STRING;
SET BQ_LAST_BLOCK = (
    SELECT MAX(number)
    FROM `celo-testnet-production.blockscout_data.rpl_blocks`
);
SET DSQL = '"
    SELECT
        consensus,
        difficulty,
        gas_limit::text,
        gas_used::text,
        CONCAT(\'0x\', ENCODE(hash, \'hex\')) as hash,
        CONCAT(\'0x\', ENCODE(miner_hash, \'hex\')) as miner_hash,
        CONCAT(\'0x\', ENCODE(nonce, \'hex\')) as nonce,
        number,
        CONCAT(\'0x\', ENCODE(parent_hash, \'hex\')) as parent_hash,
        size,
        timestamp,
        total_difficulty,
        inserted_at,
        updated_at,
        refetch_needed,
        update_count,
        CONCAT(\'0x\', ENCODE(extra_data, \'hex\')) as extra_data,
        round
    FROM blocks
    WHERE number > ('|| BQ_LAST_BLOCK ||')
"';
EXECUTE IMMEDIATE '
    INSERT INTO celo-testnet-production.blockscout_data.rpl_blocks (`consensus`,`difficulty`,`gas_limit`,`gas_used`,`hash`,`miner_hash`,`nonce`,`number`,`parent_hash`,`size`,`timestamp`,`total_difficulty`,`inserted_at`,`updated_at`,`refetch_needed`,`update_count`,`extra_data`,`round`) (
        SELECT
            consensus,
            difficulty,
            gas_limit,
            gas_used,
            `hash`,
            miner_hash,
            nonce,
            number,
            parent_hash,
            size,
            TIMESTAMP(timestamp) AS `timestamp`,
            total_difficulty,
            TIMESTAMP(inserted_at) AS inserted_at,
            TIMESTAMP(updated_at) as updated_at,
            refetch_needed,
            update_count,
            extra_data,
            `round`
        FROM EXTERNAL_QUERY(
            "us-west1.blockscout-rc13",
            '|| DSQL || '
        )
    )';