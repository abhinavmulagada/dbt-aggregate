INSERT INTO `celo-testnet-production.blockscout_data.oracle_transactions_by_day`
WITH oracles_data AS (
    SELECT * FROM UNNEST([
        STRUCT(LOWER('0x641C6466dAE2c0B1f1f4f9c547bc3f54F4744A1d') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0x75bEcD8E400552bAc29cBE0534D8C7d6cBa49979') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0xacaD5B2913e21CcC073B80e431Fec651Cd8231C6') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0xFe9925e6AE9c4Cd50ae471B90766Aaef37AD307E') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0x12baD172b47287A754048F0D294221A499D1690f') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0xe037F31121f3A96C0Cc49D0CF55b2F5d6deFF19E') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0xD3405621f6cdCD95519a79D37f91C78e7c79CEfa') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0x0aeE051bE85BA9c7c1bC635fb76b52039341AB26') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0xCa9Ae47493F763a7166ab8310686B197984964B4') as address_hash, 'cUSD' as token),
        STRUCT(LOWER('0x223ab67272891Dd352194bE61597042eCf9C272a') as address_hash, 'cUSD' as token),

        STRUCT(LOWER('0x24c303e6395DD19806F739619960A311764e3F40') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0x87C45738DAd8Dc3D2b1cCe779E0766329cc408C6') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xeF1E143C554EFC43B0537Af00Ac27C828dE6cF8D') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xb8bDBfdd591a5be5980983A7ba1710a5F46f42B5') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xF4B4AA107F30206EA019DE145A9b778a220f9fc0') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0x929Ad7f2b781CE830014E824CA2eF0b7b8de87C2') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xDA413875FB45E5905950Bc08a908ebD246Ee6581') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xBD955F84e11EE53095F9068b88C9d2Ffd3Def707') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xCCC0B54edD8dAe3c15b5C002dd5d348495d4f7fe') as address_hash, 'cEUR' as token),
        STRUCT(LOWER('0xC5280DDbDEC37540436935C01C912869B2d5Ae1c') as address_hash, 'cEUR' as token)
    ])
), oracle_transactions AS (
    SELECT
    COUNT(1) as total_oracle_transactions,

    SUM(CAST(t.gas_used as BIGNUMERIC)) as total_oracle_gas_used,

    MIN(CAST(t.gas_used as BIGNUMERIC)) as min_gas_used,
    MAX(CAST(t.gas_used as BIGNUMERIC)) as max_gas_used,
    CAST(AVG(CAST(t.gas_used as BIGNUMERIC)) as BIGNUMERIC) as avg_gas_used,

    MIN(CAST(t.gas_price as BIGNUMERIC)) as min_gas_price,
    MAX(CAST(t.gas_price as BIGNUMERIC)) as max_gas_price,
    CAST(AVG(CAST(t.gas_price as BIGNUMERIC)) as BIGNUMERIC) as avg_gas_price,

    o.token as token,

    DATE(b.timestamp) as `date`

    FROM `celo-testnet-production.blockscout_data.transactions` AS t
    INNER JOIN oracles_data AS o
    ON t.from_address_hash = o.address_hash
    LEFT JOIN `celo-testnet-production.blockscout_data.blocks` b
    ON t.block_hash = b.hash
    WHERE DATE(b.timestamp) > (
        SELECT MAX(`date`) as max_date 
        FROM `celo-testnet-production.blockscout_data.oracle_transactions_by_day`
    )
      AND DATE(b.timestamp) < CURRENT_DATE()
    GROUP BY DATE(b.timestamp), token
), all_transactions AS (
    SELECT
        COUNT(1) as total_transactions,
        SUM(CAST(t.gas_used as BIGNUMERIC)) as total_gas_used,
        DATE(b.timestamp) as `date`,
    FROM `celo-testnet-production.blockscout_data.transactions` AS t
    LEFT JOIN `celo-testnet-production.blockscout_data.blocks` b
    ON t.block_hash = b.hash
    WHERE DATE(b.timestamp) > (
            SELECT MAX(`date`) as max_date 
            FROM `celo-testnet-production.blockscout_data.oracle_transactions_by_day`
        )
      AND DATE(b.timestamp) < CURRENT_DATE()
    GROUP BY DATE(b.timestamp)
), block_data AS (
    SELECT
        SUM(CAST(b.gas_limit as BIGNUMERIC)) as total_gas_limit,
        DATE(b.timestamp) as `date`,
    FROM `celo-testnet-production.blockscout_data.blocks` b
    WHERE DATE(b.timestamp) > (
            SELECT MAX(`date`) as max_date
            FROM `celo-testnet-production.blockscout_data.oracle_transactions_by_day`
        )
      AND DATE(b.timestamp) < CURRENT_DATE()
    GROUP BY DATE(b.timestamp)
)
SELECT
    alt.total_transactions,
    ot.total_oracle_transactions,

    bd.total_gas_limit,

    alt.total_gas_used,
    ot.total_oracle_gas_used,

    ot.min_gas_used,
    ot.max_gas_used,
    ot.avg_gas_used,

    ot.min_gas_price,
    ot.max_gas_price,
    ot.avg_gas_price,

    ot.token,
    ot.`date`

FROM oracle_transactions AS ot
LEFT JOIN all_transactions AS alt
ON ot.`date` = alt.`date`
LEFT JOIN block_data as bd
ON alt.`date` = bd.`date`;