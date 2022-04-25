DELETE FROM `celo-testnet-production.blockscout_data.dash_stable_token_stats_by_day` 
WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY);

INSERT INTO `celo-testnet-production.blockscout_data.dash_stable_token_stats_by_day`
WITH stables_data AS (
    SELECT
        s.name,
        t.contract_address_hash,
        CAST(t.total_supply AS BIGNUMERIC) as total_supply,
        t.holder_count
    FROM `celo-testnet-production.blockscout_data.celo_stable_tokens` AS s
    INNER JOIN `celo-testnet-production.blockscout_data.rpl_tokens` AS t
    ON LOWER(t.contract_address_hash) = LOWER(s.contract_address_hash)
), token_balances AS (
    SELECT
        token_contract_address_hash,
        avg_balance
    FROM EXTERNAL_QUERY(
        'us-west1.blockscout-rc13',
        '''
        SELECT
        CONCAT('0x', ENCODE(token_contract_address_hash, 'hex')) as token_contract_address_hash,
        AVG(value) as avg_balance
        FROM address_current_token_balances
        WHERE token_contract_address_hash IN (
            '\\x765de816845861e75a25fca122bb6898b8b1282a',
            '\\xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
            '\\xe8537a3d056da446677b9e9d6c5db704eaab4787')
          AND value > 0
        GROUP BY token_contract_address_hash
        '''
    )
), token_transfers AS (
    SELECT
        `date`,
        contract_address_hash,
        number_of_transfers
    FROM EXTERNAL_QUERY(
        'us-west1.blockscout-rc13',
        '''
        SELECT
            MAX(DATE(b.timestamp)) as date,
            CONCAT('0x', ENCODE(t.token_contract_address_hash, 'hex')) AS contract_address_hash,
            COUNT(1) as number_of_transfers
        FROM token_transfers AS t
        INNER JOIN blocks AS b
        ON b.number = t.block_number
        WHERE b.timestamp >= (current_date::timestamp - INTERVAL '1 day')
          AND b.timestamp < current_date::timestamp
          AND token_contract_address_hash IN (
            '\\x765de816845861e75a25fca122bb6898b8b1282a',
            '\\xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
            '\\xe8537a3d056da446677b9e9d6c5db704eaab4787')
        GROUP BY token_contract_address_hash, DATE(b.timestamp)
        '''
    )
)
SELECT
    t.`date`,
    s.name,
    s.contract_address_hash,
    s.total_supply,
    s.holder_count,
    b.avg_balance,
    t.number_of_transfers
FROM stables_data AS s
INNER JOIN token_balances AS b
ON s.contract_address_hash = b.token_contract_address_hash
INNER JOIN token_transfers AS t
ON t.contract_address_hash = s.contract_address_hash; 