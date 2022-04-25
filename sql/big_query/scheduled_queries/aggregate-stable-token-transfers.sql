DELETE FROM `celo-testnet-production.blockscout_data.dash_stable_token_transfers`
WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY);

INSERT INTO `celo-testnet-production.blockscout_data.dash_stable_token_transfers` (`date`, `name`, `contract_address_hash`, `avg_amount`, `total_amount`, `number_of_transfers`)
SELECT
    s1.`date`,
    c.name,
    s1.contract_address_hash,
    s1.avg_amount,
    s1.total_amount,
    s1.number_of_transfers
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT
        MAX(DATE(b.timestamp)) AS date,
        CONCAT('0x', ENCODE(t.token_contract_address_hash, 'hex')) AS contract_address_hash,
        AVG(amount) AS avg_amount,
        SUM(amount) AS total_amount,
        COUNT(1) AS number_of_transfers
    FROM blocks AS b
    LEFT JOIN token_transfers AS t
    ON b.number = t.block_number
    WHERE token_contract_address_hash IN (
        '\\x765de816845861e75a25fca122bb6898b8b1282a',
        '\\xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
        '\\xe8537a3d056da446677b9e9d6c5db704eaab4787'
    ) 
      AND t.to_address_hash NOT IN ('\\x0000000000000000000000000000000000000000')
      AND t.from_address_hash NOT IN ('\\x0000000000000000000000000000000000000000')
      AND b.timestamp >= (current_date::timestamp - INTERVAL '1 day')
      AND b.timestamp < current_date::timestamp
    GROUP BY DATE(b.timestamp), t.token_contract_address_hash
    ORDER BY DATE(b.timestamp);
    '''
) s1
LEFT JOIN `celo-testnet-production.blockscout_data.celo_stable_tokens` c
ON s1.contract_address_hash = c.contract_address_hash;