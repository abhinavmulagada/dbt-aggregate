DELETE FROM `celo-testnet-production.blockscout_data.rpl_market_history`
WHERE id IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_market_history` (
    id,
    `date`,
    closing_price,
    opening_price
)
SELECT
    id,
    `date`,
    closing_price,
    opening_price
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13-replica-2',
    '''
    SELECT
        id,
        date,
        closing_price,
        opening_price
    FROM market_history
    ORDER BY date DESC
    '''
);