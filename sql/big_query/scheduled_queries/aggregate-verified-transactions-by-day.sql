DELETE FROM `celo-testnet-production.blockscout_data.verified_transactions_by_day` 
WHERE `date` >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY);

INSERT INTO `celo-testnet-production.blockscout_data.verified_transactions_by_day` (number_of_transactions, `date`)
SELECT
    COUNT(1) as number_of_transactions,
    DATE(b.timestamp) as `date`
FROM `celo-testnet-production.blockscout_data.rpl_transactions` as t

INNER JOIN `celo-testnet-production.blockscout_data.rpl_smart_contracts` AS s
ON t.to_address_hash = s.address_hash OR t.from_address_hash = s.address_hash

LEFT JOIN `celo-testnet-production.blockscout_data.rpl_blocks` b
ON b.number = t.block_number

WHERE DATE(b.timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
  AND DATE(b.timestamp) < CURRENT_DATE()
GROUP BY DATE(b.timestamp);