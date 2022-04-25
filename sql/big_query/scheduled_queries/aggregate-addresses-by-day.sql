INSERT INTO `celo-testnet-production.blockscout_data.addresses_by_day`
SELECT
    COUNT(*) as total_number_of_addresses,
    DATE(inserted_at) as `date`
FROM `celo-testnet-production.blockscout_data.rpl_addresses`
WHERE DATE(`inserted_at`) > (
    SELECT MAX(`date`) as max_date
    FROM `celo-testnet-production.blockscout_data.addresses_by_day`
  )
  AND DATE(`inserted_at`) < CURRENT_DATE()
GROUP BY DATE(`inserted_at`);