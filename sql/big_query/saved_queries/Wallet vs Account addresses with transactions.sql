SELECT
    'walletJoin' as join_column,
    COUNT(DISTINCT address) as resulting_addresses
FROM addresses_with_receiving_transactions A
JOIN `celo-testnet-production.analytics_eksportisto.account_to_wallet_address` B
ON A.address = B.walletAddress

UNION ALL

SELECT
    'accountJoin' as join_column,
    COUNT(DISTINCT address) as resulting_addresses
FROM addresses_with_receiving_transactions A
JOIN `celo-testnet-production.analytics_eksportisto.account_to_wallet_address` B
ON A.address = B.account