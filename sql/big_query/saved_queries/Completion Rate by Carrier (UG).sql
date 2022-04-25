SELECT *, completedCount/total AS completionRate
FROM (
    SELECT 
        context_network_carrier, 
        COUNTIF(isCompleted) AS completedCount, 
        COUNT(*) AS total
    FROM `celo-testnet-production.analytics_eksportisto.attestation_state_with_user_data` AS t1
    INNER JOIN (
        SELECT DISTINCT user_address, context_network_carrier
        FROM `celo-testnet-production.mobile_wallet_production_mobile.verification_events`) AS t2
    ON t1.user_address = t2.user_address
    WHERE t1.country = "UG"
      AND t1.timestamp >= TIMESTAMP("2021-03-01 00:00:00")
    GROUP BY context_network_carrier
)
ORDER BY completionRate ASC