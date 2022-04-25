SELECT failureRate, SUM(completed) 
OVER (
    ORDER BY failureRate
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
FROM (
    SELECT failureRate, COUNTIF(completed = true) AS completed, COUNT(*) AS total
    FROM (
        SELECT user_address, completed, MAX(FailureRate) AS failureRate
        FROM
            # Individual attestation completion rate by issuer
            (SELECT issuer, ROUND((`TotalMissing`/`Total`), 1) AS `FailureRate`
            FROM
                (SELECT issuer, issuerName, sum(`total`) AS `Total`, sum(`totalMissed`) AS `TotalMissing`, sum(`onlyIssuerMissing`) AS `OnlyMissingIssuer`
                FROM `celo-testnet-production.analytics_eksportisto.missed_attestations` 
                WHERE DATE(date) > "2020-12-16" 
                AND DATE(date) < "2021-01-16"
                GROUP BY `issuer`, `issuerName`)
            ORDER BY `FailureRate` DESC) AS issuer_failure_bucketized
        INNER JOIN (
            SELECT issuer, user_address, completed
            FROM (
                SELECT DISTINCT verification_events.user_address, completed
                FROM `celo-testnet-production.mobile_wallet_production_mobile.verification_events` AS verification_events
                FULL OUTER JOIN (
                    SELECT DISTINCT user_address, true as completed
                    FROM `celo-testnet-production.mobile_wallet_production_mobile.verification_events` 
                    WHERE event = "verification_complete"
                      AND DATE(date) > "2020-12-16" 
                      AND DATE(date) < "2021-01-16"
                ) AS completed_set
                ON verification_events.user_address = completed_set.user_address
                WHERE DATE(date) > "2020-12-16" 
                  AND DATE(date) < "2021-01-16"
            ) AS user_status 
            INNER JOIN
                # Issuer / walletAddress pair
                (SELECT issuer, walletAddress
                FROM `celo-testnet-production.analytics_eksportisto.attestation_state` AS attestation_state
                INNER JOIN `celo-testnet-production.analytics_eksportisto.account_to_wallet_address` AS account_to_wallet_address
                ON attestation_state.account = account_to_wallet_address.account
                WHERE DATE(timestamp) > "2020-12-16" 
                  AND DATE(timestamp) < "2021-01-16") AS pairing
            ON LOWER(user_status.user_address) = LOWER(pairing.walletAddress)) AS issuer_user_completion
        ON issuer_failure_bucketized.issuer = issuer_user_completion.issuer
        GROUP BY user_address, completed)
    GROUP BY failureRate)