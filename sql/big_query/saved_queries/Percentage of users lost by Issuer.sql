SELECT issuer_failure.issuer, FailureRate, AttributableFailureRate, 1-(completed/total) AS percentageOfUsersLost, completed, total
FROM
    # Individual attestation completion rate by issuer
    (SELECT issuer, (`TotalMissing`/`Total`) AS `FailureRate`,  (`OnlyMissingIssuer`/`Total`) AS `AttributableFailureRate`
    FROM
        (SELECT issuer, issuerName, sum(`total`) AS `Total`, sum(`totalMissed`) AS `TotalMissing`, sum(`onlyIssuerMissing`) AS `OnlyMissingIssuer`
        FROM `celo-testnet-production.analytics_eksportisto.missed_attestations` 
        WHERE DATE(date) >= "2021-01-29"
        AND DATE(date) < "2021-02-05"
        GROUP BY `issuer`, `issuerName`)
    ORDER BY `AttributableFailureRate` DESC) AS issuer_failure
INNER JOIN
    # Flow completion rate by issuer
    (SELECT issuer, COUNTIF(completed = true) AS completed, COUNT(*) AS total
    FROM
        # Completion event by user
        (SELECT DISTINCT verification_events.user_address, completed
        FROM `celo-testnet-production.mobile_wallet_production_mobile.verification_events` AS verification_events
        FULL OUTER JOIN 
            (SELECT DISTINCT user_address, true as completed
            FROM `celo-testnet-production.mobile_wallet_production_mobile.verification_events` 
            WHERE event = "verification_complete"
            AND DATE(date) >= "2021-01-29"
            AND DATE(date) < "2021-02-05") AS completed_set
        ON verification_events.user_address = completed_set.user_address
        WHERE DATE(date) >= "2021-01-29"
        AND DATE(date) < "2021-02-05") AS user_status 
    INNER JOIN
        # Issuer / walletAddress pair
        (SELECT issuer, walletAddress
        FROM `celo-testnet-production.analytics_eksportisto.attestation_state` AS attestation_state
        INNER JOIN
        `celo-testnet-production.analytics_eksportisto.account_to_wallet_address` AS account_to_wallet_address
        ON attestation_state.account = account_to_wallet_address.account
        WHERE DATE(timestamp) >= "2021-01-29"
        AND DATE(timestamp) < "2021-02-05") AS pairing
    ON LOWER(user_status.user_address) = LOWER(pairing.walletAddress)
    GROUP BY issuer) AS issuer_completion
ON issuer_failure.issuer = issuer_completion.issuer
ORDER BY FailureRate