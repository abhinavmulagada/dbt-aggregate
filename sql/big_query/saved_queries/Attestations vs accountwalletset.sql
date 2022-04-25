select
    attestations ,
    COUNT(1) as addresses_count
FROM (
    SELECT B.account, B.walletAddress, coalesce(attestations, 0) as attestations
    FROM (
        (
            SELECT account, COUNT(1) as attestations
            FROM analytics_eksportisto.attestation_completed
            GROUP BY 1
        ) AS A 
        RIGHT JOIN (
            SELECT account, walletAddress
            FROM analytics.eksportisto_data
            WHERE eventname = 'AccountWalletAddressSet'
            GROUP BY 1, 2
        ) AS B
        ON lower(A.account) = lower(B.account)
    )
)
GROUP BY 1
order by attestations