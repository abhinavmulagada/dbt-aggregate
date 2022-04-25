with verif_1_15 as (
    SELECT
        user_address,
        COUNT(1) as num_attempts
    FROM celo-testnet-production.mobile_wallet_production.verification_start
    WHERE context_app_version = '1.15.1'
    GROUP BY 1
),
verifs as (
    SELECT 
        B.walletAddress,
        A.attestations,
        A.attestation_date
    FROM (
        (
            SELECT account, COUNT(1) as attestations, MAX(timestamp_trunc(timestamp, day)) as attestation_date, 
            FROM analytics_eksportisto.attestation_completed
            GROUP BY 1
        ) AS A
        JOIN (
            SELECT
            walletAddress, account
            FROM 
            (
                SELECT row_number() OVER (PARTITION BY walletAddress order by timestamp desc) as row_, account, walletAddress
                FROM analytics.eksportisto_data
                WHERE eventname = 'AccountWalletAddressSet'
            )
            WHERE row_ = 1
        ) AS B
        ON A.account = B.account
    )
)
SELECT 
    num_attestations,
    COUNT(1) as num_attempts
FROM (
    SELECT 
        A.user_address,
        Coalesce(B.attestations, 0) as num_attestations
    FROM verif_1_15 A
        left join verifs B
        ON lower(A.user_address) = lower(B.walletAddress)
)
GROUP BY 1
order by 1