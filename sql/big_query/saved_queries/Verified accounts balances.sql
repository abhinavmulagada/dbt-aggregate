WITH attestations as (
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
            SELECT account, walletAddress
            FROM analytics.eksportisto_data
            WHERE eventname = 'AccountWalletAddressSet'
            GROUP BY 1, 2
        ) AS B
        ON A.account = B.account
    )
    WHERE A.attestations >= 3
),
all_dates as (
    SELECT timestamp_trunc(timestamp, day) as ds
    FROM analytics.eksportisto_data
    GROUP BY 1
),
address_x_dates as
(
	SELECT walletAddress, ds
	FROM (
		SELECT B.attestation_date, B.walletAddress, A.ds
		FROM all_dates A
		CROSS JOIN attestations B
	)
	WHERE ds >= attestation_date
)
select
    A.ds,
    COUNTIF(coalesce(B.eod_balance, 0) >= 1) * 1.0 / COUNT(1) as pct_addresse_over_1cUSD,
    COUNTIF(coalesce(B.eod_balance, 0) >= 5) * 1.0 / COUNT(1) as pct_addresse_over_5cUSD,
    COUNTIF(coalesce(B.eod_balance, 0) >= 10) * 1.0 / COUNT(1) as pct_addresse_over_10cUSD,
    COUNTIF(coalesce(B.eod_balance, 0) >= 20) * 1.0 / COUNT(1) as pct_addresse_over_20cUSD,
    COUNTIF(coalesce(B.eod_balance, 0) >= 100) as verified_accounts_100,
    COUNTIF(coalesce(B.eod_balance, 0) >= 500) as verified_accounts_500,
    COUNT(1) as addresses_count,
    SUM(coalesce(B.eod_balance, 0)) as verified_accounts_cumulative_balances
    -- AVG(coalesce(B.eod_balance, 0)) as verified_accounts_avg_balances,
    -- APPROX_QUANTILES(B.eod_balance, 2)[OFFSET(1)] as median
FROM address_x_dates A
LEFT JOIN analytics_eksportisto.daily_balances B
ON lower(A.walletAddress) = lower(B.address)
    AND A.ds = B.ds
GROUP BY 1
ORDER BY 1