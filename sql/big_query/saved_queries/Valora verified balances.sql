SELECT *
FROM (
    SELECT
        address,
        MAX(cusd) as  cusd_balance,
        MAX(celo) as celo_balance
    FROM `mobile_wallet_production_mobile.valora_balances`
    WHERE date = '2021-01-08'
    GROUP BY 1
) A
JOIN (
    SELECT user_address
    FROM `mobile_wallet_production_mobile.verification_events`
    WHERE event='verification_complete'
      AND context_app_namespace = "co.clabs.valora"
      AND context_app_version < "1.8.0"
    GROUP BY 1
) B
ON A.address = B.user_address

ORDER BY cusd_balance desc
limit 100