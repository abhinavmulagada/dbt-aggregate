SELECT 
    COUNT(1) as verified_address_count, 
    COUNTIF(cusd_balance > 0) as verified_cusd_address_count, 
    COUNTIF(celo_balance > 0) as verified_celo_address_count
FROM (
    SELECT
    address,
    cusd as  cusd_balance,
    celo as celo_balance
    FROM `mobile_wallet_production_mobile.valora_balances`
    WHERE date='2020-12-13'
    GROUP BY 1, 2, 3
) A
JOIN (
    SELECT user_address 
    FROM `mobile_wallet_production_mobile.verification_events` 
    WHERE event = 'verification_complete'
      AND context_app_namespace = "co.clabs.valora"
      AND context_app_version < "1.8.0"
    GROUP BY 1
) B
ON A.address = B.user_address