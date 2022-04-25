SELECT (completed IS NOT NULL AND completed = true) AS Verified, COUNT (*) AS count
FROM
    (SELECT DISTINCT T1.user_address, completed
    FROM 
        (SELECT DISTINCT user_address 
        FROM `celo-testnet-production.analytics_mobile.transaction_times` 
        WHERE DATE(start_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
        ) AS T1
    LEFT OUTER JOIN
        (SELECT DISTINCT user_address, completed
        FROM `celo-testnet-production.analytics_mobile.verification_by_date`
        ) AS T2
    ON T1.user_address = T2.user_address)
GROUP BY Verified