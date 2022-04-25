SELECT country, sum(monthly_users) AS mau
FROM `mobile_wallet_production_mobile.valora_usage`
WHERE date_trunc(date, day) = '2021-03-25'
GROUP BY 1
order by 2 desc