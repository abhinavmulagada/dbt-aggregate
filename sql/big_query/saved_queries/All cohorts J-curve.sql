WITH join_dates as
(
	SELECT  
        address, 
        min(ds) as date_joined
	FROM analytics_eksportisto.daily_balances
    GROUP BY 1
),
address_daily_activity as (
    SELECT 
        A.address,
        A.date_joined,
        B.ds,
        B.daily_transactions
    FROM 
        join_dates A
        JOIN 
        analytics_eksportisto.daily_balances B
        ON A.address = B.address
),
by_address_info as (
    SELECT 
        address,
        DATE_TRUNC(date(date_joined), WEEK) as cohort,
        div(date_diff(date(ds), date(date_joined), DAY), 7) as weeks_since_joined,
        SUM(daily_transactions) as weekly_transactions,
    FROM address_daily_activity
    WHERE DATE_TRUNC(date(ds), WEEK) < DATE_TRUNC(CURRENT_DATE(), WEEK)
    GROUP BY 1, 2, 3
),
weekly_new_users as (
    SELECT 
        DATE_TRUNC(date(date_joined), WEEK) as cohort,
        COUNT(1) as addresses_count
    FROM join_dates
    GROUP BY 1
),
weekly_actives_by_cohort as (
SELECT 
    cohort,
    weeks_since_joined,
    COUNTIF(weekly_transactions > 0) as weekly_actives,
FROM by_address_info 
GROUP BY 1, 2
),
triangle_info as (
    SELECT
        B.cohort,
        B.weeks_since_joined,
        B.weekly_actives,
        A.addresses_count as weekly_new_users
    FROM weekly_new_users A
    JOIN weekly_actives_by_cohort B
    ON A.cohort = B.cohort
)
SELECT
    weeks_since_joined,
    SUM(weekly_actives),
    SUM(weekly_new_users),
    SUM(weekly_actives) * 1.0 / SUM(weekly_new_users) as pct_still_active
FROM triangle_info
GROUP BY 1
order by 1