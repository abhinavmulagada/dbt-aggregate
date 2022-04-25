WITH join_dates as (
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
    FROM join_dates A
    JOIN analytics_eksportisto.daily_balances B
    ON A.address = B.address
),
by_address_info as (
    SELECT 
        address,
        DATE_TRUNC(date(date_joined), DAY) as cohort,
        date_diff(date(ds), date(date_joined), DAY) as days_since_joined,
        SUM(daily_transactions) as daily_transactions,
    FROM address_daily_activity
    WHERE DATE_TRUNC(date(ds), DAY) < DATE_TRUNC(CURRENT_DATE(), DAY)
    GROUP BY 1, 2, 3
),
daily_new_users as (
    SELECT 
        DATE_TRUNC(date(date_joined), DAY) as cohort,
        COUNT(1) as addresses_count
    FROM join_dates
    GROUP BY 1
),
daily_actives_by_cohort as (
SELECT 
    cohort,
    days_since_joined,
    COUNTIF(daily_transactions > 0) as daily_actives,
FROM by_address_info 
GROUP BY 1, 2
),
triangle_info as (
    SELECT
        B.cohort,
        B.days_since_joined,
        B.daily_actives,
        A.addresses_count as daily_new_users
    FROM daily_new_users A
    JOIN daily_actives_by_cohort B
    ON A.cohort = B.cohort
)
SELECT
    days_since_joined,
    SUM(daily_actives) as users_still_active,
    SUM(daily_new_users) as users_in_vintage,
    SUM(daily_actives) * 1.0 / SUM(daily_new_users) as pct_still_active
FROM triangle_info
GROUP BY 1
order by 1