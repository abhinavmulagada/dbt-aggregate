SELECT 
    CASE
        WHEN not has_verified_phone_number 
            then 
                case
                    when address < '0x8000000000000000000000000000000000000000' then '0. test_group'
                    else '1. control_group'
                end
        else 
        case 
            WHEN abtest_address <'0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000' then '0. test_group'
            WHEN abtest_address >= '0x8000000000000000000000000000000000000000' and address >= '0x8000000000000000000000000000000000000000' then '1. control_group'
            WHEN abtest_address >= '0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000' then 'no_in_app_but_earnings'
            else 'in_app_but_no_earnings'
        end
    END as test_group,
    -- CASE
    --     WHEN pre_experiment_balance IS NULL THEN '0. New users'
    --     WHEN pre_experiment_balance = 0 THEN '1. No pre_experiment_balance'
    --     WHEN pre_experiment_balance <= 0.01 THEN '2. Less than 1c'
    --     WHEN pre_experiment_balance <= 1 THEN '3. Less than 1cUSD'
    --     WHEN pre_experiment_balance <= 10 THEN '4. Less than 10 cUSD'
    --     WHEN pre_experiment_balance <= 100 THEN '5. Less than 100 cUSD'
    --     ELSE 'More than 100cUSD'
    -- END as pre_experiment_balance,
    COUNT(1) as num_samples,
    COUNTIF(is_new_user) as new_users_count,
    COUNTIF(is_new_user and has_verified_phone_number) as verified_new_users_count,
    COUNTIF(has_verified_phone_number) as verified_users_count,
    COUNTIF(has_verified_phone_number and average_balance_post_experiment >= 10) as verified_users_with_more_than_10cusd,
    COUNTIF(has_verified_phone_number and average_balance_post_experiment >= 1) as verified_users_with_more_than_1cusd,
    COUNTIF(has_verified_phone_number and average_balance_post_experiment > 0) as verified_users_with_positive_balance,
    COUNTIF(average_balance_post_experiment > 0) as users_with_positive_balance,
    avg(average_balance_post_experiment) as avg_balance,
    max(average_balance_post_experiment) as max_balance,
    avg(lness_during_test) as avg_lness
    -- avg(cusd_balance) + 1.96 * (SELECT stddev(cusd_balance) from full_address_details) / Sqrt(count(1)) as upper_CI,
    -- avg(cusd_balance) - 1.96 * (SELECT stddev(cusd_balance) from full_address_details) / Sqrt(count(1)) as lower_CI
FROM `celo-testnet-production.analytics.savings_experiment_results`
-- WHERE is_new_user
-- WHERE has_verified_phone_number 
-- WHERE ((abtest_address <'0x8000000000000000000000000000000000000000' and address < '0x8000000000000000000000000000000000000000') OR (abtest_address >= '0x8000000000000000000000000000000000000000' and address >= '0x8000000000000000000000000000000000000000'))
GROUP BY 1
