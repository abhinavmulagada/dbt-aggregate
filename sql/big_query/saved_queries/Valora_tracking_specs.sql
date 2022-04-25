WITH
    event_details AS (
        SELECT
            event_name,
            event_parameters.key,
            platform,
            CASE
                WHEN event_parameters.value.string_value IS NOT NULL THEN "string_value"
                WHEN event_parameters.value.int_value IS NOT NULL THEN "int_value"
                WHEN event_parameters.value.float_value IS NOT NULL THEN "float_value"
                WHEN event_parameters.value.double_value IS NOT NULL THEN "double_value"
                ELSE NULL
            END AS Type,
            MIN(app_info.version) minimumVersion,
            MAX(app_info.version) maximumVersion
        FROM `celo-mobile-mainnet.analytics_230361027.events_20*`, unnest(event_params) as event_parameters
        WHERE PARSE_DATE('%y%m%d', _TABLE_SUFFIX) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
          and NOT REGEXP_CONTAINS(app_info.version, r'.*debug.*')
        GROUP BY 1, 2, 3, 4
        ORDER BY 3
),
event_screen AS (
    SELECT
        event_name,
        CASE
            WHEN event_parameters.key = 'firebase_screen' THEN event_parameters.value.string_value
            ELSE NULL
        END as screen
    FROM `celo-mobile-mainnet.analytics_230361027.events_20*`, unnest(event_params) as event_parameters
    WHERE event_parameters.key = 'firebase_screen'
      AND PARSE_DATE('%y%m%d', _TABLE_SUFFIX) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    GROUP BY 1, 2
)
SELECT
    event_details.*,
    event_screen.screen
FROM event_details
LEFT JOIN event_screen
ON event_details.event_name = event_screen.event_name