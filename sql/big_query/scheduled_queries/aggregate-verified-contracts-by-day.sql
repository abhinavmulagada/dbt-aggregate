DELETE FROM `celo-testnet-production.blockscout_data.verified_contracts_by_day`
WHERE `date` IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.verified_contracts_by_day`
SELECT 
  CAST(verified_contracts AS INTEGER) AS number_of_contracts,
  `date`
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    WITH data AS (
        SELECT
            DISTINCT "date",
            SUM(verified_contracts) OVER (ORDER BY "date") verified_contracts
        FROM (
            SELECT
                DATE(inserted_at) AS date,
                COUNT(1) AS verified_contracts
            FROM smart_contracts
            GROUP BY DATE(inserted_at)
            ORDER BY DATE(inserted_at) ASC
        ) AS s1
    ) 
    SELECT (
        SELECT verified_contracts
        FROM data
        WHERE data."date" <= s."date"
        ORDER BY "date" DESC
        LIMIT 1
    ), 
    s."date"
    FROM (
        SELECT generate_series(min("date"), max("date"), '1 day')::date "date"
        FROM data
    ) s 
    ORDER BY "date" ASC;
    '''
);