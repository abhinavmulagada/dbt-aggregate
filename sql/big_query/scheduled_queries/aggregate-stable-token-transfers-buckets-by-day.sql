DELETE FROM `celo-testnet-production.blockscout_data.dash_stable_token_transfers_buckets`
WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY);

INSERT INTO `celo-testnet-production.blockscout_data.dash_stable_token_transfers_buckets` (`date`, `name`, `contract_address_hash`, `bucket`, `avg_amount`, `number_of_transfers`)
SELECT
    s1.`date`,
    c.name,
    s1.contract_address_hash,
    s1.bucket,
    s1.avg_amount,
    s1.number_of_transfers
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT
        MAX(date) AS date,
        CONCAT('0x', ENCODE(token_contract_address_hash, 'hex')) as contract_address_hash,
        bucket,
        COUNT(1) as number_of_transfers,
        AVG(amount) as avg_amount
    FROM (
        select
            DATE(b.timestamp) as date,
            t.token_contract_address_hash,
            amount,
            case
                when amount < 10000000000000000000 then 0
                when amount >= 10000000000000000000 and amount < 100000000000000000000 then 1
                when amount >= 100000000000000000000 and amount < 1000000000000000000000 then 2
                when amount >= 1000000000000000000000 and amount < 10000000000000000000000 then 3
                else 4
            end as bucket
        from blocks b
        LEFT JOIN token_transfers t
        ON b.number = t.block_number
        WHERE b.timestamp >= (current_date::timestamp - INTERVAL '1 day')
          AND b.timestamp < current_date::timestamp
          AND token_contract_address_hash IN (
              '\\x765de816845861e75a25fca122bb6898b8b1282a',
              '\\xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
              '\\xe8537a3d056da446677b9e9d6c5db704eaab4787')
            ) s1
        GROUP BY date, token_contract_address_hash, bucket
        ORDER BY date, bucket;
    '''
) s1
LEFT JOIN `celo-testnet-production.blockscout_data.celo_stable_tokens` c
ON s1.contract_address_hash = c.contract_address_hash;