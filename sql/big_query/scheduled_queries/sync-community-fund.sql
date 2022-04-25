DELETE FROM `celo-testnet-production.blockscout_data.dash_reserve_community_fund`
WHERE `date` >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY);

INSERT INTO `celo-testnet-production.blockscout_data.dash_reserve_community_fund` (`date`, `name`, `contract_address_hash`, `balance`)
SELECT
    s1.`date`,
    'CELO',
    LOWER('0x471EcE3750Da237f93B8E339c536989b8978a438'),
    IFNULL(s1.value, 0)
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT
        DISTINCT ON (
            t.token_contract_address_hash,
            DATE(b.timestamp)
        ) t.value,
        DATE(b.timestamp) as date
    FROM address_token_balances AS t
    LEFT JOIN blocks AS b
    ON b.number = t.block_number
    WHERE address_hash = '\\xD533Ca259b330c7A88f74E000a3FaEa2d63B7972'
      AND token_contract_address_hash = '\\x471EcE3750Da237f93B8E339c536989b8978a438'
      AND b.timestamp >= (current_date::timestamp - INTERVAL '1 day')
    ORDER BY
        t.token_contract_address_hash,
        DATE(b.timestamp),
        b.timestamp DESC
    '''
) s1;