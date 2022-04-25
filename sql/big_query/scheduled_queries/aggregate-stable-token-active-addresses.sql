INSERT INTO `celo-testnet-production.blockscout_data.dash_stable_token_active_addresses` (`date`, `name`, `group`, `number_of_addresses`)
WITH last_update_ts AS (
    SELECT MAX(`date`)
    FROM `celo-testnet-production.blockscout_data.dash_stable_token_active_addresses`
)
SELECT
    `date`,
    s.name,
    CASE
        WHEN DATE(a.inserted_at) = `date` THEN 'new'
        ELSE 'returning'
    END AS `group`,
    COUNT(1) as number_of_addresses
FROM (
    SELECT
        `date`,
        `address_hash`,
        `token_contract_address_hash`
    FROM (
        SELECT
            DATE(b.timestamp) AS `date`,
            t.to_address_hash AS address_hash,
            t.token_contract_address_hash
        FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` AS t
        INNER JOIN `celo-testnet-production.blockscout_data.rpl_blocks` AS b
        ON t.block_number = b.number
        WHERE t.token_contract_address_hash IN (
            '0x765de816845861e75a25fca122bb6898b8b1282a',
            '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
            '0xe8537a3d056da446677b9e9d6c5db704eaab4787'
        )
        AND DATE(b.timestamp) > (
            SELECT *
            FROM last_update_ts
        )
        AND DATE(b.timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT
            DATE(b.timestamp) AS `date`,
            t.from_address_hash AS address_hash,
            t.token_contract_address_hash
        FROM `celo-testnet-production.blockscout_data.rpl_token_transfers` AS t
        INNER JOIN `celo-testnet-production.blockscout_data.rpl_blocks` AS b
        ON t.block_number = b.number
        WHERE t.token_contract_address_hash IN (
            '0x765de816845861e75a25fca122bb6898b8b1282a',
            '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73',
            '0xe8537a3d056da446677b9e9d6c5db704eaab4787'
        )
        AND DATE(b.timestamp) > (
            SELECT *
            FROM last_update_ts
        ) AND DATE(b.timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2, 3
    ) AS s1
    GROUP BY 1, 2, 3
) s2

INNER JOIN `celo-testnet-production.blockscout_data.rpl_addresses` AS a
ON a.hash = s2.address_hash

LEFT JOIN `celo-testnet-production.blockscout_data.celo_stable_tokens` AS s
ON s.contract_address_hash = s2.token_contract_address_hash

GROUP BY 1, 2, 3
ORDER BY 1