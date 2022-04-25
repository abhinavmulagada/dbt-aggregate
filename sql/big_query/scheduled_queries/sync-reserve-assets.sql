DELETE FROM `celo-testnet-production.blockscout_data.dash_reserve_assets`
WHERE date >= CURRENT_DATE();

INSERT INTO `celo-testnet-production.blockscout_data.dash_reserve_assets` (`date`, `symbol`, `amount`)
SELECT
    CURRENT_DATE('UTC') as date,
    'CELO' as symbol,
    s1.amount
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT SUM(value) as amount
    FROM address_current_token_balances
    WHERE token_contract_address_hash = '\\x471EcE3750Da237f93B8E339c536989b8978a438'
      AND address_hash IN (
          '\\x9380fA34Fd9e4Fd14c06305fd7B6199089eD4eb9',
          '\\x246f4599eFD3fA67AC44335Ed5e749E518Ffd8bB',
          '\\x298FbD6dad2Fc2cB56d7E37d8aCad8Bf07324f67'
        );
    '''
) s1

UNION ALL

SELECT
    CURRENT_DATE('UTC') as date,
    'cMCO2' as symbol,
    s2.amount
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT SUM(value) as amount
    FROM address_current_token_balances
    WHERE token_contract_address_hash = '\\x32A9FE697a32135BFd313a6Ac28792DaE4D9979d'
      AND address_hash IN ('\\x298FbD6dad2Fc2cB56d7E37d8aCad8Bf07324f67');
    '''
) s2

UNION ALL

-- by default we think that other crypto assets don't change, another pipeline would replace default values
SELECT
    CURRENT_DATE() as `date`,
    symbol,
    amount
FROM `celo-testnet-production.blockscout_data.dash_reserve_assets`
WHERE symbol IN ('ETH', 'BTC', 'DAI') 
  AND `date` = DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -1 DAY);