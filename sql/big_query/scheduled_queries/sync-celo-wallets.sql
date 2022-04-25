DELETE FROM `celo-testnet-production.blockscout_data.rpl_celo_wallets` 
WHERE id IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_celo_wallets` ( id, wallet_address_hash, account_address_hash, block_number, inserted_at, updated_at ) 
SELECT 
    id,
    wallet_address_hash,
    account_address_hash,
    block_number,
    TIMESTAMP(inserted_at) as inserted_at,
    TIMESTAMP(updated_at) as updated_at,
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13-replica-2',
    '''
    SELECT
        id,
        CONCAT('0x', ENCODE(wallet_address_hash, 'hex')) as wallet_address_hash,
        CONCAT('0x', ENCODE(account_address_hash, 'hex')) as account_address_hash,
        block_number,
        inserted_at,
        updated_at
    FROM celo_wallets
    ''' );