DELETE FROM `celo-testnet-production.blockscout_data.rpl_celo_params`
WHERE id IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_celo_params` (id, name, number_value, block_number, inserted_at, updated_at, address_value)
SELECT
    id,
    name,
    number_value,
    block_number,
    TIMESTAMP(inserted_at) as inserted_at,
    TIMESTAMP(updated_at) as updated_at,
    address_value
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13-replica-2',
    '''
    SELECT 
        id,
        name,
        number_value,
        block_number,
        inserted_at,
        updated_at,
        CONCAT('0x', ENCODE(address_value, 'hex')) as address_value
    FROM celo_params
    '''
);