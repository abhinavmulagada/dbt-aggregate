DELETE FROM `celo-testnet-production.blockscout_data.rpl_celo_validator`
WHERE id IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_celo_validator` (id, address, group_address_hash, signer_address_hash, score, member, inserted_at, updated_at)
SELECT
    id,
    address,
    group_address_hash,
    signer_address_hash,
    score,
    member,
    TIMESTAMP(inserted_at) as inserted_at,
    TIMESTAMP(updated_at) as updated_at
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13-replica-2',
    '''
    SELECT
        id,
        CONCAT('0x', ENCODE(address, 'hex')) as address,
        CONCAT('0x', ENCODE(group_address_hash, 'hex')) as group_address_hash,
        CONCAT('0x', ENCODE(signer_address_hash, 'hex')) as signer_address_hash,
        score::text,
        member,
        inserted_at,
        updated_at
    FROM celo_validator
    '''
);