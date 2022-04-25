DELETE FROM `celo-testnet-production.blockscout_data.rpl_tokens`
WHERE inserted_at IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_tokens` (
    name,
    symbol,
    total_supply,
    decimals,
    type,
    cataloged,
    contract_address_hash,
    inserted_at,
    updated_at,
    holder_count,
    bridged,
    skip_metadata
)
SELECT
    name,
    symbol,
    total_supply,
    decimals,
    type,
    cataloged,
    contract_address_hash,
    TIMESTAMP(inserted_at) as inserted_at,
    TIMESTAMP(updated_at) as updated_at,
    holder_count,
    bridged,
    skip_metadata
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13-replica-2',
    '''
    SELECT
        name,
        symbol,
        total_supply::text,
        decimals::text,
        type,
        cataloged,
        CONCAT('0x', ENCODE(contract_address_hash, 'hex')) as contract_address_hash,
        inserted_at,
        updated_at,
        holder_count,
        bridged,
        skip_metadata
    FROM tokens
    '''
);