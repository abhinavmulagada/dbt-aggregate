DELETE FROM `celo-testnet-production.blockscout_data.rpl_smart_contracts`
WHERE id IS NOT NULL;

INSERT INTO `celo-testnet-production.blockscout_data.rpl_smart_contracts` (id, name, compiler_version, optimization, address_hash, inserted_at, updated_at, optimization_runs, evm_version, verified_via_sourcify, partially_verified, is_vyper_contract, file_path)
SELECT
    id,
    name,
    compiler_version,
    optimization,
    address_hash,
    TIMESTAMP(inserted_at) as inserted_at,
    TIMESTAMP(updated_at) as updated_at,
    optimization_runs,
    evm_version,
    verified_via_sourcify,
    partially_verified,
    is_vyper_contract,
    file_path
FROM EXTERNAL_QUERY(
    'us-west1.blockscout-rc13',
    '''
    SELECT
        id,
        name,
        compiler_version,
        optimization,
        CONCAT('0x', ENCODE(address_hash, 'hex')) as address_hash,
        inserted_at,
        updated_at,
        optimization_runs,
        evm_version,
        verified_via_sourcify,
        partially_verified,
        is_vyper_contract,
        file_path
    FROM smart_contracts
    '''
) as t;