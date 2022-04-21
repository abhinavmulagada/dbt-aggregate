-- currently not in use

{{
    config(
        materialized='incremental',
        unique_key='date'
    )
}}

SELECT count(*) attestation_request_count, 
       date(timestamp) as date
FROM `celo-testnet-production.analytics_eksportisto.attestation_requests` 


{% if is_incremental() %}
  where `date` >= (select max(`date`) 
                   FROM `celo-testnet-production.analytics_eksportisto.attestation_requests` 

{% endif %}

group by date