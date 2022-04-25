WITH errors AS (
  SELECT * FROM celo-testnet-production.mobile_wallet_production.verification_error
  WHERE error = "transactionTimeout"  AND celo_network = "mainnet"
)

SELECT * FROM (
  -- marker is simply a visual field to make scanning the table with human eyes easier.
  SELECT anonymous_id, session_id, "***" as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM errors
  --UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.network_sync_lost
  --UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.network_sync_restored
  --UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.network_connected
  --UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.network_disconnected
  UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.transaction_receipt_received
  UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.transaction_start
  UNION ALL SELECT anonymous_id, session_id, "   " as marker, timestamp, event_text, tx_id, tx_hash FROM celo-testnet-production.mobile_wallet_production.transaction_hash_received
  UNION ALL (
    SELECT anonymous_id, NULL AS session_id, "  <" as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.application_backgrounded
    WHERE anonymous_id IN (SELECT anonymous_id FROM errors) AND timestamp > TIMESTAMP("2020-09-1")
  )
  UNION ALL (
    SELECT anonymous_id, NULL AS session_id, "  >" as marker, timestamp, event_text, NULL AS tx_id, NULL AS tx_hash FROM celo-testnet-production.mobile_wallet_production.application_opened
    WHERE anonymous_id IN (SELECT anonymous_id FROM errors) AND timestamp > TIMESTAMP("2020-09-1")
  )
)
WHERE (session_id IS NULL OR session_id IN (SELECT session_id FROM errors)) AND timestamp > TIMESTAMP("2020-09-1")
ORDER BY anonymous_id, timestamp DESC