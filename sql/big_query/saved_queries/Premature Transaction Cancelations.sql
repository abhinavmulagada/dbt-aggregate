WITH verification_errors AS (
  SELECT event, error, timestamp, session_id, user_address
  FROM celo-testnet-production.mobile_wallet_production.verification_error
  WHERE error = "transactionTimeout"
)
SELECT error.*, receipt.timestamp AS receipt_timestamp, TIMESTAMP_DIFF(receipt.timestamp, start.timestamp, SECOND) AS delta FROM (
  SELECT * FROM (
    SELECT *, LAST_VALUE(tx_id) OVER (PARTITION BY session_id ORDER BY timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS foo
    FROM (
      SELECT *, NULL as tx_id 
      FROM verification_errors
      UNION ALL
      SELECT event, NULL AS error, timestamp, session_id, user_address, tx_id
      FROM celo-testnet-production.mobile_wallet_production.transaction_start
      WHERE session_id IN (SELECT session_id FROM verification_errors)
        AND context_app_version > "1.1.0"
      ORDER BY timestamp DESC
    )
  )
  WHERE event = "verification_error" AND foo IS NOT NULL
) AS error
JOIN celo-testnet-production.mobile_wallet_production.transaction_receipt_received AS receipt
ON error.foo = receipt.tx_id
JOIN celo-testnet-production.mobile_wallet_production.transaction_start AS start
ON error.foo = start.tx_id
ORDER BY delta DESC