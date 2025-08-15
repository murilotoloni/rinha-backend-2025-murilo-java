#!/bin/bash
set -e

echo "🧹 Limpando dados anteriores via purge..."

curl --location 'http://localhost:8002/admin/purge-payments' \
  --header 'X-Rinha-Token: 123' \
  --header 'Content-Type: application/json' \
  --data '{}'

curl --location 'http://localhost:8001/admin/purge-payments' \
  --header 'X-Rinha-Token: 123' \
  --header 'Content-Type: application/json' \
  --data '{}'

curl --location 'http://localhost:9999/payments-purge' \
  --header 'X-Rinha-Token: 123' \
  --header 'Content-Type: application/json' \
  --data '{}'

echo "✅ Purge concluído!"
