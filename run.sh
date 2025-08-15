#!/bin/bash
set -e

function cleanup {
  echo "🛑 Interrompido! Removendo containers..."
  docker compose down
  (
    cd ..
    cd fork/rinha-de-backend-2025/payment-processor
    docker compose down
  )
  exit 0
}

trap cleanup SIGINT

echo "🐳 Subindo containers do payment-processor..."
(

  cd ..
  cd fork/rinha-de-backend-2025/payment-processor
  docker compose up -d --build
)

echo "🐳 Subindo containers do diretório atual..."
docker compose up -d

until curl -s -o /dev/null -w "%{http_code}" http://localhost:9999/payments-summary | grep -q "200"; do
  sleep 1
done

echo "✅ payments-summary está no ar!"

  (
    k6 run -e MAX_REQUESTS=550 rinha.js
  )

cleanup