#!/bin/bash
set -e

YSQL_HOST="${YSQL_HOST:-yugabytedb}"
YSQL_PORT="${YSQL_PORT:-5433}"
TEST_TFM="${TEST_TFM:-net8.0}"
CONFIG="${CONFIG:-Release}"

echo "============================================"
echo " Npgsql Test Runner against YugabyteDB"
echo "============================================"
echo ""

echo "Waiting for YugabyteDB YSQL to be ready..."
MAX_RETRIES=60
RETRY_COUNT=0
until pg_isready -h "$YSQL_HOST" -p "$YSQL_PORT" -U yugabyte -q 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: YugabyteDB did not become ready after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "  Attempt $RETRY_COUNT/$MAX_RETRIES - waiting 5s..."
    sleep 5
done
echo "YugabyteDB YSQL is ready."
echo ""

echo "Running database initialization..."
bash /workspace/docker/yugabyte/init-yugabyte.sh
echo ""

echo "Building solution..."
dotnet build -c "$CONFIG"
echo ""

echo "============================================"
echo " Running Npgsql.Tests"
echo "============================================"
dotnet test -c "$CONFIG" -f "$TEST_TFM" test/Npgsql.Tests --logger "console;verbosity=detailed" || true
echo ""

echo "============================================"
echo " Running Npgsql.DependencyInjection.Tests"
echo "============================================"
dotnet test -c "$CONFIG" -f "$TEST_TFM" test/Npgsql.DependencyInjection.Tests --logger "console;verbosity=detailed" || true
echo ""

echo "============================================"
echo " Running Npgsql.PluginTests"
echo "============================================"
dotnet test -c "$CONFIG" -f "$TEST_TFM" test/Npgsql.PluginTests --logger "console;verbosity=detailed" || true
echo ""

echo "============================================"
echo " All test runs complete."
echo "============================================"
