#!/bin/bash
set -e

YSQL_HOST="${YSQL_HOST:-yugabytedb}"
YSQL_PORT="${YSQL_PORT:-5433}"
YSQL_USER="${YSQL_USER:-yugabyte}"

PSQL="psql -h $YSQL_HOST -p $YSQL_PORT -U $YSQL_USER -d yugabyte"

echo "=== Initializing YugabyteDB for Npgsql tests ==="

echo "Creating test users..."
$PSQL -c "CREATE USER npgsql_tests SUPERUSER PASSWORD 'npgsql_tests';" || echo "User npgsql_tests may already exist"
$PSQL -c "CREATE USER npgsql_tests_ssl SUPERUSER PASSWORD 'npgsql_tests_ssl';" || echo "User npgsql_tests_ssl may already exist"
$PSQL -c "CREATE USER npgsql_tests_nossl SUPERUSER PASSWORD 'npgsql_tests_nossl';" || echo "User npgsql_tests_nossl may already exist"
$PSQL -c "CREATE USER npgsql_tests_scram SUPERUSER PASSWORD 'npgsql_tests_scram';" || echo "User npgsql_tests_scram may already exist"

echo "Creating test database..."
$PSQL -c "CREATE DATABASE npgsql_tests OWNER npgsql_tests;" || echo "Database npgsql_tests may already exist"

PSQL_TESTDB="psql -h $YSQL_HOST -p $YSQL_PORT -U npgsql_tests -d npgsql_tests"

echo "Installing extensions on npgsql_tests database..."
$PSQL_TESTDB -c "CREATE EXTENSION IF NOT EXISTS hstore;" || echo "hstore extension not available"
$PSQL_TESTDB -c "CREATE EXTENSION IF NOT EXISTS citext;" || echo "citext extension not available"
$PSQL_TESTDB -c "CREATE EXTENSION IF NOT EXISTS ltree;" || echo "ltree extension not available"

echo "=== YugabyteDB initialization complete ==="
echo ""
echo "Verifying setup..."
$PSQL -c "\du npgsql_tests*"
$PSQL -c "\l npgsql_tests"
$PSQL_TESTDB -c "\dx"
