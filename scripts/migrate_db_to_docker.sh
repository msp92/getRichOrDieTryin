#!/bin/bash

source .env.local
source .env.rds

PGSSLMODE=require

# Local DB environment
LOCAL_DB_USER="$DB_USER"
LOCAL_DB_NAME="$DB_NAME"
LOCAL_DB_HOST="$DB_HOST"  # or your Docker container hostname
LOCAL_DB_PORT="5432"

# RDS environment
RDS_USER="$RDS_USER"
RDS_PASSWORD="$RDS_PASSWORD"
RDS_HOST="$RDS_HOST"
RDS_DB="$RDS_DB"
RDS_PORT="5432"  # or custom port if different

# Step 1: Dump the local database
#echo "Dumping local database..."
#pg_dump --format=c --host="$LOCAL_DB_HOST" --port="$LOCAL_DB_PORT" --username="$LOCAL_DB_USER" "$LOCAL_DB_NAME" > local_backup.dump

# Step 2: Restore into RDS
echo "Restoring into RDS..."
PGPASSWORD="$RDS_PASSWORD" pg_restore --host="$RDS_HOST" --port="$RDS_PORT" --username="$RDS_USER" --dbname="$RDS_DB" --verbose --no-owner local_backup.dump

# Cleanup
#rm local_backup.dump
echo "Migration to RDS completed!"
