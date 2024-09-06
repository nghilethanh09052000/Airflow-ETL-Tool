#!/bin/bash


AUTH_DIR="/opt/airflow/authentication"


export $(cat $AUTH_DIR/.env | xargs)


gpg --pinentry-mode loopback --passphrase "$PASSPHRASE_HERE" \
    -o $AUTH_DIR/airflow-dev.json \
    -d $AUTH_DIR/airflow-dev.gpg


chmod 400 $AUTH_DIR/airflow-dev.json

export DBT_KEY_FILE="$AUTH_DIR/airflow-dev.json"

rm -rf $AUTH_DIR/.env
rm -rf $AUTH_DIR/airflow-dev.gpg
