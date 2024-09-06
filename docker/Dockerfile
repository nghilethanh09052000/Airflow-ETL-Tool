FROM apache/airflow:2.9.1-python3.11

LABEL maintainer="Nghi Le Thanh (Data)"

RUN mkdir -p /opt/airflow/authentication
COPY airflow-dev.gpg /opt/airflow/authentication/
COPY .env /opt/airflow/authentication/

RUN export $(cat /opt/airflow/authentication/.env | xargs) && \
    gpg --pinentry-mode loopback --passphrase "$PASSPHRASE_HERE" \
    -o /opt/airflow/authentication/airflow-dev.json \
    -d /opt/airflow/authentication/airflow-dev.gpg && \
    rm -rf /opt/airflow/authentication/airflow-dev.gpg && \
    rm -rf /opt/airflow/authentication/.env


USER root
RUN chmod 400 /opt/airflow/authentication/airflow-dev.json

USER airflow

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

EXPOSE 8080 5555 8888
