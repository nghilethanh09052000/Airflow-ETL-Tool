FROM apache/airflow:2.9.1-python3.11

LABEL maintainer="Nghi Le Thanh (Data)"

RUN mkdir -p /opt/airflow/authentication
COPY airflow-dev.gpg /opt/airflow/authentication/
COPY .env /opt/airflow/authentication/
COPY airflow_init.sh /opt/airflow/init/airflow_init.sh


USER root
RUN chmod +x /opt/airflow/init/airflow_init.sh

USER airflow
RUN /opt/airflow/init/airflow_init.sh
COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

EXPOSE 8080 5555 8888
