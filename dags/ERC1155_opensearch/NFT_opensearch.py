import datetime
from datetime import date

from airflow import DAG
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

from utils.alerting.airflow import airflow_callback

GCE_INSTANCE = "sipherian-system-hybrid"
GCE_ZONE = "us-central1-a"
GCP_PROJECT_ID = "sipher-data-platform"

run_date = date.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "tri.nguyen",
    "start_date": datetime.datetime(2022, 9, 7),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

with DAG(dag_id="NFT_opensearch", default_args=default_args,) as dag:

    ssh_pull_opensearch_data = SSHOperator(
        task_id="ssh_pull_opensearch_data",
        ssh_hook=ComputeEngineSSHHook(
            instance_name=GCE_INSTANCE,
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            use_oslogin=True,
            use_iap_tunnel=False,
            use_internal_ip=True,
            gcp_conn_id="sipher_gcp_vm",
        ),
        command=f"""
    cd .. \
    && cd gcp-vm_django \
    && python3 test.py \
    """,
        dag=dag,
    )

    ssh_upload_data_to_gcs = SSHOperator(
        task_id="ssh_upload_data_to_gcs",
        ssh_hook=ComputeEngineSSHHook(
            instance_name=GCE_INSTANCE,
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            use_oslogin=True,
            use_iap_tunnel=False,
            use_internal_ip=True,
            gcp_conn_id="sipher_gcp_vm",
        ),
        command=f"""
    cd .. \
    && cd gcp-vm_django \
    && gsutil cp INU.csv gs://opensearch-onchain-data/INU/dt={run_date}/ \
    && gsutil cp NEKO.csv gs://opensearch-onchain-data/NEKO/dt={run_date}/ \
    && gsutil cp Lootbox.csv gs://opensearch-onchain-data/Lootbox/dt={run_date}/ \
    && gsutil cp Spaceship.csv gs://opensearch-onchain-data/Spaceship/dt={run_date}/ \
    && gsutil cp Spaceship_Parts.csv gs://opensearch-onchain-data/Spaceship_Parts/dt={run_date}/ \
    && gsutil cp Sculpture.csv gs://opensearch-onchain-data/Sculpture/dt={run_date}/ \
    """,
        dag=dag,
    )

    ssh_pull_opensearch_data >> ssh_upload_data_to_gcs
