from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime
from docker.types import Mount
from airflow import models

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,8,30),
    'retries': 0,
}

with models.DAG('yandex_dag',
                default_args=default_args,
                max_active_runs=1,
                max_active_tasks=1,
                schedule_interval='0 5 * * MON-FRI') as dag: # Time is according to GMT 0 !!!

    yandex_market = DockerOperator(
        task_id='yandex_market',
        image='holding_integrations_yandex',
        command=["bash", "-c", "set -a && source .env && python -u main_yandex_market.py"],
        api_version='auto',
        auto_remove=True,
        network_mode='bridge',
        mount_tmp_dir=False,
  #      environment={
  #          'YM_BUSINESS_ID': "860724",
  #  },
        mounts=[
            Mount(source="/usr/app/holding_integrations", target="/home/appuser", type="bind")
]
    )

yandex_market