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

with models.DAG('ozon_dag',
                default_args=default_args,
                max_active_runs=1,
                max_active_tasks=1,
                catchup=False,
                schedule_interval='0 5 * * MON-FRI') as dag: # Time is according to GMT 0 !!!

    ozon_market = DockerOperator(
        task_id='ozon_market',
        image='holding_integrations_ozon',
        command=["bash", "-c", "set -a && source .env && python -u main_ozon_market.py"],
        api_version='auto',
        auto_remove=True,
        network_mode='bridge',
        mount_tmp_dir=False,
#        environment={
#            'GP_DB': "holding",
#    },
        mounts=[
            Mount(source="/usr/app/holding_integrations", target="/home/appuser", type="bind")
]
    )

ozon_market