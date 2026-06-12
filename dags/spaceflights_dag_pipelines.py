from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "spaceflights"
conf_source = "" or Path.cwd() / "conf"


# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(
    dag_id="spaceflights_pipelines",
    start_date=datetime(2023, 1, 1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule="@once",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        # retries=1,
        # retry_delay=timedelta(minutes=5),
    ),
)
def spaceflights_pipelines():
    DOCKER_IMAGE = "ruairiodonohoe/spaceflights:latest"

    data_processing = DockerOperator(
        image=DOCKER_IMAGE,
        task_id="run_project",
        command="kedro run --pipeline data_processing",
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/home/ruairi/Documents/data",  # The full path on your laptop
                target="/home/kedro_docker/data",  # The path inside the container
                type="bind",
            )
        ],
    )

    data_science = DockerOperator(
        image=DOCKER_IMAGE,
        task_id="run_project",
        command="kedro run --pipeline data_science",
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/home/ruairi/Documents/data",  # The full path on your laptop
                target="/home/kedro_docker/data",  # The path inside the container
                type="bind",
            )
        ],
    )

    data_processing >> data_science


spaceflights_pipelines()
