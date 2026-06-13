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
package_name = "iris"
conf_source = "" or Path.cwd() / "conf"

DOCKER_IMAGE = "ruairiodonohoe/iris:latest"
mount = Mount(
    source="/home/ruairi/Documents/data/iris",  # The full path on your laptop
    target="/home/kedro_docker/data",  # The path inside the container
    type="bind",
)


# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(
    dag_id="iris",
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
        retries=1,
        retry_delay=timedelta(minutes=5),
    ),
)
def iris():
    tasks = {
        "split": DockerOperator(
            task_id="split",
            command="kedro run --nodes split",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "train": DockerOperator(
            task_id="train",
            command="kedro run --nodes train",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "predict": DockerOperator(
            task_id="predict",
            command="kedro run --nodes predict",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "report": DockerOperator(
            task_id="report",
            command="kedro run --nodes report",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
    }
    tasks["split"] >> tasks["train"]
    tasks["train"] >> tasks["predict"]
    tasks["split"] >> tasks["predict"]
    tasks["split"] >> tasks["report"]
    tasks["predict"] >> tasks["report"]


iris()
