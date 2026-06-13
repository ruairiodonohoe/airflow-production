from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "spaceflights"
conf_source = "" or Path.cwd() / "conf"

mount = Mount(
    source="/home/ruairi/Documents/data",  # The full path on your laptop
    target="/home/kedro_docker/data",  # The path inside the container
    type="bind",
)


# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(
    dag_id="spaceflights_tasks",
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
def spaceflights_tasks():
    DOCKER_IMAGE = "ruairiodonohoe/spaceflights:latest"

    tasks = {
        "create-confusion-matrix-adb01ae6": DockerOperator(
            task_id="create-confusion-matrix-adb01ae6",
            image=DOCKER_IMAGE,
            command="kedro run --node create-confusion-matrix",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "preprocess-companies-node": DockerOperator(
            task_id="preprocess-companies-node",
            image=DOCKER_IMAGE,
            command="kedro run --node preprocess-companies",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "preprocess-shuttles-node": DockerOperator(
            task_id="preprocess-shuttles-node",
            image=DOCKER_IMAGE,
            command="kedro run --node preprocess-shuttles",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "compare-passenger-capacity-exp-43c60170": DockerOperator(
            task_id="compare-passenger-capacity-exp-43c60170",
            image=DOCKER_IMAGE,
            command="kedro run --node compare-passenger-capacity-exp",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "compare-passenger-capacity-go-738187f2": DockerOperator(
            task_id="compare-passenger-capacity-go-738187f2",
            image=DOCKER_IMAGE,
            command="kedro run --node compare-passenger-capacity-go",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "create-model-input-table-node": DockerOperator(
            task_id="create-model-input-table-node",
            image=DOCKER_IMAGE,
            command="kedro run --node create-model-input-table",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-split-data-node": DockerOperator(
            task_id="active-modelling-pipeline-split-data-node",
            image=DOCKER_IMAGE,
            command="kedro run --node active-modelling-pipeline-split-data",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-split-data-node": DockerOperator(
            task_id="candidate-modelling-pipeline-split-data-node",
            image=DOCKER_IMAGE,
            command="kedro run --node candidate-modelling-pipeline-split-data",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-train-model-node": DockerOperator(
            task_id="active-modelling-pipeline-train-model-node",
            image=DOCKER_IMAGE,
            command="kedro run --node active-modelling-pipeline-train-model",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-train-model-node": DockerOperator(
            task_id="candidate-modelling-pipeline-train-model-node",
            image=DOCKER_IMAGE,
            command="kedro run --node candidate-modelling-pipeline-train-model",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-evaluate-model-node": DockerOperator(
            task_id="active-modelling-pipeline-evaluate-model-node",
            image=DOCKER_IMAGE,
            command="kedro run --node active-modelling-pipeline-evaluate-model",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-evaluate-model-node": DockerOperator(
            task_id="candidate-modelling-pipeline-evaluate-model-node",
            image=DOCKER_IMAGE,
            command="kedro run --node candidate-modelling-pipeline-evaluate-model",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
            mounts=[mount],
        ),
    }

    (
        tasks["preprocess-shuttles-node"]
        >> tasks["compare-passenger-capacity-exp-43c60170"]
    )
    tasks["preprocess-shuttles-node"] >> tasks["compare-passenger-capacity-go-738187f2"]
    tasks["preprocess-shuttles-node"] >> tasks["create-model-input-table-node"]
    tasks["preprocess-companies-node"] >> tasks["create-model-input-table-node"]
    (
        tasks["create-model-input-table-node"]
        >> tasks["active-modelling-pipeline-split-data-node"]
    )
    (
        tasks["create-model-input-table-node"]
        >> tasks["candidate-modelling-pipeline-split-data-node"]
    )
    (
        tasks["active-modelling-pipeline-split-data-node"]
        >> tasks["active-modelling-pipeline-train-model-node"]
    )
    (
        tasks["candidate-modelling-pipeline-split-data-node"]
        >> tasks["candidate-modelling-pipeline-train-model-node"]
    )
    (
        tasks["active-modelling-pipeline-train-model-node"]
        >> tasks["active-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["active-modelling-pipeline-split-data-node"]
        >> tasks["active-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["candidate-modelling-pipeline-split-data-node"]
        >> tasks["candidate-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["candidate-modelling-pipeline-train-model-node"]
        >> tasks["candidate-modelling-pipeline-evaluate-model-node"]
    )
