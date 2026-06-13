from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DOCKER_IMAGE = "ruairiodonohoe/spaceflights:latest"
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
    tasks = {
        "create-confusion-matrix-adb01ae6": DockerOperator(
            task_id="create-confusion-matrix-adb01ae6",
            command="kedro run --nodes create_confusion_matrix__adb01ae6",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "preprocess-companies-node": DockerOperator(
            task_id="preprocess-companies-node",
            command="kedro run --nodes preprocess_companies_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "preprocess-shuttles-node": DockerOperator(
            task_id="preprocess-shuttles-node",
            command="kedro run --nodes preprocess_shuttles_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "compare-passenger-capacity-exp-43c60170": DockerOperator(
            task_id="compare-passenger-capacity-exp-43c60170",
            command="kedro run --nodes compare_passenger_capacity_exp__43c60170",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "compare-passenger-capacity-go-738187f2": DockerOperator(
            task_id="compare-passenger-capacity-go-738187f2",
            command="kedro run --nodes compare_passenger_capacity_go__738187f2",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "create-model-input-table-node": DockerOperator(
            task_id="create-model-input-table-node",
            command="kedro run --nodes create_model_input_table_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-split-data-node": DockerOperator(
            task_id="active-modelling-pipeline-split-data-node",
            command="kedro run --nodes active_modelling_pipeline.split_data_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-split-data-node": DockerOperator(
            task_id="candidate-modelling-pipeline-split-data-node",
            command="kedro run --nodes candidate_modelling_pipeline.split_data_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-train-model-node": DockerOperator(
            task_id="active-modelling-pipeline-train-model-node",
            command="kedro run --nodes active_modelling_pipeline.train_model_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-train-model-node": DockerOperator(
            task_id="candidate-modelling-pipeline-train-model-node",
            command="kedro run --nodes candidate_modelling_pipeline.train_model_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "active-modelling-pipeline-evaluate-model-node": DockerOperator(
            task_id="active-modelling-pipeline-evaluate-model-node",
            command="kedro run --nodes active_modelling_pipeline.evaluate_model_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
        "candidate-modelling-pipeline-evaluate-model-node": DockerOperator(
            task_id="candidate-modelling-pipeline-evaluate-model-node",
            command="kedro run --nodes candidate_modelling_pipeline.evaluate_model_node",
            image=DOCKER_IMAGE,
            network_mode="bridge",
            mounts=[mount],
        ),
    }
    (
        tasks["preprocess-shuttles-node"]
        >> tasks["compare-passenger-capacity-exp-43c60170"]
    )
    tasks["preprocess-shuttles-node"] >> tasks["compare-passenger-capacity-go-738187f2"]
    tasks["preprocess-companies-node"] >> tasks["create-model-input-table-node"]
    tasks["preprocess-shuttles-node"] >> tasks["create-model-input-table-node"]
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
        tasks["active-modelling-pipeline-split-data-node"]
        >> tasks["active-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["active-modelling-pipeline-train-model-node"]
        >> tasks["active-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["candidate-modelling-pipeline-train-model-node"]
        >> tasks["candidate-modelling-pipeline-evaluate-model-node"]
    )
    (
        tasks["candidate-modelling-pipeline-split-data-node"]
        >> tasks["candidate-modelling-pipeline-evaluate-model-node"]
    )


spaceflights_tasks()
