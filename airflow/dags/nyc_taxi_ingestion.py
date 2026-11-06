from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="nyc_taxi_ingestion",
    description="Download, load, and transform NYC Taxi data in Snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["nyc-taxi", "ingestion", "snowflake", "dbt"],
)
def nyc_taxi_ingestion():

    @task
    def run_ingestion():
        import subprocess

        result = subprocess.run(
            ["python", "/opt/airflow/scripts/run_ingestion.py"],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError("NYC Taxi ingestion failed")

        return "NYC Taxi ingestion completed successfully"

    @task
    def run_dbt_build():
        import subprocess

        result = subprocess.run(
            [
                "/home/airflow/dbt_venv/bin/dbt",
                "build",
                "--project-dir",
                "/opt/airflow/nyc_taxi_dbt",
                "--profiles-dir",
                "/home/airflow/.dbt",
                "--target-path",
                "/tmp/dbt_target",
                "--log-path",
                "/tmp/dbt_logs",
            ],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt build failed with exit code {result.returncode}"
            )

        return "dbt build completed successfully"

    ingestion_task = run_ingestion()
    dbt_task = run_dbt_build()

    ingestion_task >> dbt_task


nyc_taxi_ingestion()