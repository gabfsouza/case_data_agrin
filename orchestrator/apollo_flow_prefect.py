from prefect import flow, task
import subprocess
from prefect.server.schemas.schedules import CronSchedule

gold_path = "gold/modeling_indicators.py"
silver_path = "silver/services/apollo_silver_processor.py"
bronze_path = "bronze/insert_s3.py"

@task(
    name="Executar job Spark dentro do container",
    retries=1,
    retry_delay_seconds=15,
)
def run_pipeline_job(path: str):
    """
    Executa um job Spark (Bronze, Silver ou Gold)
    dentro do container Spark
    """
    command = [
        "docker",
        "exec",
        "spark",
        "python3",
        f"/home/hadoop/workspace/{path}",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Job failed ({path}):\n{result.stderr}"
        )

    print(result.stdout)


@flow(name="bronze-silver-gold-pipeline")
def pipeline_flow(execution_date: str | None = None):
    # Bronze
    run_pipeline_job(bronze_path)
    # Silver
    run_pipeline_job(silver_path)
    # Gold
    run_pipeline_job(gold_path)


if __name__ == "__main__":
    pipeline_flow.serve(
        name="local-bronze-silver-gold",
        tags=["spark", "datalake", "case"],
        schedule={
            "cron": "0 6 * * *",
            "timezone": "America/Sao_Paulo"
        }
    )
