# deployment.py

from web_to_gcs_github_block import etl_web_to_gcs
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block_storage = GitHub.load("web-to-gcs")

deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="web-to-gcs-with-github-block-storage",
    parameters={"color": "green", "year": 2020, "month": 11},
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="default",
    storage=github_block_storage,
)

if __name__ == "__main__":
    deployment.apply()