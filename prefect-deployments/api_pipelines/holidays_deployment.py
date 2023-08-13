from holidays import run_pipeline
from prefect.deployments import Deployment
from prefect.filesystems import GCS

storage = GCS.load("code-storage")

deployment = Deployment.build_from_flow(
    flow=run_pipeline,
    name="Public Holidays", 
    version=1, 
    work_queue_name="main",
    storage=storage,
    path='daily_apis/'
)
deployment.apply()