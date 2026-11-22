from databricks.sdk import WorkspaceClient
import os
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

ws = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"]
)

job_trigger = ws.jobs.run_now(job_id=int(os.environ["DATABRICKS_JOB_ID"]))

while True:
    job_status = ws.jobs.get_run(run_id=job_trigger.run_id)
    if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_status.state.result_state == RunResultState.SUCCESS:
            print("Job completed successfully!")
            break
        else:
            print(f"Job failed with state: {job_status.state.life_cycle_state}")
    time.sleep(5)