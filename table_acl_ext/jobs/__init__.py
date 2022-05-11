import time

from multiprocessing.pool import ThreadPool
from typing import List, Dict

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config
from databricks_cli.sdk.service import JobsService
from decouple import config

_synapse_unload_job_id = config("SYNAPSE_UNLOAD_JOB_ID",
                                cast=str)


def is_job_done(job_run_info):
    return job_run_info["state"]["life_cycle_state"] == "TERMINATED"


def execute(params):
    js = JobsService(_get_api_client(get_config()))
    job_run_id = js.run_now(job_id=_synapse_unload_job_id,
                            notebook_params=params)

    run_id = job_run_id["run_id"]
    job_run_info = js.get_run(run_id=run_id)

    while not is_job_done(job_run_info):
        job_run_info = js.get_run(run_id=run_id)
        time.sleep(10)
    # TODO: return correct results
    return job_run_info


def par_execute(params_list: List[Dict[str, str]]):
    with ThreadPool(processes=3) as pool:
        return pool.map(execute, params_list)
