"""Opt-in Temporal trial of the canonical read-only release verifier.

No production work kind, provider, training job or order is routed here. Imports
of the optional SDK occur only when a Temporal trial explicitly selects it.
"""
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from pathlib import Path
    import time
    from downloader.artifact_io import atomic_write_json
    from stockagent.runtime_identity import verify_source_release


@activity.defn
async def verify_recorded_release(request: dict) -> dict:
    info = activity.info()
    path = Path(request['trace_root'])/f"{request['stage']}-attempt-{info.attempt}.json"
    if request['stage'] == 'first' and info.attempt == 1:
        atomic_write_json(path, {'stage':'first','attempt':1,'state':'injected_transient_failure',
                                'handler_executed':False})
        raise ApplicationError('bounded trial transient fault',type='TrialTransient')
    started = time.perf_counter()
    proof = verify_source_release(Path(request['receipt']),Path(request['source_root']))
    if any(proof[key] != request[key] for key in ('receipt_sha256','source_sha256')):
        raise ApplicationError('immutable source identity differs',non_retryable=True)
    atomic_write_json(path,{'stage':request['stage'],'attempt':info.attempt,'state':'succeeded',
                           'complete_wall_seconds':time.perf_counter()-started,'proof':proof})
    return proof


@workflow.defn
class ReleaseVerificationWorkflow:
    def __init__(self):
        self.status = 'created'
        self.resumed = False

    @workflow.signal
    def resume(self):
        self.resumed = True

    @workflow.query
    def state(self) -> str:
        return self.status

    @workflow.run
    async def run(self, request: dict) -> dict:
        self.status = 'first_verification'
        retry = RetryPolicy(initial_interval=timedelta(seconds=.2),maximum_attempts=3)
        first = await workflow.execute_activity(verify_recorded_release,{**request,'stage':'first'},
            start_to_close_timeout=timedelta(seconds=30),retry_policy=retry)
        self.status = 'durable_timer'
        await workflow.sleep(timedelta(seconds=2))
        self.status = 'waiting_for_resume'
        await workflow.wait_condition(lambda:self.resumed)
        self.status = 'second_verification'
        second = await workflow.execute_activity(verify_recorded_release,{**request,'stage':'second'},
            start_to_close_timeout=timedelta(seconds=30),retry_policy=retry)
        self.status = 'completed'
        return {'first':first,'second':second,'scope':'canonical read-only source verification only'}
