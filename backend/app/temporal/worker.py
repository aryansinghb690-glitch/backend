import asyncio

from temporalio.worker import Worker

from app.core.config import get_settings
from app.services.temporal_client import get_temporal_client
from app.temporal.activities import evaluate_decision_activity, run_node_activity
from app.temporal.workflows import SagePilotWorkflow


async def main() -> None:
    settings = get_settings()
    client = await get_temporal_client()
    # This worker is pretty small on purpose, just enough to wire workflows + activities.
    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_TASK_QUEUE,
        workflows=[SagePilotWorkflow],
        activities=[run_node_activity, evaluate_decision_activity],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
