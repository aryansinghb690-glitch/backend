import asyncio

from temporalio.worker import Worker

from app.core.config import get_settings
from app.services.temporal_client import get_temporal_client
from app.temporal.activities import evaluate_decision_activity, run_node_activity
from app.temporal.workflows import SagePilotWorkflow


async def main() -> None:
    settings = get_settings()

    print("Starting Temporal worker...")
    print("Task queue:", settings.TEMPORAL_TASK_QUEUE)

    client = await get_temporal_client()
    print("Connected to Temporal")

    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_TASK_QUEUE,
        workflows=[SagePilotWorkflow],
        activities=[run_node_activity, evaluate_decision_activity],
    )

    print("Worker running and waiting for tasks...")
    await worker.run()
