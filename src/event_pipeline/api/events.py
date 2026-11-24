"""`POST /events`: one event in, `202` once the broker acknowledged it (ADR 0003)."""

from uuid import UUID

from fastapi import APIRouter, status
from pydantic import BaseModel

from event_pipeline.api.dependencies import AppLateness, AppPublisher, CappedBody
from event_pipeline.api.errors import INGEST_ERROR_RESPONSES
from event_pipeline.services.ingest import ingest_event

router = APIRouter()


class EventAccepted(BaseModel):
    event_id: UUID


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    responses=INGEST_ERROR_RESPONSES,
    summary="Ingest one event",
)
async def post_event(
    body: CappedBody, publisher: AppPublisher, lateness: AppLateness
) -> EventAccepted:
    # The body is read as raw bytes, not as a Pydantic parameter: the bytes go to the topic
    # unchanged (ADR 0003, decision 2), and `parse_event` owns validation and its reason codes.
    event_id = await ingest_event(body, publisher, lateness)
    return EventAccepted(event_id=event_id)
