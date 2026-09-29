import asyncio
import json

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.api.deps import verify_token
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


class AckRequest(BaseModel):
    subscriber_id: str
    event_ids: list[str]


@router.get("/sse")
async def sse_updates(
    request: Request, subscriber_id: str | None = None
) -> StreamingResponse:
    subscriber_id, queue = await notifications.subscribe(subscriber_id)

    async def event_generator():
        try:
            # 1. Hand the client its subscriber id so it can ack later.
            pending = notifications.get_pending_messages(subscriber_id)
            opened_payload = {
                "subscriber_id": subscriber_id,
                "pending_count": len(pending),
            }
            yield f"event: connection.opened\ndata: {json.dumps(opened_payload)}\n\n"
            await asyncio.sleep(0)

            # 2. Replay any unacknowledged events from a previous connection.
            for message in pending:
                if await request.is_disconnected():
                    break
                yield _format_event(message)
                await asyncio.sleep(0)

            # 3. Stream new events as they arrive.
            while True:
                if await request.is_disconnected():
                    break
                try:
                    await asyncio.sleep(0)
                    event_id = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    yield ":keep-alive\n\n"
                    await asyncio.sleep(0)
                    continue

                message = notifications.get_message(subscriber_id, event_id)
                if message is not None:
                    yield _format_event(message)
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        finally:
            await notifications.unsubscribe(subscriber_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked",
        },
    )


@router.post("/ack")
async def acknowledge_updates(data: AckRequest) -> dict:
    removed = await notifications.ack(data.subscriber_id, data.event_ids)
    return {"acknowledged": removed}


def _format_event(message: dict) -> str:
    return f"id: {message['id']}\nevent: {message['event']}\ndata: {json.dumps(message['payload'])}\n\n"
