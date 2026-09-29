from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import get_session, verify_token
from app.models.channel import Channel
from app.models.project import utc_now
from app.models.tag import ChannelTag, Tag
from app.schemas.channel import ChannelOut
from app.schemas.tag import TagCreate, TagOut
from app.services.notification_service import notifications

router = APIRouter(dependencies=[Depends(verify_token)])


@router.get("/tags", response_model=list[TagOut])
async def list_tags(session: AsyncSession = Depends(get_session)):
    return (await session.exec(select(Tag).order_by(Tag.name))).scalars().all()


@router.post("/tags", response_model=TagOut)
async def create_tag(data: TagCreate, session: AsyncSession = Depends(get_session)):
    await session.exec(
        insert(Tag).values(id=uuid4(), name=data.name)
        .on_conflict_do_nothing(index_elements=["name"])
    )
    await session.commit()
    return (await session.exec(select(Tag).where(Tag.name == data.name))).scalar_one()


async def change_channel_tag(
    channel_id: UUID, tag_id: UUID, session: AsyncSession, *, attach: bool
):
    channel = await session.get(Channel, channel_id)
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    if await session.get(Tag, tag_id) is None:
        raise HTTPException(status_code=404, detail="Tag not found")
    if attach:
        await session.exec(
            insert(ChannelTag).values(channel_id=channel_id, tag_id=tag_id)
            .on_conflict_do_nothing(index_elements=["channel_id", "tag_id"])
        )
    else:
        await session.exec(delete(ChannelTag).where(
            ChannelTag.channel_id == channel_id, ChannelTag.tag_id == tag_id
        ))
    channel.updated_at = utc_now()
    await session.commit()
    await session.refresh(channel)
    result = ChannelOut.model_validate(channel)
    await notifications.broadcast(
        "channel.updated", {"channel": result.model_dump(mode="json")}
    )
    return result


@router.put("/channels/{channel_id}/tags/{tag_id}", response_model=ChannelOut)
async def add_channel_tag(
    channel_id: UUID, tag_id: UUID, session: AsyncSession = Depends(get_session)
):
    return await change_channel_tag(channel_id, tag_id, session, attach=True)


@router.delete("/channels/{channel_id}/tags/{tag_id}", response_model=ChannelOut)
async def remove_channel_tag(
    channel_id: UUID, tag_id: UUID, session: AsyncSession = Depends(get_session)
):
    return await change_channel_tag(channel_id, tag_id, session, attach=False)
