from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class ChannelTag(SQLModel, table=True):
    __tablename__ = "channel_tag"

    channel_id: UUID = Field(foreign_key="channel.id", primary_key=True)
    tag_id: UUID = Field(foreign_key="tag.id", primary_key=True)


class Tag(SQLModel, table=True):
    __tablename__ = "tag"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str = Field(unique=True, index=True)
