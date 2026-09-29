from uuid import UUID

from sqlalchemy import delete, or_, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.security import decrypt, encrypt
from app.models.project import GCPProject
from app.models.channel import Channel
from app.models.channel_project_auth import ChannelProjectAuth
from app.models.oauth_state import OAuthState
from app.schemas.project import ProjectOut
from app.services.notification_service import notifications


def project_payload(project: GCPProject) -> dict:
    return ProjectOut.model_validate(project).model_dump(mode="json")


class ProjectService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self,
        name: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        scope: str,
        token_uri: str,
        auth_uri: str,
        api_upload: str,
    ) -> GCPProject:
        result = await self.session.exec(select(GCPProject))
        for existing in result.scalars().all():
            if (
                existing.client_id == client_id
                and decrypt(existing.client_secret_enc) == client_secret
            ):
                raise ValueError(
                    "A project with the same client_id and client_secret already exists"
                )
        project = GCPProject(
            name=name,
            client_id=client_id,
            client_secret_enc=encrypt(client_secret),
            redirect_uri=redirect_uri,
            scope=scope,
            token_uri=token_uri,
            auth_uri=auth_uri,
            api_upload=api_upload,
        )
        self.session.add(project)
        await self.session.commit()
        await self.session.refresh(project)
        await notifications.broadcast(
            "project.created", {"project": project_payload(project)}
        )
        return project

    async def list(self) -> list[GCPProject]:
        result = await self.session.exec(select(GCPProject))
        return list(result.scalars().all())

    async def get(self, project_id: UUID) -> GCPProject | None:
        return await self.session.get(GCPProject, project_id)

    async def update(
        self, project: GCPProject, **data: str | int | bool
    ) -> GCPProject:
        if "client_secret" in data:
            data["client_secret_enc"] = encrypt(data.pop("client_secret"))  # type: ignore[arg-type]
        for key, value in data.items():
            setattr(project, key, value)
        await self.session.commit()
        await self.session.refresh(project)
        await notifications.broadcast(
            "project.updated", {"project": project_payload(project)}
        )
        return project

    async def delete(self, project: GCPProject) -> None:
        project_id = project.id
        channels_result = await self.session.exec(
            select(Channel).where(
                or_(
                    Channel.project_id == project_id,
                    Channel.pending_project_id == project_id,
                )
            )
        )
        if channels_result.scalars().first():
            raise ValueError(
                "Cannot delete a project assigned to channels. "
                "Re-authorize or move the channels first."
            )
        oauth_result = await self.session.exec(
            select(OAuthState).where(OAuthState.project_id == project_id)
        )
        for oauth_state in oauth_result.scalars().all():
            await self.session.delete(oauth_state)
        await self.session.exec(
            delete(ChannelProjectAuth).where(
                ChannelProjectAuth.project_id == project_id
            )
        )
        await self.session.delete(project)
        await self.session.commit()
        await notifications.broadcast(
            "project.deleted", {"project_id": str(project_id)}
        )

    def get_client_secret(self, project: GCPProject) -> str:
        return decrypt(project.client_secret_enc)
