from pydantic import BaseModel


class ManualUploadForm(BaseModel):
    title: str | None = None
    description: str | None = None
