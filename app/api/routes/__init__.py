from fastapi import APIRouter

from app.api.routes.ai_prompts import router as ai_prompts_router
from app.api.routes.ai_providers import router as ai_providers_router
from app.api.routes.ai_types import router as ai_types_router
from app.api.routes.channels import router as channels_router
from app.api.routes.projects import router as projects_router
from app.api.routes.tasks import router as tasks_router
from app.api.routes.tags import router as tags_router
from app.api.routes.updates import router as updates_router
from app.api.routes.uploads import router as uploads_router
from app.api.routes.proxies import router as proxies_router

api_router = APIRouter()
api_router.include_router(tags_router, tags=["tags"])
api_router.include_router(projects_router, prefix="/projects", tags=["projects"])
api_router.include_router(channels_router, prefix="/channels", tags=["channels"])
api_router.include_router(uploads_router, prefix="/uploads", tags=["uploads"])
api_router.include_router(tasks_router, prefix="/tasks", tags=["tasks"])
api_router.include_router(updates_router, prefix="/updates", tags=["updates"])
api_router.include_router(proxies_router, prefix="/proxies", tags=["proxies"])
api_router.include_router(ai_types_router, prefix="/ai-types", tags=["ai-types"])
api_router.include_router(
    ai_providers_router, prefix="/ai-providers", tags=["ai-providers"]
)
api_router.include_router(ai_prompts_router, prefix="/ai-prompts", tags=["ai-prompts"])
