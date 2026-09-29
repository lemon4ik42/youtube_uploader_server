from pydantic import BaseModel, Field, field_validator

MAX_PROMPT_LENGTH = 20_000


class AIPromptsOut(BaseModel):
    title_prompt: str
    description_prompt: str


class AIPromptsUpdate(BaseModel):
    title_prompt: str | None = None
    description_prompt: str | None = None

    @field_validator("title_prompt", "description_prompt")
    @classmethod
    def _validate_prompt(cls, value: str | None) -> str | None:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("Prompt must not be empty")
        if len(value) > MAX_PROMPT_LENGTH:
            raise ValueError(f"Prompt must not exceed {MAX_PROMPT_LENGTH} characters")
        return value
