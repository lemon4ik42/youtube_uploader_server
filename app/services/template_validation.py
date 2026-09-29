from datetime import datetime, timezone
from pathlib import Path


# YouTube metadata limits.
_MAX_YOUTUBE_TITLE_LENGTH = 100
_MAX_YOUTUBE_DESCRIPTION_LENGTH = 5000

# Sample file name used to validate templates. Real filenames may be longer, but
# a template that fits this length is likely safe for typical uploads.
_SAMPLE_FILENAME = "a" * 80


def validate_channel_templates(title_template: str | None, description_template: str | None) -> None:
    """Validate channel title/description templates against YouTube limits.

    Raises ValueError with a descriptive message if the templates produce an
    empty title or content that exceeds YouTube limits.
    """
    file_path = f"/tmp/{_SAMPLE_FILENAME}.mp4"
    path = Path(file_path)
    values = {
        "filename": path.stem,
        "file": path.name,
        "ext": path.suffix.lstrip("."),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
    }

    title_template = (title_template or "").strip()
    try:
        title = title_template.format(**values) if title_template else values["filename"]
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Invalid title_template: {exc}") from exc
    title = title.strip()
    if not title:
        raise ValueError("title_template produces an empty video title")
    if len(title) > _MAX_YOUTUBE_TITLE_LENGTH:
        raise ValueError(
            f"title_template produces a title of {len(title)} characters; "
            f"YouTube limit is {_MAX_YOUTUBE_TITLE_LENGTH}"
        )

    description_template = (description_template or "").strip()
    try:
        description = (
            description_template.format(**values) if description_template else ""
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Invalid description_template: {exc}") from exc
    if len(description) > _MAX_YOUTUBE_DESCRIPTION_LENGTH:
        raise ValueError(
            f"description_template produces a description of {len(description)} characters; "
            f"YouTube limit is {_MAX_YOUTUBE_DESCRIPTION_LENGTH}"
        )
