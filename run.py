import uvicorn

from app.core.config import settings

if __name__ == "__main__":
    print(f"Starting YouTube Uploader Server on {settings.host}:{settings.port}")
    # Raising timeout-keep-alive above the client's body-send duration reduces
    # "connection reset" errors during large multipart uploads. On Windows,
    # h11 with a generous keep-alive is more forgiving of slow TCP resets.
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        workers=1,
        timeout_keep_alive=300,
    )
