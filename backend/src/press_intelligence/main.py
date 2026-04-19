from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from press_intelligence.api.routes import router
from press_intelligence.core.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Press Intelligence API",
        version="0.1.0",
        summary="Editorial analytics and operations API for Guardian content pipelines.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router, prefix="/api")

    @app.get("/")
    async def root() -> dict[str, str]:
        return {
            "name": "press-intelligence",
            "mode": settings.data_mode,
            "status": "ok",
        }

    return app


app = create_app()


def main() -> None:
    uvicorn.run("press_intelligence.main:app", host="0.0.0.0", port=8000, reload=True)
