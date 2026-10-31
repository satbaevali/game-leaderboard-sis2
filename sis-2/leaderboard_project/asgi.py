"""ASGI application for Django + lifecycle management for Postgres pool, Redis, gRPC server and jobs."""

import os

from django.core.asgi import get_asgi_application

from app.runtime import get_runtime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "leaderboard_project.settings")
django_asgi_app = get_asgi_application()


async def application(scope, receive, send):
    if scope["type"] == "lifespan":
        await handle_lifespan(receive, send)
        return
    runtime = await get_runtime()
    scope["runtime"] = runtime
    await django_asgi_app(scope, receive, send)


async def handle_lifespan(receive, send) -> None:
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await get_runtime()
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            runtime = await get_runtime()
            await runtime.stop()
            await send({"type": "lifespan.shutdown.complete"})
            return
