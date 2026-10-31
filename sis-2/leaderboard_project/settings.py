"""Minimal Django settings for the High-Load Game Leaderboard service."""

import os

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "sis2-leaderboard-local-dev-key")
DEBUG = os.getenv("DJANGO_DEBUG", "false").lower() == "true"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "app",
]

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "leaderboard_project.urls"
WSGI_APPLICATION = "leaderboard_project.wsgi.application"
ASGI_APPLICATION = "leaderboard_project.asgi.application"

USE_TZ = True
TIME_ZONE = "UTC"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
