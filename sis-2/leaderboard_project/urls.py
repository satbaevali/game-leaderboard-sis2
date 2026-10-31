"""Root URL configuration for the Django Game Leaderboard project."""

from django.urls import include, path

urlpatterns = [
    path("", include("app.urls")),
]
