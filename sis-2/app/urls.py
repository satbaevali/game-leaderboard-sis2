"""URL routing for the Leaderboard Django app."""

from django.urls import path

from app import views

urlpatterns = [
    path("api/v1/leaderboards/<int:season_id>/top", views.top_view, name="leaderboard-top"),
    path(
        "api/v1/leaderboards/<int:season_id>/players/<int:player_id>/rank",
        views.rank_view,
        name="leaderboard-rank",
    ),
    path(
        "api/v1/leaderboards/<int:season_id>/players/<int:player_id>/around",
        views.around_view,
        name="leaderboard-around",
    ),
    path("health", views.health_view, name="health"),
    path("metrics", views.metrics_view, name="metrics"),
]
