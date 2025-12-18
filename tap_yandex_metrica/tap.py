"""YandexMetrica tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_yandex_metrica import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapYandexMetrica(Tap):
    """Singer tap for YandexMetrica."""

    name = "tap-yandex-metrica"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "counter_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="counter_id",
            description="Yandex Metrica counter ID",
        ),
        th.Property(
            "start_date",
            th.StringType(nullable=True),
            description="The earliest record date to sync: YYYY-MM-DD",
        ),
        th.Property(
            "end_date",
            th.StringType(nullable=True),
            description="The latest record date to sync: YYYY-MM-DD",
        ),
        th.Property(
            "days_ago",
            th.StringType(nullable=True),
            default="2",
            description="Amount of days to reload if no start & end dates provided: 2",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.YandexMetricaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.RequestVisitsStream(self),
            streams.VisitsStream(self),
            streams.RequestHitsStream(self),
            streams.HitsStream(self),
        ]


if __name__ == "__main__":
    TapYandexMetrica.cli()
