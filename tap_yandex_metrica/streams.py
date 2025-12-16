"""Stream type classes for tap-yandex-metrica."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_yandex_metrica.client import YandexMetricaStream

# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


# class UsersStream(YandexMetricaStream):
#     """Define custom stream."""

#     name = "users"
#     path = "/users"
#     primary_keys = ("id",)
#     replication_key = None
#     # Optionally, you may also use `schema_filepath` in place of `schema`:
#     # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
#     schema = th.PropertiesList(
#         th.Property("name", th.StringType),
#         th.Property(
#             "id",
#             th.StringType,
#             description="The user's system ID",
#         ),
#         th.Property(
#             "age",
#             th.IntegerType,
#             description="The user's age in years",
#         ),
#         th.Property(
#             "email",
#             th.StringType,
#             description="The user's email address",
#         ),
#         th.Property("street", th.StringType),
#         th.Property("city", th.StringType),
#         th.Property(
#             "state",
#             th.StringType,
#             description="State name in ISO 3166-2 format",
#         ),
#         th.Property("zip", th.StringType),
#     ).to_dict()


# class GroupsStream(YandexMetricaStream):
#     """Define custom stream."""

#     name = "groups"
#     path = "/groups"
#     primary_keys = ("id",)
#     replication_key = "modified"
#     schema = th.PropertiesList(
#         th.Property("name", th.StringType),
#         th.Property("id", th.StringType),
#         th.Property("modified", th.DateTimeType),
#     ).to_dict()


class VisitsStream(YandexMetricaStream):
    """Define custom stream."""

    name = "visits"
    stream__path = "logrequest/{requestId}/part/{partNumber}/download"
    path = None
    primary_keys = ("visitID",)
    # replication_key = "modified"

    stream__source = "visits"
    stream__fields = [
        "ym:s:dateTime",
        "ym:s:date",
        "ym:s:visitID",
        "ym:s:clientID",
        "ym:s:startURL",
        "ym:s:referer",
        "ym:s:pageViews",
        "ym:s:isNewUser",
        "ym:s:regionCountry",
        "ym:s:regionCity",
        "ym:s:lastsignTrafficSource",
        "ym:s:lastTrafficSource",
        "ym:s:lastSearchEngineRoot",
        "ym:s:lastSearchEngine",
        "ym:s:lastUTMSource",
        "ym:s:lastUTMMedium",
        "ym:s:lastUTMCampaign",
        "ym:s:lastUTMTerm",
        "ym:s:lastUTMContent",
        "ym:s:lasthasGCLID",
        "ym:s:lastGCLID",
    ]

    schema = th.PropertiesList(
        th.Property("ym_s_dateTime", th.StringType),
        th.Property("ym_s_date", th.StringType),
        th.Property("ym_s_visitID", th.StringType),
        th.Property("ym_s_clientID", th.StringType),
        th.Property("ym_s_startURL", th.StringType),
        th.Property("ym_s_referer", th.StringType),
        th.Property("ym_s_pageViews", th.StringType),
        th.Property("ym_s_isNewUser", th.StringType),
        th.Property("ym_s_regionCountry", th.StringType),
        th.Property("ym_s_regionCity", th.StringType),
        th.Property("ym_s_lastsignTrafficSource", th.StringType),
        th.Property("ym_s_lastTrafficSource", th.StringType),
        th.Property("ym_s_lastSearchEngineRoot", th.StringType),
        th.Property("ym_s_lastSearchEngine", th.StringType),
        th.Property("ym_s_lastUTMSource", th.StringType),
        th.Property("ym_s_lastUTMMedium", th.StringType),
        th.Property("ym_s_lastUTMCampaign", th.StringType),
        th.Property("ym_s_lastUTMTerm", th.StringType),
        th.Property("ym_s_lastUTMContent", th.StringType),
        th.Property("ym_s_lasthasGCLID", th.StringType),
        th.Property("ym_s_lastGCLID", th.StringType),
    ).to_dict()
