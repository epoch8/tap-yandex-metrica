"""Stream type classes for tap-yandex-metrica."""

from __future__ import annotations

import requests
import pandas as pd
from typing import TYPE_CHECKING, Any, ClassVar, Optional
import io

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Auth, Context

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


class RequestVisitsStream(YandexMetricaStream):
    """Define custom stream."""

    name = "request_visits"
    path = "logrequests"
    # path = "logrequest/{requestId}"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "requestId": str(self.stream__request_id),
            "partNumber": record["part_number"],
        }
    
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
        th.Property("part_number", th.IntegerType),
        th.Property("size", th.IntegerType),
    ).to_dict()

    # def post_process(
    #     self,
    #     row: dict,
    #     context: Context | None = None,
    # ) -> dict | None:
    #     """As needed, append or transform raw data to match expected structure.

    #     Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
    #     You should not need to call this method directly in custom `get_records` implementations.

    #     Args:
    #         row: An individual record from the stream.
    #         context: The stream context.

    #     Returns:
    #         The updated record dictionary, or ``None`` to skip the record.
    #     """
    #     # TODO: Delete this method if not needed.
    #     row["part_number"] = f"{row['part_number']}"
    #     row["size"] = f"{row['size']}"

    #     return row


class VisitsStream(RequestVisitsStream):
    """Define custom stream."""

    name = "visits"
    parent_stream_type = RequestVisitsStream
    ignore_parent_replication_keys = True
    path = "logrequest/{requestId}/part/{partNumber}/download"
    primary_keys = ("ym_s_visitID",)
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

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        # self.logger.warning(context)
        # self.logger.warning(self.path)
        return {}
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        df = pd.read_csv(io.BytesIO(response.content), sep="\t", dtype=str)

        self.logger.info(f"DF shape: {df.shape}")
        # self.logger.warning(response.status_code)
        # self.logger.warning(response.text)
        # self.logger.warning(self.path)

        yield from df.itertuples(index=False) # FIX ME!!! ==========================================================
        # raise Exception(f"parse_response")


class HitsStream(YandexMetricaStream):
    """Define custom stream."""

    name = "hits"
    path = "logrequest/{requestId}/part/{partNumber}/download"
    primary_keys = ("ym_pv_watchID",)
    # replication_key = "modified"

    stream__source = "hits"
    stream__fields = [
        "ym:pv:dateTime",
        "ym:pv:watchID",
        "ym:pv:pageViewID",
        "ym:pv:clientID",
        "ym:pv:URL",
        "ym:pv:referer",
        "ym:pv:UTMSource",
        "ym:pv:UTMMedium",
        "ym:pv:UTMCampaign",
        "ym:pv:UTMTerm",
        "ym:pv:UTMContent",
        "ym:pv:hasGCLID",
        "ym:pv:GCLID",
        "ym:pv:params",
        "ym:pv:deviceCategory",
        "ym:pv:operatingSystem",
        "ym:pv:browser",
        "ym:pv:browserMajorVersion",
        "ym:pv:browserMinorVersion",
        "ym:pv:regionCountry",
        "ym:pv:regionCity",
        "ym:pv:browserLanguage",
        "ym:pv:screenWidth",
        "ym:pv:screenHeight",
        "ym:pv:isPageView",
        "ym:pv:link",
        "ym:pv:download",
        "ym:pv:notBounce",
        "ym:pv:ecommerce",
    ]

    schema = th.PropertiesList(
        th.Property("ym_pv_dateTime", th.StringType),
        th.Property("ym_pv_watchID", th.StringType),
        th.Property("ym_pv_pageViewID", th.StringType),
        th.Property("ym_pv_clientID", th.StringType),
        th.Property("ym_pv_URL", th.StringType),
        th.Property("ym_pv_referer", th.StringType),
        th.Property("ym_pv_UTMSource", th.StringType),
        th.Property("ym_pv_UTMMedium", th.StringType),
        th.Property("ym_pv_UTMCampaign", th.StringType),
        th.Property("ym_pv_UTMTerm", th.StringType),
        th.Property("ym_pv_UTMContent", th.StringType),
        th.Property("ym_pv_hasGCLID", th.StringType),
        th.Property("ym_pv_GCLID", th.StringType),
        th.Property("ym_pv_params", th.StringType),
        th.Property("ym_pv_deviceCategory", th.StringType),
        th.Property("ym_pv_operatingSystem", th.StringType),
        th.Property("ym_pv_browser", th.StringType),
        th.Property("ym_pv_browserMajorVersion", th.StringType),
        th.Property("ym_pv_browserMinorVersion", th.StringType),
        th.Property("ym_pv_regionCountry", th.StringType),
        th.Property("ym_pv_regionCity", th.StringType),
        th.Property("ym_pv_browserLanguage", th.StringType),
        th.Property("ym_pv_screenWidth", th.StringType),
        th.Property("ym_pv_screenHeight", th.StringType),
        th.Property("ym_pv_isPageView", th.StringType),
        th.Property("ym_pv_link", th.StringType),
        th.Property("ym_pv_download", th.StringType),
        th.Property("ym_pv_notBounce", th.StringType),
        th.Property("ym_pv_ecommerce", th.StringType),
    ).to_dict()
