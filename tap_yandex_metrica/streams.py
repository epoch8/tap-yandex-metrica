"""Stream type classes for tap-yandex-metrica."""

from __future__ import annotations

import requests
import pandas as pd
from typing import Any, Optional
import io
from collections.abc import Iterable
import datetime

from singer_sdk.helpers.types import Auth, Context
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_yandex_metrica.client import YandexMetricaStream


class RequestVisitsStream(YandexMetricaStream):
    """Define custom stream."""

    name = "request_visits"
    path = "logrequests"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "requestId": str(self.stream__request_id),
            "part_number": str(record["part_number"]),
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


class VisitsStream(RequestVisitsStream):
    """Define custom stream."""

    name = "visits"
    parent_stream_type = RequestVisitsStream
    ignore_parent_replication_keys = True
    path = "logrequest/{requestId}/part/{part_number}/download"
    primary_keys = ("ym_s_visitID",)

    schema = th.PropertiesList(
        th.Property("request_id", th.StringType),
        th.Property("part_number", th.StringType),
        th.Property("extracted_at", th.StringType),
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
        return {}
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        df = pd.read_csv(io.BytesIO(response.content), sep="\t", dtype=str)
        df.columns = [column.replace(":", "_") for column in df.columns]
        yield from df.to_dict(orient="records")

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row["request_id"] = context["requestId"]
        row["part_number"] = context["part_number"]
        row["extracted_at"] = f"{datetime.datetime.now()}"
        return row


class RequestHitsStream(YandexMetricaStream):
    """Define custom stream."""

    name = "request_hits"
    path = "logrequests"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "requestId": str(self.stream__request_id),
            "part_number": str(record["part_number"]),
        }
    
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
        th.Property("part_number", th.IntegerType),
        th.Property("size", th.IntegerType),
    ).to_dict()


class HitsStream(RequestHitsStream):
    """Define custom stream."""

    name = "hits"
    parent_stream_type = RequestHitsStream
    ignore_parent_replication_keys = True
    path = "logrequest/{requestId}/part/{part_number}/download"
    primary_keys = ("ym_pv_watchID",)

    schema = th.PropertiesList(
        th.Property("request_id", th.StringType),
        th.Property("part_number", th.StringType),
        th.Property("extracted_at", th.StringType),
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
        return {}
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        df = pd.read_csv(io.BytesIO(response.content), sep="\t", dtype=str)
        df.columns = [column.replace(":", "_") for column in df.columns]
        yield from df.to_dict(orient="records")

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row["request_id"] = context["requestId"]
        row["part_number"] = context["part_number"]
        row["extracted_at"] = f"{datetime.datetime.now()}"
        return row
