"""REST client handling, including YandexMetricaStream base class."""

from __future__ import annotations

import decimal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar
import backoff
import time
import datetime
import pandas as pd
import io
import requests

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError

from tap_yandex_metrica import schemas
# from tap_yandex_metrica.auth import YandexMetricaAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Auth, Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = SchemaDirectory(schemas)

N_WAIT_RETRIES = 60
WAIT_SECONDS = 60


class YandexMetricaStream(RESTStream):
    """YandexMetrica stream class."""

    # Update this value if necessary or override `parse_response`.
    # records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    # next_page_token_jsonpath = "$.next_page"  # noqa: S105

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    stream__source = None
    stream__fields = None
    stream__request = None
    stream__path = None
    stream__parts = None
    stream__part = None

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        # return "https://api.mysample.com"
        return f"https://api-metrika.yandex.net/management/v1/counter/{self.config['counter_id']}/"

    @override
    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        # return YandexMetricaAuthenticator(
        #     client_id=self.config["client_id"],
        #     client_secret=self.config["client_secret"],
        #     auth_endpoint="TODO: OAuth Endpoint URL",
        #     oauth_scopes="TODO: OAuth Scopes",
        # )
        return SimpleAuthenticator(
            self, {"Authorization": f"OAuth {self.config['auth_token']}"}
        )
    
    def backoff_wait_generator(self) -> Generator[float, None, None]:
        return backoff.constant(120)

    def backoff_max_tries(self) -> int:
        return 100

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {"Authorization": f"OAuth {self.config['auth_token']}"}

    @override
    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        # return super().get_new_paginator()
        return None

    @override
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
        # params: dict = {}
        # if next_page_token:
        #     params["page"] = next_page_token
        # if self.replication_key:
        #     params["sort"] = "asc"
        #     params["order_by"] = self.replication_key

        date1 = self.config.get("start_date") or datetime.datetime.now().date().__sub__(datetime.timedelta(days=1)).strftime(f"%Y-%m-%d")
        date2 = self.config.get("start_date") or datetime.datetime.now().date().__sub__(datetime.timedelta(days=1)).strftime(f"%Y-%m-%d")

        if not self.stream__request:
            self.stream__request = self.find_logrequest(date1, date2)

        # self.logger.warning(f"Request ID: {self.stream__request}")

        if not self.stream__request:
            # raise Exception(f"self.stream__request is None")
            self.stream__request = self.create_logrequest(date1, date2)

        self.logger.warning(f"Request ID: {self.stream__request}")

        self.stream__parts = self.wait_logrequest(self.stream__request)
        self.stream__part = self.stream__parts[0]

        self.update_path()

        request_params = {
            "date1": date1,
            "date2": date2,
            "source": self.stream__source,
            "fields": ",".join(self.stream__fields),
        }
        
        # self.logger.warning(f"self.path: {self.path}")

        return request_params
        # return {}

    # @override
    # def prepare_request_payload(
    #     self,
    #     context: Context | None,
    #     next_page_token: Any | None,
    # ) -> dict | None:
    #     """Prepare the data payload for the REST API request.

    #     By default, no payload will be sent (return None).

    #     Args:
    #         context: The stream context.
    #         next_page_token: The next page index or value.

    #     Returns:
    #         A dictionary with the JSON body for a POST requests.
    #     """
    #     # TODO: Delete this method if no payload is required. (Most REST APIs.)

    #     return None

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        # yield from extract_jsonpath(
        #     self.records_jsonpath,
        #     input=response.json(parse_float=decimal.Decimal),
        # )

        # yield from pd.read_csv(io.BytesIO(response.content), sep="\t", dtype=str).itertuples(index=False)
        raise Exception(f"parse_response")


    @override
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
        # TODO: Delete this method if not needed.
        return row


    def create_logrequest(self, date1, date2):
        URL__CREATE_LOGREQUEST = self.url_base + "logrequests"

        request_params = {
            "date1": date1,
            "date2": date2,
            "source": self.stream__source,
            "fields": ",".join(self.stream__fields),
        }

        request = requests.post(
            URL__CREATE_LOGREQUEST,
            headers={"Authorization": f"OAuth {self.config['auth_token']}"},
            params=request_params,
        )

        return request.json().get("log_request", {}).get("request_id")


    def find_logrequest(self, date1, date2):
        URL__FIND_LOGREQUEST = self.url_base + "logrequests"

        request = requests.get(
            URL__FIND_LOGREQUEST,
            headers={"Authorization": f"OAuth {self.config['auth_token']}"},
        )

        logrequests = request.json().get("requests")

        stream_fields = self.stream__fields
        stream_fields.sort()
        stream_fields = ",".join(stream_fields)

        for logrequest in logrequests:

            logrequest_fields = logrequest["fields"]
            logrequest_fields.sort()
            logrequest_fields = ",".join(logrequest_fields)

            # self.logger.warning(logrequest["request_id"])
            # self.logger.warning(self.config["counter_id"] == str(logrequest["counter_id"]))
            # self.logger.warning(self.stream__source == logrequest["source"])
            # self.logger.warning(date1 == logrequest["date1"])
            # self.logger.warning(date2 == logrequest["date2"])
            # self.logger.warning(stream_fields == logrequest_fields)

            if (
                self.config["counter_id"] == str(logrequest["counter_id"])
                and self.stream__source == logrequest["source"]
                and date1 == logrequest["date1"]
                and date2 == logrequest["date2"]
                and stream_fields == logrequest_fields
            ):
                return logrequest["request_id"]


    def check_logrequest(self, request_id):
        URL__CHECK_LOGREQUEST = self.url_base + f"logrequest/{request_id}"

        request = requests.get(
            URL__CHECK_LOGREQUEST,
            headers={"Authorization": f"OAuth {self.config['auth_token']}"},
        )

        if request.json().get("errors"):
            raise Exception(request.json())

        status = request.json().get("log_request", {}).get("status")

        if status == "processed":
            parts = request.json().get("log_request", {}).get("parts")
            parts = [part["part_number"] for part in parts]
            return parts


    def wait_logrequest(self, request_id):
        wait_attempt = N_WAIT_RETRIES

        while wait_attempt > 0:
            parts = self.check_logrequest(request_id)

            if parts:
                return parts
            
            wait_attempt = wait_attempt - 1
            self.logger.info(f"Request ID {request_id} status is not \"processed\", waiting {WAIT_SECONDS} secods for retry. Attemps left: {wait_attempt}")
            time.sleep(WAIT_SECONDS)

        raise Exception(f"Request ID {request_id} hasn't been processed during {N_WAIT_RETRIES} retries.")


    def clean_logrequest(self, request_id):
        URL__CLEAN_LOGREQUEST = self.url_base + f"logrequest/{request_id}/clean"

        request = requests.post(
            URL__CLEAN_LOGREQUEST,
            headers={"Authorization": f"OAuth {self.config['auth_token']}"},
        )


    def update_path(self):
        self.path = self.stream__path.format(requestId=self.stream__request, partNumber=self.stream__part)


    # def request_decorator(self, func: RequestFunc) -> RequestFunc:
    #     """Instantiate a decorator for handling request failures.

    #     Uses a wait generator defined in `backoff_wait_generator` to
    #     determine backoff behaviour. Try limit is defined in
    #     `backoff_max_tries`, and will trigger the event defined in
    #     `backoff_handler` before retrying. Developers may override one or
    #     all of these methods to provide custom backoff or retry handling.

    #     Args:
    #         func: Function to decorate.

    #     Returns:
    #         A decorated method.
    #     """
    #     decorator: t.Callable = backoff.on_exception(
    #         self.backoff_wait_generator,
    #         (
    #             ConnectionResetError,
    #             RetriableAPIError,
    #             requests.exceptions.Timeout,
    #             requests.exceptions.ConnectionError,
    #             requests.exceptions.ChunkedEncodingError,
    #             requests.exceptions.ContentDecodingError,
    #         ),
    #         # target=print,
    #         # print,
    #         max_tries=self.backoff_max_tries,
    #         on_backoff=self.backoff_handler,
    #         jitter=self.backoff_jitter,
    #         logger=self.logger,
    #     )(func)
    #     return decorator
