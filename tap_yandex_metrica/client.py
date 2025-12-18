"""REST client handling, including YandexMetricaStream base class."""

from __future__ import annotations

import decimal
import sys
from typing import TYPE_CHECKING, Any, ClassVar, Generator
import backoff
import time
import datetime
import requests

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream

from tap_yandex_metrica import schemas

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

    records_jsonpath = "$.parts[*]"

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    stream__source = None
    stream__fields = None
    stream__request_id = None
    stream__last_part = None
    is_request_cleaned = False


    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"https://api-metrika.yandex.net/management/v1/counter/{self.config['counter_id']}/"

    
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
        date1 = (
            self.config.get("start_date")
            or datetime.datetime.now().date().__sub__(datetime.timedelta(days=1+int(self.config["days_ago"]))).strftime(f"%Y-%m-%d")
        )
        date2 = (
            self.config.get("end_date")
            or datetime.datetime.now().date().__sub__(datetime.timedelta(days=1)).strftime(f"%Y-%m-%d")
        )

        if not self.stream__request_id:
            self.stream__request_id = self.find_logrequest(date1, date2)

        if not self.stream__request_id:
            self.stream__request_id = self.create_logrequest(date1, date2)

        self.logger.info(f"Request ID: {self.stream__request_id}")
        self.stream__last_part = self.wait_logrequest(self.stream__request_id)[-1]
        return {}


    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        def find_request(requests):
            for request in requests["requests"]:

                if (
                    str(request["counter_id"]) == str(self.config["counter_id"])
                    and str(request["request_id"]) == str(self.stream__request_id)
                ):
                    return request


        request = find_request(response.json(parse_float=decimal.Decimal))
        assert request is not None

        yield from extract_jsonpath(
            self.records_jsonpath,
            input=request,
        )


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
        
        return status


    def wait_logrequest(self, request_id):
        wait_attempt = N_WAIT_RETRIES

        while wait_attempt > 0:
            parts = self.check_logrequest(request_id)

            if isinstance(parts, list):
                return parts
            
            wait_attempt = wait_attempt - 1
            self.logger.info(f"Request ID {request_id} status is \"{parts}\", waiting {WAIT_SECONDS} secods for retry. Attemps left: {wait_attempt}")
            time.sleep(WAIT_SECONDS)

        raise Exception(f"Request ID {request_id} hasn't been processed during {N_WAIT_RETRIES} retries.")


    def clean_logrequest(self, request_id):
        URL__CLEAN_LOGREQUEST = self.url_base + f"logrequest/{request_id}/clean"

        requests.post(
            URL__CLEAN_LOGREQUEST,
            headers={"Authorization": f"OAuth {self.config['auth_token']}"},
        )

        self.logger.info(f"Request ID {request_id} has been cleaned.")
