"""REST client handling, including BitbucketStream base class."""

from __future__ import annotations

from urllib.parse import parse_qsl
from typing import Any, Iterable

import requests
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

class BitbucketPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        return response.json().get("next")


class BitbucketStream(RESTStream):
    """bitbucket stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.bitbucket.org/2.0"

    records_jsonpath = "$.values[*]"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("auth_username", ""),
            password=self.config.get("auth_password", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {
            "Content-Type": "application/json"
        }

        return headers

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return BitbucketPaginator()

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """

        params: dict = {}
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        starting_date = self.get_starting_timestamp(context)
        if starting_date and self.replication_key:
            params["q"] = f"({self.replication_key} >= {starting_date.isoformat()})"

        if self.replication_key:
            params["sort"] = f"~{self.replication_key}"

        return params

    # def prepare_request_payload(
    #     self,
    #     context: dict | None,  # noqa: ARG002
    #     next_page_token: Any | None,  # noqa: ARG002, ANN401
    # ) -> dict | None:
    #     """Prepare the data payload for the REST API request.

    #     By default, no payload will be sent (return None).

    #     Args:
    #         context: The stream context.
    #         next_page_token: The next page index or value.

    #     Returns:
    #         A dictionary with the JSON body for a POST requests.
    #     """
    #     return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    # def post_process(
    #     self,
    #     row: dict,
    #     context: dict | None = None,  # noqa: ARG002
    # ) -> dict | None:
    #     """As needed, append or transform raw data to match expected structure.

    #     Args:
    #         row: An individual record from the stream.
    #         context: The stream context.

    #     Returns:
    #         The updated record dictionary, or ``None`` to skip the record.
    #     """
    #     return row
