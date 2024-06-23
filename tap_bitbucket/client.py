"""REST client handling, including BitbucketStream base class."""

from __future__ import annotations

from urllib.parse import parse_qsl
from typing import Any, Iterable, Callable, Generator

import requests
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
import random
import backoff

class BitbucketBasePaginator(BaseHATEOASPaginator):
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
        return BitbucketBasePaginator()

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

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def backoff_jitter(self, value):
        return random.uniform(0, value)

    def backoff_max_tries(self):
        return 1000
    
    def backoff_wait_generator(self):
        return backoff.expo(base=10, factor=2, max_value=300)
