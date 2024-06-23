"""REST client handling, including BitbucketStream base class."""

from __future__ import annotations

from urllib.parse import parse_qsl
from typing import Any

import requests
from singer_sdk.pagination import BaseHATEOASPaginator  # noqa: TCH002
from .client import BitbucketStream

class BitbucketCommitsPaginator(BaseHATEOASPaginator):
    def __init__(self, is_force_stop, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.is_force_stop = is_force_stop

    def get_next_url(self, response):
        return response.json().get("next")

    def has_more(self, response: requests.Response) -> bool:
        if self.is_force_stop():
            return False
        return super().has_more(response)


class BitbucketCommitStream(BitbucketStream):
    """bitbucket stream class."""

    _is_force_stop = False

    def is_force_stop(self):
        return self._is_force_stop

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return BitbucketCommitsPaginator(self.is_force_stop)

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

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """

        starting_date = self.get_starting_timestamp(context)
        if starting_date and self.name == "commits":
            if row["date"] <= starting_date.isoformat():
                self._is_force_stop = True ## assuming commits api is sorted desc
                return None

        return row
