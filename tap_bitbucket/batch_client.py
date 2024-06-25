"""REST client handling, including BitbucketStream base class."""

from __future__ import annotations

from typing import Any

import requests
from singer_sdk.pagination import BaseHATEOASPaginator  # noqa: TCH002
from .client import BitbucketStream

class BitbucketPaginator(BaseHATEOASPaginator):
    def __init__(self, is_force_stop, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.is_force_stop = is_force_stop

    def get_next_url(self, response):
        return response.json().get("next")

    def has_more(self, response: requests.Response) -> bool:
        if self.is_force_stop():
            return False
        return super().has_more(response)


class BitbucketBatchStream(BitbucketStream):
    """bitbucket stream class."""

    _is_force_stop = False

    def is_force_stop(self):
        return self._is_force_stop

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return BitbucketPaginator(self.is_force_stop)

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
        if starting_date:
            if row[self.replication_key] <= starting_date.isoformat():
                self._is_force_stop = True ## assuming api response sorted desc
                return None

        return row
