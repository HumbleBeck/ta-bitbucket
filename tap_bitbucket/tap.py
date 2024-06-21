"""bitbucket tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_bitbucket import streams


class TapBitbucket(Tap):
    """bitbucket tap class."""

    name = "tap-bitbucket"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The username to authenticate against the API service",
        ),
        th.Property(
            "auth_password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="App token to authenticate against the API service",
        ),
        th.Property(
            "workspaces",
            th.ArrayType(th.StringType),
            required=True,
            description="Workspace IDs to replicate",
        ),
        th.Property(
            "repositories",
            th.ArrayType(th.StringType),
            required=False,
            default=[],
            description="Respository IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.BitbucketStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.WorspaceStream(self),
            streams.RepositoryStream(self),
            streams.CommitStream(self),
            # streams.DeplymentStream(self),
        ]


if __name__ == "__main__":
    TapBitbucket.cli()
