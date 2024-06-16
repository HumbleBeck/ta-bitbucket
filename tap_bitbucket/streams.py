"""Stream type classes for tap-bitbucket."""

from __future__ import annotations

from typing import Optional, List, Any, ClassVar, Dict


from tap_bitbucket.client import BitbucketStream

import importlib.resources as importlib_resources


SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


class WorspaceStream(BitbucketStream):
    """Define custom stream."""

    name = "workspaces"
    path = "/workspaces"
    primary_keys: ClassVar[list[str]] = ["slug", "uuid"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "workspaces.json"

    @property
    def partitions(self) -> Optional[List[Dict]]:
        return [{"workspace": workspace} for workspace in self.config["workspaces"]]

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "workspace_id": record["slug"],
        }

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401s
    ) -> dict[str, Any]:
        params: dict = super().get_url_params(context, next_page_token)
        if workspaces := self.config.get("workspaces"):
            slug_filter = " OR ".join(
                [f'slug = "{workspace}"' for workspace in workspaces]
            )
            params["q"] = " AND ".join(
                filter(None, [params.get("q"), f"({slug_filter})"])
            )

        return params


class RepositoryStream(BitbucketStream):
    """Define custom stream."""

    name = "repositories"
    path = "/repositories/{workspace_id}"
    primary_keys: ClassVar[list[str]] = ["uuid"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "repositories.json"  # noqa: ERA001

    parent_stream_type = WorspaceStream

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "repository_id": record["full_name"],
        }

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401s
    ) -> dict[str, Any]:
        params: dict = super().get_url_params(context, next_page_token)
        if repositories := self.config.get("repositories"):
            name_filter = " OR ".join(
                [f'full_name = "{repository}"' for repository in repositories]
            )
            params["q"] = " AND ".join(
                filter(None, [params.get("q"), f"({name_filter})"])
            )

        return params


class CommitStream(BitbucketStream):
    """Define custom stream."""

    name = "commits"
    path = "/repositories/{repository_id}/commits"
    primary_keys: ClassVar[list[str]] = ["hash"]
    replication_key = "date"
    is_sorted = False

    schema_filepath = SCHEMAS_DIR / "commits.json"  # noqa: ERA001

    parent_stream_type = RepositoryStream


class DeplymentStream(BitbucketStream):
    """Define custom stream."""

    name = "deployments"
    path = "/repositories/{repository_id}/deployments"
    primary_keys: ClassVar[list[str]] = ["uuid"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "deployments.json"  # noqa: ERA001

    parent_stream_type = RepositoryStream
