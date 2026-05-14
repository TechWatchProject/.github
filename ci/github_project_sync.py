"""Sync GitHub issues and pull requests to a GitHub Projects v2 board.

Reads configuration from environment variables set by the
project_automation.yml workflow. Uses the GitHub GraphQL API via the
`gh` CLI to:

  - look up a project by owner + number
  - add the issue or PR to the project
  - set a Status single-select field (only on open/close/draft events —
    never overwrites a manual status change made on the board)
  - set a Date field on first-add only
  - map standard labels onto the project board's single-select fields:
      priority:p0..p3      -> Priority (P0/P1/P2/P3)
      severity:{c,h,m,l}*  -> Severity (Critical/High/Medium/Low)
      type:{bug,feature,task,docs,ops}    -> Issue Type
      goal:datalake-migration | extension-stability -> Goal

Requires a GitHub App token with project read/write scope.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

VALID_OWNER_TYPES = {"organization", "user"}

# Standard label -> (project board field name, option name on that field).
# Keep these in sync with ci/standard_labels.json (label names) and the
# board's single-select option names.
LABEL_TO_FIELD_OPTION: dict[str, tuple[str, str]] = {
    "priority:p0": ("Priority", "P0"),
    "priority:p1": ("Priority", "P1"),
    "priority:p2": ("Priority", "P2"),
    "priority:p3": ("Priority", "P3"),
    "severity:critical": ("Severity", "Critical"),
    "severity:high":     ("Severity", "High"),
    "severity:medium":   ("Severity", "Medium"),
    "severity:low":      ("Severity", "Low"),
    "type:bug":     ("Issue Type", "Bug"),
    "type:feature": ("Issue Type", "Feature"),
    "type:task":    ("Issue Type", "Task"),
    "type:docs":    ("Issue Type", "Docs"),
    "type:ops":     ("Issue Type", "Ops"),
    "goal:datalake-migration":  ("Goal", "Datalake migration"),
    "goal:extension-stability": ("Goal", "Extension stability"),
}


@dataclass
class FieldOption:
    field_id: str
    option_id: str


@dataclass
class Config:
    project_owner: str
    project_owner_type: str
    project_number: int
    status_field: str
    status_todo: str
    status_done: str
    status_draft: str
    date_field: str
    event_name: str
    event_action: str
    item_node_id: str
    is_draft: bool

    @classmethod
    def from_env(cls) -> "Config":
        project_number_raw = os.environ.get("PROJECT_NUMBER", "")
        if not project_number_raw:
            raise SystemExit("PROJECT_NUMBER is required")

        item_node_id = os.environ.get("ITEM_NODE_ID", "")
        if not item_node_id:
            raise SystemExit("ITEM_NODE_ID is required (no issue or PR node ID)")

        return cls(
            project_owner=os.environ.get("PROJECT_OWNER", ""),
            project_owner_type=os.environ.get("PROJECT_OWNER_TYPE", "organization"),
            project_number=int(project_number_raw),
            status_field=os.environ.get("STATUS_FIELD", "Status"),
            status_todo=os.environ.get("STATUS_TODO", "Triage"),
            status_done=os.environ.get("STATUS_DONE", "Done"),
            status_draft=os.environ.get("STATUS_DRAFT", ""),
            date_field=os.environ.get("DATE_FIELD", ""),
            event_name=os.environ.get("EVENT_NAME", ""),
            event_action=os.environ.get("EVENT_ACTION", ""),
            item_node_id=item_node_id,
            is_draft=os.environ.get("IS_DRAFT", "false").lower() == "true",
        )


def graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    cmd = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        cmd.extend(["-F", f"{key}={value}"])

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f"GraphQL error (stderr): {result.stderr}", file=sys.stderr)
        raise SystemExit(f"GraphQL request failed: {result.returncode}")

    data: dict[str, Any] = json.loads(result.stdout)
    if "errors" in data:
        print(
            f"GraphQL errors: {json.dumps(data['errors'], indent=2)}",
            file=sys.stderr,
        )
        raise SystemExit("GraphQL returned errors")
    return data


def find_project(owner: str, owner_type: str, number: int) -> str:
    if owner_type not in VALID_OWNER_TYPES:
        raise SystemExit(
            f"PROJECT_OWNER_TYPE must be 'organization' or 'user', got '{owner_type}'"
        )

    if owner_type == "organization":
        query = """
        query($owner: String!, $number: Int!) {
          organization(login: $owner) { projectV2(number: $number) { id } }
        }
        """
        data = graphql(query, {"owner": owner, "number": number})
        project = data["data"]["organization"]["projectV2"]
    else:
        query = """
        query($owner: String!, $number: Int!) {
          user(login: $owner) { projectV2(number: $number) { id } }
        }
        """
        data = graphql(query, {"owner": owner, "number": number})
        project = data["data"]["user"]["projectV2"]

    if not project:
        raise SystemExit(f"Project #{number} not found for {owner_type} '{owner}'")
    return project["id"]


def add_item_to_project(project_id: str, content_id: str) -> str:
    query = """
    mutation($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
        item { id }
      }
    }
    """
    data = graphql(query, {"projectId": project_id, "contentId": content_id})
    return data["data"]["addProjectV2ItemById"]["item"]["id"]


def fetch_all_select_fields(project_id: str) -> dict[str, dict[str, Any]]:
    """Return {field_name: {id, options: {option_name: option_id}}} for all
    single-select fields on the project. One query, used as a cache."""
    query = """
    query($projectId: ID!) {
      node(id: $projectId) {
        ... on ProjectV2 {
          fields(first: 50) {
            nodes {
              ... on ProjectV2SingleSelectField {
                id name
                options { id name }
              }
              ... on ProjectV2Field {
                id name dataType
              }
            }
          }
        }
      }
    }
    """
    data = graphql(query, {"projectId": project_id})
    out: dict[str, dict[str, Any]] = {}
    for field in data["data"]["node"]["fields"]["nodes"]:
        name = field.get("name")
        if not name:
            continue
        options = field.get("options")
        out[name] = {
            "id": field["id"],
            "data_type": field.get("dataType"),
            "options": (
                {opt["name"]: opt["id"] for opt in options}
                if options is not None
                else None
            ),
        }
    return out


def get_field_and_option(
    fields: dict[str, dict[str, Any]], field_name: str, option_name: str
) -> FieldOption:
    field = fields.get(field_name)
    if field is None:
        raise SystemExit(
            f"Field '{field_name}' not found. Available: {list(fields)}"
        )
    options = field.get("options") or {}
    if option_name not in options:
        raise SystemExit(
            f"Option '{option_name}' not found in field '{field_name}'. "
            f"Available: {list(options)}"
        )
    return FieldOption(field_id=field["id"], option_id=options[option_name])


def get_date_field_id(fields: dict[str, dict[str, Any]], field_name: str) -> str:
    field = fields.get(field_name)
    if field is None or field.get("data_type") != "DATE":
        raise SystemExit(f"Date field '{field_name}' not found in project")
    return field["id"]


def fetch_item_labels(node_id: str) -> list[str]:
    """Fetch labels of an issue or PR by its global node ID."""
    query = """
    query($id: ID!) {
      node(id: $id) {
        __typename
        ... on Issue { labels(first: 50) { nodes { name } } }
        ... on PullRequest { labels(first: 50) { nodes { name } } }
      }
    }
    """
    data = graphql(query, {"id": node_id})
    node = data["data"]["node"]
    if not node:
        return []
    label_nodes = node.get("labels", {}).get("nodes", []) if isinstance(node, dict) else []
    return [lab["name"] for lab in label_nodes if "name" in lab]


def update_single_select(
    project_id: str, item_id: str, field_id: str, option_id: str
) -> None:
    query = """
    mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId
        itemId: $itemId
        fieldId: $fieldId
        value: {singleSelectOptionId: $optionId}
      }) { projectV2Item { id } }
    }
    """
    graphql(
        query,
        {
            "projectId": project_id,
            "itemId": item_id,
            "fieldId": field_id,
            "optionId": option_id,
        },
    )


def update_date(
    project_id: str, item_id: str, field_id: str, date_value: str
) -> None:
    query = """
    mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $dateValue: Date!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId
        itemId: $itemId
        fieldId: $fieldId
        value: {date: $dateValue}
      }) { projectV2Item { id } }
    }
    """
    graphql(
        query,
        {
            "projectId": project_id,
            "itemId": item_id,
            "fieldId": field_id,
            "dateValue": date_value,
        },
    )


def determine_status(config: Config) -> Optional[str]:
    """Return the Status to set on this event, or None to leave it alone.

    Importantly we never set Status on label-only events (`labeled`,
    `unlabeled`, `edited`, etc.) — that would overwrite manual board moves.
    """
    if config.event_action == "closed":
        return config.status_done

    if (
        config.event_name == "pull_request_target"
        and config.is_draft
        and config.status_draft
        and config.event_action in ("opened", "reopened", "converted_to_draft")
    ):
        return config.status_draft

    if config.event_action in ("opened", "reopened", "ready_for_review"):
        return config.status_todo

    # labeled / unlabeled / edited / etc. — leave Status as-is on the board
    return None


def apply_label_fields(
    project_id: str,
    item_id: str,
    fields: dict[str, dict[str, Any]],
    labels: list[str],
) -> None:
    """For each known standard label on the issue/PR, set the corresponding
    project board single-select. First match per (field) wins, so if both
    `priority:p0` and `priority:p1` are applied, the first one we encounter
    is what lands on the board. Unknown labels are ignored. Missing project
    fields/options are warned and skipped (the project may not have all
    optional fields)."""
    applied_fields: set[str] = set()
    for label in labels:
        mapping = LABEL_TO_FIELD_OPTION.get(label)
        if mapping is None:
            continue
        field_name, option_name = mapping
        if field_name in applied_fields:
            continue  # first label of this kind wins
        field = fields.get(field_name)
        if field is None or field.get("options") is None:
            print(
                f"  skip label '{label}': project has no '{field_name}' "
                f"single-select field"
            )
            continue
        option_id = field["options"].get(option_name)
        if not option_id:
            print(
                f"  skip label '{label}': option '{option_name}' not present "
                f"on field '{field_name}'"
            )
            continue
        update_single_select(project_id, item_id, field["id"], option_id)
        applied_fields.add(field_name)
        print(f"  set {field_name} = {option_name} (from label '{label}')")


def main() -> None:
    config = Config.from_env()

    if not config.project_owner:
        raise SystemExit("PROJECT_OWNER is required")

    print(
        f"Event: {config.event_name}.{config.event_action} "
        f"(is_draft={config.is_draft})"
    )
    print(f"Looking up project: {config.project_owner}#{config.project_number}")
    project_id = find_project(
        config.project_owner, config.project_owner_type, config.project_number
    )
    print(f"Project ID: {project_id}")

    print(f"Adding item {config.item_node_id} to project")
    item_id = add_item_to_project(project_id, config.item_node_id)
    print(f"Project item ID: {item_id}")

    # One fetch of all single-select fields, reused for Status + label mapping.
    fields = fetch_all_select_fields(project_id)

    status_name = determine_status(config)
    if status_name:
        print(f"Setting status to: {status_name}")
        resolved = get_field_and_option(fields, config.status_field, status_name)
        update_single_select(
            project_id, item_id, resolved.field_id, resolved.option_id
        )
        print("Status updated")
    else:
        print(
            f"Skipping Status update for event "
            f"{config.event_name}.{config.event_action}"
        )

    if config.date_field and config.event_action in ("opened", "ready_for_review"):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        print(f"Setting {config.date_field} to {today}")
        date_field_id = get_date_field_id(fields, config.date_field)
        update_date(project_id, item_id, date_field_id, today)
        print("Date updated")

    # Label-driven field mapping. Runs on every event the workflow triggers,
    # so adding a `priority:p0` label after the fact updates the board.
    labels = fetch_item_labels(config.item_node_id)
    if labels:
        print(f"Mapping labels: {labels}")
        apply_label_fields(project_id, item_id, fields, labels)
    else:
        print("No labels on item; nothing to map")

    print("Done")


if __name__ == "__main__":
    main()
