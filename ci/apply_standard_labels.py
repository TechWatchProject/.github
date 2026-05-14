"""Apply the org's standard label taxonomy to every non-archived repo.

Reads `ci/standard_labels.json` (alongside this script) and, for each repo
in the org:

  - creates any missing standard label
  - updates color/description on labels whose name matches but properties
    differ
  - leaves all other (non-standard) labels alone — never deletes user labels

Idempotent. Safe to re-run.

Env vars:
  GH_TOKEN         Token with `repo` scope (PAT) OR a GitHub App installation
                   token with `metadata: read` + `issues: write` on every
                   repo it should touch.
  ORG              Org login (default: "TechWatchProject").
  EXCLUDE_REPOS    Comma-separated repo names to skip (default: "").
  DRY_RUN          "true" to log intended changes without applying.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def run_gh(args: list[str], check: bool = True) -> tuple[int, str, str]:
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=False,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        print(f"FAIL: gh {' '.join(args)}: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.returncode, result.stdout or "", result.stderr or ""


def load_standard_labels() -> list[dict[str, str]]:
    spec_path = Path(__file__).parent / "standard_labels.json"
    with spec_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return [
        {
            "name": entry["name"],
            "color": entry["color"],
            "description": entry.get("description", ""),
        }
        for entry in data["labels"]
    ]


def list_repos(org: str) -> list[dict[str, Any]]:
    _, out, _ = run_gh(
        [
            "repo",
            "list",
            org,
            "--limit",
            "300",
            "--no-archived",
            "--json",
            "name,isArchived",
        ]
    )
    return json.loads(out)  # type: ignore[no-any-return]


def list_labels(org: str, repo: str) -> list[dict[str, Any]]:
    code, out, _ = run_gh(
        [
            "api",
            "--paginate",
            f"repos/{org}/{repo}/labels?per_page=100",
        ],
        check=False,
    )
    if code != 0:
        return []
    # When --paginate concatenates pages, gh returns a JSON array fine.
    try:
        return json.loads(out)  # type: ignore[no-any-return]
    except json.JSONDecodeError:
        # Fallback for paginated output that isn't a single array
        labels: list[dict[str, Any]] = []
        for chunk in out.replace("][", "],[").split("\n"):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                parsed = json.loads(chunk)
                if isinstance(parsed, list):
                    labels.extend(parsed)
            except json.JSONDecodeError:
                continue
        return labels


def create_label(org: str, repo: str, label: dict[str, str], dry_run: bool) -> bool:
    if dry_run:
        print(f"    [dry-run] would CREATE {label['name']}")
        return True
    code, _, err = run_gh(
        [
            "api",
            "-X",
            "POST",
            f"repos/{org}/{repo}/labels",
            "-f",
            f"name={label['name']}",
            "-f",
            f"color={label['color']}",
            "-f",
            f"description={label['description']}",
        ],
        check=False,
    )
    if code != 0:
        print(f"    ! CREATE failed for {label['name']}: {err.strip()}", file=sys.stderr)
        return False
    print(f"    + created {label['name']}")
    return True


def update_label(
    org: str, repo: str, label: dict[str, str], dry_run: bool
) -> bool:
    if dry_run:
        print(f"    [dry-run] would UPDATE {label['name']}")
        return True
    code, _, err = run_gh(
        [
            "api",
            "-X",
            "PATCH",
            f"repos/{org}/{repo}/labels/{label['name']}",
            "-f",
            f"new_name={label['name']}",
            "-f",
            f"color={label['color']}",
            "-f",
            f"description={label['description']}",
        ],
        check=False,
    )
    if code != 0:
        print(f"    ! UPDATE failed for {label['name']}: {err.strip()}", file=sys.stderr)
        return False
    print(f"    ~ updated {label['name']}")
    return True


def reconcile(
    org: str,
    repo: str,
    standard: list[dict[str, str]],
    dry_run: bool,
) -> tuple[int, int, int]:
    existing = list_labels(org, repo)
    by_name = {lab["name"]: lab for lab in existing}

    created = 0
    updated = 0
    untouched = 0

    for spec in standard:
        live = by_name.get(spec["name"])
        if live is None:
            if create_label(org, repo, spec, dry_run):
                created += 1
        elif live.get("color", "").lower() != spec["color"].lower() or (
            (live.get("description") or "") != spec["description"]
        ):
            if update_label(org, repo, spec, dry_run):
                updated += 1
        else:
            untouched += 1

    return created, updated, untouched


def main() -> int:
    org = os.environ.get("ORG", "TechWatchProject").strip() or "TechWatchProject"
    exclude_raw = os.environ.get("EXCLUDE_REPOS", "").strip()
    dry_run = os.environ.get("DRY_RUN", "false").strip().lower() == "true"

    excluded = {r.strip() for r in exclude_raw.split(",") if r.strip()}

    standard = load_standard_labels()
    print(f"Loaded {len(standard)} standard labels:")
    for lab in standard:
        print(f"  {lab['name']}  ({lab['color']})")

    repos = list_repos(org)
    print(f"\nFound {len(repos)} non-archived repos in {org}.\n")

    total_created = 0
    total_updated = 0
    total_untouched = 0
    failed_repos: list[str] = []

    for entry in sorted(repos, key=lambda r: r["name"].lower()):
        name = entry["name"]
        if name in excluded:
            print(f"=== {name}: SKIPPED (excluded) ===")
            continue
        print(f"=== {name} ===")
        try:
            c, u, t = reconcile(org, name, standard, dry_run)
            total_created += c
            total_updated += u
            total_untouched += t
            print(f"    create={c}  update={u}  unchanged={t}")
        except Exception as exc:  # noqa: BLE001
            print(f"    FAILED: {exc}", file=sys.stderr)
            failed_repos.append(name)

    print(
        f"\nDone. Total: create={total_created}, "
        f"update={total_updated}, unchanged={total_untouched}, "
        f"failed_repos={len(failed_repos)}"
    )
    if failed_repos:
        for r in failed_repos:
            print(f"  - {r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
