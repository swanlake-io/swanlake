#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_TOML = ROOT / "Cargo.toml"
LOCKFILE = ROOT / "Cargo.lock"
ARROW_DEPENDENCIES = (
    "arrow",
    "arrow-array",
    "arrow-buffer",
    "arrow-schema",
    "arrow-flight",
    "arrow-ipc",
    "arrow-cast",
    "arrow-select",
)


def fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


def parse_major(version_req: str) -> int | None:
    match = re.search(r"(\d+)", version_req)
    if match is None:
        return None
    return int(match.group(1))


def dependency_version_requirement(dependency: Any) -> str | None:
    if isinstance(dependency, str):
        return dependency
    if isinstance(dependency, dict):
        version = dependency.get("version")
        if isinstance(version, str):
            return version
    return None


def duckdb_arrow_major(lock_data: dict[str, Any]) -> tuple[int, str] | None:
    packages = lock_data.get("package")
    if not isinstance(packages, list):
        return None

    arrow_versions: set[str] = set()
    for package in packages:
        if not isinstance(package, dict):
            continue
        if package.get("name") != "arrow":
            continue
        version = package.get("version")
        if isinstance(version, str):
            arrow_versions.add(version)

    for package in packages:
        if not isinstance(package, dict):
            continue
        if package.get("name") != "duckdb":
            continue
        dependencies = package.get("dependencies", [])
        if not isinstance(dependencies, list):
            continue
        for dependency in dependencies:
            if not isinstance(dependency, str):
                continue
            if dependency == "arrow":
                if len(arrow_versions) == 0:
                    return None
                if len(arrow_versions) > 1:
                    sorted_versions = sorted(arrow_versions)
                    print(
                        f"ERROR: multiple arrow versions in Cargo.lock: {', '.join(sorted_versions)}. "
                        "The workspace must resolve to exactly one arrow version.",
                        file=sys.stderr,
                    )
                    return None
                version = next(iter(arrow_versions))
                return int(version.split(".", maxsplit=1)[0]), f"arrow {version}"
            match = re.fullmatch(r"arrow (\d+)\.\d+\.\d+.*", dependency)
            if match is None:
                continue
            return int(match.group(1)), dependency
    return None


def main() -> int:
    workspace_data = tomllib.loads(WORKSPACE_TOML.read_text())
    lock_data = tomllib.loads(LOCKFILE.read_text())

    workspace_deps = workspace_data.get("workspace", {}).get("dependencies", {})
    if not isinstance(workspace_deps, dict):
        return fail("workspace.dependencies missing from Cargo.toml")

    duckdb_arrow = duckdb_arrow_major(lock_data)
    if duckdb_arrow is None:
        return fail(
            "unable to determine duckdb's resolved Arrow dependency from Cargo.lock"
        )

    expected_major, resolved_dependency = duckdb_arrow
    mismatches: list[str] = []

    for dependency_name in ARROW_DEPENDENCIES:
        dependency = workspace_deps.get(dependency_name)
        version_req = dependency_version_requirement(dependency)
        if version_req is None:
            mismatches.append(f"{dependency_name}: missing version requirement")
            continue

        major = parse_major(version_req)
        if major is None:
            mismatches.append(
                f"{dependency_name}: could not parse major version from {version_req!r}"
            )
            continue

        if major != expected_major:
            mismatches.append(
                f"{dependency_name}: expected Arrow major {expected_major}, found {version_req!r}"
            )

    if mismatches:
        print(
            "Arrow workspace dependencies must match the Arrow major resolved through duckdb.",
            file=sys.stderr,
        )
        print(f"duckdb resolved dependency: {resolved_dependency}", file=sys.stderr)
        for mismatch in mismatches:
            print(f"- {mismatch}", file=sys.stderr)
        return 1

    print(f"Arrow/DuckDB alignment OK ({resolved_dependency})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
