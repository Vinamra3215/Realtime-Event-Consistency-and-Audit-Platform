"""Streaming checksums and honest environment metadata."""

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def environment():
    versions = {}
    for package in (
        "recap-team25",
        "SQLAlchemy",
        "psycopg",
        "confluent-kafka",
        "jsonschema",
        "PyYAML",
    ):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    commit, dirty = None, None
    try:
        root = Path(__file__).resolve().parents[2]
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.DEVNULL, text=True, timeout=5
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"],
                cwd=root,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            ).strip()
        )
    except (OSError, subprocess.SubprocessError):
        pass
    source_hashes = {}
    for path in sorted(Path(__file__).resolve().parent.rglob("*.py")):
        source_hashes[
            "src/recap/" + path.relative_to(Path(__file__).resolve().parent).as_posix()
        ] = sha256_file(path)
    from recap.resources import resource_directory

    for name in ("schemas", "migrations"):
        for path in sorted(resource_directory(name).iterdir()):
            if path.is_file():
                source_hashes[name + "/" + path.name] = sha256_file(path)
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "dependencies": versions,
        "commit": commit,
        "working_tree_dirty": dirty,
        "image_tag": os.environ.get("RECAP_IMAGE_TAG"),
        "schema_version": "1.0",
        "source_tree_sha256": stable_hash(source_hashes),
    }


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
