"""Freeze the complete live Python source tree for a new campaign."""

import hashlib
from pathlib import Path


CODE_ROOTS = (Path("src/vectorbench"), Path("scripts"))
INPUT_FILES = (
    Path("pyproject.toml"),
    Path("requirements/base.txt"),
    Path("requirements/orchestration.txt"),
    Path("requirements.txt"),
    Path("requirements-orchestration.txt"),
)
SYNTHETIC_PLAN = (Path("docs/synthetic-insertion/plan.md"), Path("docs/research/references.md"))


def source_files(extra=()):
    paths = {path for folder in CODE_ROOTS for path in folder.rglob("*.py")}
    paths.update(path for path in Path("configs").rglob("*") if path.is_file() and path.name != "README.md")
    paths.update(INPUT_FILES)
    paths.update(extra)
    return sorted(paths)


def source_hashes(extra=()):
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files(extra)}


def freeze_sources(root, extra=()):
    hashes = {}
    for path in source_files(extra):
        data = path.read_bytes()
        target = Path(root) / "source" / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        hashes[str(path)] = hashlib.sha256(data).hexdigest()
    return hashes
