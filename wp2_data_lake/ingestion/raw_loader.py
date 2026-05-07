import json
from pathlib import Path
from typing import Iterator

from loguru import logger


def _iter_jsonl(path: Path) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"Skipping malformed JSON at {path}:{lineno}")


def load_raw(
    raw_dir: Path,
    source_type: str = None,
    date_str: str = None,
) -> Iterator[dict]:
    """Yield every raw record from wp1 bronze storage.

    Args:
        raw_dir:     Path to data/raw/
        source_type: Restrict to a single source folder (e.g. "twitter").
        date_str:    Restrict to a single date file (e.g. "2026-04-20").
    """
    if source_type:
        candidate_dirs = [raw_dir / source_type]
    else:
        candidate_dirs = sorted(
            [d for d in raw_dir.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )

    for source_dir in candidate_dirs:
        if not source_dir.exists():
            logger.warning(f"Source directory not found: {source_dir}")
            continue
        for jsonl_file in sorted(source_dir.glob("*.jsonl")):
            if date_str and jsonl_file.stem != date_str:
                continue
            logger.debug(f"Loading {jsonl_file}")
            yield from _iter_jsonl(jsonl_file)
