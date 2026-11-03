"""Canonical hashing, so key order and whitespace never create a "changed" record."""

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(obj: Any) -> bytes:
    """sha256 of the canonical JSON of `obj` (32 bytes)."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).digest()
