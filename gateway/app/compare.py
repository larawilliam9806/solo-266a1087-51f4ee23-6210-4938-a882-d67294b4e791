"""Ordered doc-id comparison between the legacy and candidate result sets."""
from __future__ import annotations

from typing import List, Optional


def compare_doc_ids(
    legacy_doc_ids: List[str], candidate_doc_ids: List[str]
) -> dict:
    """Compare two ordered doc-id lists position by position.

    Returns an ``identical`` flag (true only for the exact same list, including
    length) plus a per-rank detail table used by the UI.
    """
    positions = []
    for index in range(max(len(legacy_doc_ids), len(candidate_doc_ids))):
        legacy_id: Optional[str] = (
            legacy_doc_ids[index] if index < len(legacy_doc_ids) else None
        )
        candidate_id: Optional[str] = (
            candidate_doc_ids[index] if index < len(candidate_doc_ids) else None
        )
        positions.append(
            {
                "rank": index + 1,
                "legacy_doc_id": legacy_id,
                "candidate_doc_id": candidate_id,
                "match": legacy_id is not None
                and candidate_id is not None
                and legacy_id == candidate_id,
            }
        )
    return {
        "identical": legacy_doc_ids == candidate_doc_ids,
        "positions": positions,
    }
