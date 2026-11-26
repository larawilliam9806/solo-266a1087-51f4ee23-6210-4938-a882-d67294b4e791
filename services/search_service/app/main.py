"""Local document search service.

Two instances of this app run in docker compose: the legacy build and the
candidate build.  Both share the same corpus; the `SEARCH_RANK_VARIANT` setting
selects the ranking algorithm so the two versions can disagree on ordering.

GET /search?q=<keyword>&limit=10
"""
import json
import math
import os
import time
from pathlib import Path
from typing import List

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

DATA_PATH = Path(__file__).parent / "data" / "documents.json"

VARIANT = os.getenv("SEARCH_RANK_VARIANT", "legacy").strip().lower()
DEFAULT_LIMIT = int(os.getenv("SEARCH_DEFAULT_LIMIT", "10"))
MAX_LIMIT = 50

# Optional artificial latency so the candidate build can be made slow on demand
# from the gateway UI.  A request may also override it per-call with ?delay_ms=.
BASE_DELAY_MS = int(os.getenv("SEARCH_DELAY_MS", "0"))
if not 0 <= BASE_DELAY_MS <= 60_000:
    raise RuntimeError("SEARCH_DELAY_MS must be between 0 and 60000")

with DATA_PATH.open(encoding="utf-8") as fh:
    DOCUMENTS: List[dict] = json.load(fh)

app = FastAPI(title=f"document search ({VARIANT})")


def _tokenize(text: str) -> List[str]:
    """Split on whitespace; CJK text is matched as substrings instead."""
    return [tok for tok in text.lower().split() if tok]


def _matched_fields(doc: dict, terms: List[str]) -> int:
    hay_title = doc["title"].lower()
    hay_body = doc["body"].lower()
    hay_tags = " ".join(doc["tags"]).lower()
    hay_all = f"{hay_title} {hay_body} {hay_tags}"
    for term in terms:
        if not any(term in h for h in (hay_title, hay_body, hay_tags, hay_all)):
            return 0
    return sum(
        1 for term in terms if term in hay_title
    ) + sum(1 for term in terms if term in hay_tags)


def _score_legacy(doc: dict, terms: List[str], matched: int) -> float:
    """Title-heavy, newest-first tie-break."""
    title_hits = sum(1 for t in terms if t in doc["title"].lower())
    tag_hits = sum(1 for t in terms if t in " ".join(doc["tags"]).lower())
    return title_hits * 100 + tag_hits * 10 + matched * 2 + _freshness(doc) * 0.5


def _score_candidate(doc: dict, terms: List[str], matched: int) -> float:
    """Body-weighted, shorter-title-first, different tie-break on recency."""
    body_hits = sum(1 for t in terms if t in doc["body"].lower())
    title_hits = sum(1 for t in terms if t in doc["title"].lower())
    tag_hits = sum(1 for t in terms if t in " ".join(doc["tags"]).lower())
    length_penalty = len(doc["title"]) * 0.3
    return (
        body_hits * 90
        + title_hits * 80
        + tag_hits * 12
        + matched * 2
        - length_penalty
        + _freshness(doc) * 0.3
    )


def _freshness(doc: dict) -> float:
    """Days since update, compressed so it stays a secondary signal."""
    try:
        y, m, d = (int(p) for p in doc["updated_at"].split("-"))
        days = (time.mktime((2026, 11, 26, 0, 0, 0, 0, 0, 0)) -
                time.mktime((y, m, d, 0, 0, 0, 0, 0, 0))) / 86400
    except (ValueError, KeyError):
        return 0.0
    return math.log1p(max(days, 0.0))


def search(query: str, limit: int) -> List[dict]:
    terms = _tokenize(query)
    if not terms:
        return []
    scored = []
    for doc in DOCUMENTS:
        matched = _matched_fields(doc, terms)
        if not matched:
            continue
        score = (
            _score_candidate(doc, terms, matched)
            if VARIANT == "candidate"
            else _score_legacy(doc, terms, matched)
        )
        scored.append((score, doc["doc_id"], doc))

    # Deterministic tie-break: higher score, then doc_id.
    if VARIANT == "candidate":
        scored.sort(key=lambda item: (-item[0], item[1]))
    else:
        scored.sort(key=lambda item: (-item[0], item[1]))

    return [
        {
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "score": round(score, 4),
        }
        for score, _doc_id, doc in scored[:limit]
    ]


@app.get("/health")
def health():
    return {"status": "ok", "variant": VARIANT}


@app.get("/search")
def search_endpoint(
    q: str = Query("", max_length=200),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    delay_ms: int = Query(0, ge=0, le=60_000),
):
    delay = (BASE_DELAY_MS + delay_ms) / 1000.0
    if delay:
        time.sleep(delay)
    results = search(q, limit)
    return JSONResponse(
        {
            "query": q,
            "variant": VARIANT,
            "count": len(results),
            "results": results,
        }
    )
