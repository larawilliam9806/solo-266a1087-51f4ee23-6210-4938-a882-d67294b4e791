"""FastAPI shadow-comparison gateway.

Before switching the in-house document search to the new build, an on-call
operator submits the same queries to both versions.  The gateway answers the
browser with the legacy response straight away and mirrors ("shadows") the
query to the candidate build in the background.

* At most ``MAX_INFLIGHT_SHADOWS`` (4) shadow calls may be in flight at once
  across all rounds.  When the cap is reached a submission is recorded as
  *skipped* instead of launching a shadow call.
* A round can be sealed at any time.  Sealing freezes its results and (by
  default) opens a fresh round.  Late shadow responses for a sealed round are
  discarded and can never change the frozen comparison.
* A round is switch-ready only when it holds at least one request and every
  request matched.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .compare import compare_doc_ids
from .config import Settings
from .search_client import HttpSearchBackend, SearchBackendError, extract_doc_ids
from .state import (
    FAILED,
    MATCHED,
    MISMATCH,
    PENDING,
    GatewayState,
)

logger = logging.getLogger("shadow_gateway")

STATIC_DIR = Path(__file__).parent / "static"


class SearchSubmission(BaseModel):
    query: str = Field(..., min_length=1, max_length=200)
    round_id: Optional[str] = None


class DelayUpdate(BaseModel):
    delay_ms: int = Field(..., ge=0, le=60_000)


def create_app(
    settings: Optional[Settings] = None,
    transport: Optional[httpx.AsyncBaseTransport] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    state = GatewayState(
        max_inflight=settings.max_inflight,
        default_delay_ms=settings.default_delay_ms,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(transport=transport)
        app.state.http_client = client
        app.state.backend = HttpSearchBackend(settings, client)
        try:
            yield
        finally:
            # Cancel shadow calls still in flight so shutdown is clean.
            pending = []
            for round_obj in list(state.rounds.values()):
                pending.extend(round_obj.tasks)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            await client.aclose()

    app = FastAPI(title="Document Search Shadow Gateway", lifespan=lifespan)
    app.state.settings = settings
    app.state.gateway = state
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    def shadow_timeout_s(delay_ms: int) -> float:
        # The candidate may be deliberately slowed down, so the shadow call gets
        # the configured budget plus the requested delay.
        return (settings.shadow_timeout_ms + delay_ms) / 1000.0

    async def run_shadow(
        backend: HttpSearchBackend,
        round_id: str,
        request_id: str,
        query: str,
        limit: int,
        delay_ms: int,
        legacy_doc_ids: List[str],
    ) -> None:
        current_task = asyncio.current_task()
        status = FAILED
        candidate_doc_ids = None
        candidate_results = None
        detail = None
        error = None
        try:
            payload = await backend.search(
                "candidate",
                query,
                limit,
                delay_ms=delay_ms,
                timeout_s=shadow_timeout_s(delay_ms),
            )
            candidate_results = payload["results"]
            candidate_doc_ids = extract_doc_ids(payload)
            comparison = compare_doc_ids(legacy_doc_ids, candidate_doc_ids)
            detail = comparison["positions"]
            status = MATCHED if comparison["identical"] else MISMATCH
        except SearchBackendError as exc:
            status = FAILED
            error = str(exc)
        except asyncio.CancelledError:
            # Shutdown: release the slot, leave the record untouched.
            state.release_shadow(request_id)
            state.finish_task(round_id, current_task)
            raise
        except Exception:  # never let a background task die silently
            status = FAILED
            error = "影子请求出现未预期错误"
            logger.exception("unexpected shadow failure for %s", request_id)
        finally:
            applied = state.complete_shadow(
                round_id,
                request_id,
                status,
                candidate_doc_ids=candidate_doc_ids,
                candidate_results=candidate_results,
                detail=detail,
                error=error,
            )
            if not applied:
                logger.info(
                    "discarded late shadow response %s for round %s",
                    request_id,
                    round_id,
                )
            state.finish_task(round_id, current_task)

    # -------------------------------------------------------------- routes
    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))

    @app.get("/api/state")
    async def get_state():
        snapshot = state.state()
        snapshot["comparison_limit"] = settings.comparison_limit
        return snapshot

    @app.get("/api/rounds")
    async def get_rounds():
        return state.list_rounds()

    @app.get("/api/rounds/{round_id}")
    async def get_round(round_id: str):
        payload = state.get_round(round_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="轮次不存在")
        return payload

    @app.post("/api/rounds")
    async def open_new_round():
        """Open a new round, sealing the current open one first if necessary."""
        result = state.open_round()
        return {
            "round": result["round"],
            "sealed_round": result["sealed_round"],
            "state": state.state(),
        }

    @app.post("/api/rounds/{round_id}/seal")
    async def seal_round(round_id: str, open_new: bool = True):
        """Seal a round.  If it was the current round, open a new one."""
        result = state.seal_and_maybe_open(round_id, open_new)
        if result is None:
            raise HTTPException(status_code=404, detail="轮次不存在")
        return {
            "round": result["round"],
            "new_round": result["new_round"],
            "state": state.state(),
        }

    @app.post("/api/shadow-delay")
    async def set_shadow_delay(update: DelayUpdate):
        value = state.set_shadow_delay(update.delay_ms)
        return {"shadow_delay_ms": value}

    @app.get("/api/health")
    async def health(request: Request):
        backend: HttpSearchBackend = request.app.state.backend
        return {
            "gateway": {"status": "ok"},
            "backends": await backend.health(),
        }

    @app.post("/api/search")
    async def submit_search(submission: SearchSubmission, request: Request):
        """Answer with the legacy result immediately; shadow asynchronously."""
        query = submission.query.strip()
        if not query:
            raise HTTPException(status_code=422, detail="搜索词不能为空")

        round_obj = (
            state.rounds.get(submission.round_id)
            if submission.round_id
            else state.current_round()
        )
        if round_obj is None:
            raise HTTPException(status_code=404, detail="轮次不存在")
        if round_obj.sealed:
            raise HTTPException(
                status_code=409, detail="该轮次已封存，请开启新一轮后再提交"
            )

        backend: HttpSearchBackend = request.app.state.backend

        # 1) Legacy call, synchronously: the browser gets this response.
        legacy_doc_ids: Optional[List[str]] = None
        legacy_results = None
        legacy_error: Optional[str] = None
        try:
            payload = await backend.search(
                "legacy", query, settings.comparison_limit
            )
            legacy_results = payload["results"]
            legacy_doc_ids = extract_doc_ids(payload)
        except SearchBackendError as exc:
            legacy_error = str(exc)

        # 2) Record the request and reserve a shadow slot (atomic, under lock).
        delay_ms = state.snapshot_settings()["shadow_delay_ms"]
        record = state.begin_request(
            round_obj.id,
            query,
            settings.comparison_limit,
            legacy_doc_ids=legacy_doc_ids,
            legacy_results=legacy_results,
            legacy_error=legacy_error,
        )
        if record is None:
            raise HTTPException(
                status_code=409, detail="该轮次已封存，请开启新一轮后再提交"
            )

        # 3) Fire-and-forget shadow call when a slot was reserved.
        if record.shadow.status == PENDING and legacy_doc_ids is not None:
            try:
                task = asyncio.create_task(
                    run_shadow(
                        backend,
                        round_obj.id,
                        record.id,
                        query,
                        settings.comparison_limit,
                        delay_ms,
                        legacy_doc_ids,
                    )
                )
            except Exception:
                state.complete_shadow(
                    round_obj.id, record.id, FAILED, error="无法启动影子任务"
                )
            else:
                state.register_task(round_obj.id, task)

        round_payload = state.get_round(round_obj.id)
        return {
            "round_id": round_obj.id,
            "request_id": record.id,
            "seq": record.seq,
            "query": query,
            "legacy": {
                "doc_ids": legacy_doc_ids or [],
                "results": legacy_results or [],
                "error": legacy_error,
            },
            "shadow": {
                "status": record.shadow.status,
                "launched": record.shadow.status == PENDING,
            },
            "round": round_payload,
        }

    return app


app = create_app()
