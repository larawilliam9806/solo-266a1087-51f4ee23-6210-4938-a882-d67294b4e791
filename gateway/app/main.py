"""FastAPI shadow-comparison gateway.

Before switching the in-house document search to the new build, an on-call
operator submits the same queries to both versions.  The gateway answers the
browser with the legacy response straight away and mirrors ("shadows") the
query to the candidate build in the background.

* At most ``MAX_INFLIGHT_SHADOWS`` (4) shadow calls may be in flight at once
  across all rounds.  When the cap is reached a submission is recorded as
  *skipped* instead of launching a shadow call.
* A round can be sealed at any time, even while legacy replies are still
  outstanding.  Sealing freezes its results and (by default) opens a fresh
  round, which accepts searches immediately.  Requests that were waiting or
  shadowing at seal time are kept in the sealed round and marked *incomplete*.
  Their legacy replies are still returned to the submitting browser (tagged
  with the owning round) but never written into the frozen round, and no
  shadow is launched for them.
* A submission is recorded in its round *before* the legacy call is made, so a
  legacy reply that is still outstanding can never lose its place.  Such a
  request is *waiting* and does not occupy a shadow slot; the slot is only
  reserved once the legacy reply comes back and a shadow is actually launched.
* A round is switch-ready only when it holds at least one request, nothing is
  still waiting on the legacy reply, and every request matched.
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
            # The round may have been sealed between reserving the slot and
            # this task starting; never shadow a sealed round.
            if not state.shadow_is_live(round_id, request_id):
                state.release_shadow(request_id)
                return
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
        """Seal the current round if it is still open, then open a new one.

        Safe to call while legacy replies are outstanding: those requests stay
        in the sealed round as *incomplete* and the new round is usable at once.
        """
        result = state.seal_then_open()
        return {
            "round": result["new_round"],
            "sealed_round": result["round"],
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
        """Answer with the legacy result; shadow asynchronously.

        The request is recorded in its round *before* the legacy call, so a
        seal that happens while the legacy reply is outstanding keeps the
        request in that round instead of dropping it.  Such a request does not
        take a shadow slot; if the round is sealed before the legacy reply
        arrives, the reply is still returned to this caller with the owning
        round id, the seal is left untouched and no shadow is launched.
        """
        query = submission.query.strip()
        if not query:
            raise HTTPException(status_code=422, detail="搜索词不能为空")

        if submission.round_id:
            existing = state.lookup(submission.round_id)
            if existing is None:
                raise HTTPException(status_code=404, detail="轮次不存在")
            if existing == "sealed":
                raise HTTPException(
                    status_code=409, detail="该轮次已封存，请开启新一轮后再提交"
                )

        backend: HttpSearchBackend = request.app.state.backend

        # 1) Record the submission (waiting on legacy, no slot yet).  This is
        #    the single atomic acceptance point: without an explicit round the
        #    current round is chosen under the same lock, so a concurrent seal
        #    can never drop or misroute the request.
        delay_ms = state.snapshot_settings()["shadow_delay_ms"]
        record = state.accept_request(
            submission.round_id, query, settings.comparison_limit, delay_ms
        )
        if record is None:
            raise HTTPException(
                status_code=409, detail="该轮次已封存，请开启新一轮后再提交"
            )
        round_obj = state.rounds[record.round_id]

        # 2) Legacy call, synchronously: the caller gets this response.
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
        except asyncio.CancelledError:
            # Client gone / shutdown: leave the round consistent rather than
            # stuck in ``waiting``, which would block the switch hint.
            state.resolve_legacy(
                round_obj.id, record.id, legacy_error="旧版请求被取消"
            )
            raise
        except Exception:
            # Never leave the record stuck in ``waiting`` (which would block
            # the round's switch hint forever) on an unexpected failure.
            legacy_error = "旧版请求出现未预期错误"
            logger.exception("unexpected legacy failure for %s", record.id)

        # 3) Write the legacy reply back and reserve a shadow slot.  ``None``
        #    means the round was sealed while the legacy call was in flight:
        #    the round keeps the request as incomplete, nothing is written to
        #    it, and no shadow is launched.
        applied = state.resolve_legacy(
            round_obj.id,
            record.id,
            legacy_doc_ids=legacy_doc_ids,
            legacy_results=legacy_results,
            legacy_error=legacy_error,
            delay_ms=delay_ms,
        )
        sealed_before_reply = applied is None
        shadow_status = (
            state.get_record_status(round_obj.id, record.id)
            if sealed_before_reply
            else applied
        )

        # 4) Fire-and-forget shadow call when a slot was reserved.
        if (
            not sealed_before_reply
            and applied == PENDING
            and legacy_doc_ids is not None
        ):
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
                shadow_status = FAILED
            else:
                state.register_task(round_obj.id, task)

        round_payload = state.get_round(round_obj.id)
        return {
            "round_id": round_obj.id,
            "request_id": record.id,
            "seq": record.seq,
            "query": query,
            "sealed": sealed_before_reply,
            "legacy": {
                "doc_ids": legacy_doc_ids or [],
                "results": legacy_results or [],
                "error": legacy_error,
            },
            "shadow": {
                "status": shadow_status,
                "launched": (not sealed_before_reply) and applied == PENDING,
                "reason": (
                    "提交期间本轮被其他值班员封存；结果仍返回给你，"
                    "本轮明细不再更新，也不会再发影子请求"
                    if sealed_before_reply
                    else None
                ),
            },
            "round": round_payload,
        }

    return app


app = create_app()
