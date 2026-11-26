"""In-memory round/request state for the shadow comparison gateway.

Everything lives in one process (uvicorn is started with a single worker).
Mutations are guarded by a plain threading lock; all critical sections are
short and synchronous, so they never block the event loop for meaningful time.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


# Shadow request statuses:
#   pending    - shadow call is still in flight in an open round
#   matched    - candidate top-N doc ids equal legacy top-N doc ids, in order
#   mismatch   - candidate returned a different list / order
#   failed     - shadow or legacy call failed (timeout, transport, bad payload)
#   skipped    - not launched because the in-flight budget was exhausted
#   incomplete - still in flight when the round was sealed
PENDING = "pending"
MATCHED = "matched"
MISMATCH = "mismatch"
FAILED = "failed"
SKIPPED = "skipped"
INCOMPLETE = "incomplete"

STATUS_LABELS = {
    PENDING: "进行中",
    MATCHED: "一致",
    MISMATCH: "不一致",
    FAILED: "失败",
    SKIPPED: "跳过",
    INCOMPLETE: "未完成",
}

@dataclass
class ShadowOutcome:
    status: str = PENDING
    candidate_doc_ids: Optional[List[str]] = None
    candidate_results: Optional[List[dict]] = None
    detail: Optional[List[dict]] = None
    error: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class SearchRecord:
    id: str
    seq: int
    query: str
    submitted_at: str
    limit: int
    delay_ms: int
    legacy_doc_ids: Optional[List[str]] = None
    legacy_results: Optional[List[dict]] = None
    legacy_error: Optional[str] = None
    shadow: ShadowOutcome = field(default_factory=ShadowOutcome)

    @property
    def status(self) -> str:
        if self.legacy_error is not None:
            return FAILED
        return self.shadow.status

    def to_dict(self) -> dict:
        legacy_ok = self.legacy_error is None
        shadow = self.shadow
        comparison = None
        if legacy_ok and shadow.status in (MATCHED, MISMATCH):
            comparison = {
                "identical": shadow.status == MATCHED,
                "legacy_doc_ids": self.legacy_doc_ids or [],
                "candidate_doc_ids": shadow.candidate_doc_ids or [],
                "positions": shadow.detail or [],
            }
        return {
            "id": self.id,
            "seq": self.seq,
            "query": self.query,
            "submitted_at": self.submitted_at,
            "status": self.status,
            "status_label": STATUS_LABELS.get(self.status, self.status),
            "delay_ms": self.delay_ms,
            "legacy": {
                "ok": legacy_ok,
                "doc_ids": self.legacy_doc_ids or [],
                "results": self.legacy_results or [],
                "error": self.legacy_error,
            },
            "shadow": {
                "status": shadow.status,
                "status_label": STATUS_LABELS.get(shadow.status, shadow.status),
                "candidate_doc_ids": shadow.candidate_doc_ids,
                "candidate_results": shadow.candidate_results,
                "error": shadow.error,
                "completed_at": shadow.completed_at,
            },
            "comparison": comparison,
        }


@dataclass
class Round:
    id: str
    seq: int
    opened_at: str
    sealed_at: Optional[str] = None
    requests: List[SearchRecord] = field(default_factory=list)
    ready_to_switch: Optional[bool] = None
    # asyncio.Task handles are kept so they are not garbage collected.
    tasks: Set[object] = field(default_factory=set)

    @property
    def sealed(self) -> bool:
        return self.sealed_at is not None

    def can_switch(self) -> bool:
        """At least one request, and every recorded request matched."""
        return bool(self.requests) and all(
            record.status == MATCHED for record in self.requests
        )

    def pending_count(self) -> int:
        return sum(1 for record in self.requests if record.shadow.status == PENDING)


class GatewayState:
    def __init__(self, max_inflight: int, default_delay_ms: int) -> None:
        # Cap is global: all rounds together may never exceed this many live
        # shadow calls.  Calls issued by a sealed round still occupy a slot
        # until they actually finish, they just cannot change its results.
        self.max_inflight = max_inflight
        self.shadow_delay_ms = default_delay_ms
        self.lock = threading.Lock()
        self.rounds: Dict[str, Round] = {}
        self.round_order: List[str] = []
        self.current_round_id: Optional[str] = None
        self._inflight: Dict[str, str] = {}  # request_id -> round_id, global
        self._round_seq = 0
        self._request_seq = 0
        self.open_round()  # round 1 is ready for submissions immediately

    # ---------------------------------------------------------------- rounds
    def _new_round_locked(self) -> Round:
        self._round_seq += 1
        round_id = f"r{self._round_seq}-{uuid.uuid4().hex[:8]}"
        round_obj = Round(id=round_id, seq=self._round_seq, opened_at=utcnow())
        self.rounds[round_id] = round_obj
        self.round_order.append(round_id)
        self.current_round_id = round_id
        return round_obj

    def open_round(self) -> dict:
        """Seal the current open round (if any) and open a fresh one.

        Returns ``{"round": <new round>, "sealed_round": <summary or None>}``.
        """
        with self.lock:
            sealed_summary = None
            current = self.rounds.get(self.current_round_id or "")
            if current is not None and not current.sealed:
                self._seal_locked(current)
                sealed_summary = self._round_summary_locked(current)
            round_obj = self._new_round_locked()
            return {
                "round": self._round_dict_locked(round_obj),
                "sealed_round": sealed_summary,
            }

    def seal_round(self, round_id: str) -> Optional[dict]:
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is None:
                return None
            self._seal_locked(round_obj)
            return self._round_dict_locked(round_obj)

    def seal_and_maybe_open(
        self, round_id: str, open_new: bool
    ) -> Optional[dict]:
        """Seal ``round_id``; if it was the current round, optionally open one.

        Sealing and opening happen under one lock acquisition so a submission
        can never slip into a round that is being replaced.
        """
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is None:
                return None
            was_current = self.current_round_id == round_id
            self._seal_locked(round_obj)
            sealed = self._round_dict_locked(round_obj)
            new_round = None
            if open_new and was_current:
                new_round = self._round_dict_locked(self._new_round_locked())
            return {"round": sealed, "new_round": new_round}

    def _seal_locked(self, round_obj: Round) -> None:
        if round_obj.sealed:
            return
        sealed_at = utcnow()
        round_obj.sealed_at = sealed_at
        for record in round_obj.requests:
            if record.shadow.status == PENDING:
                record.shadow.status = INCOMPLETE
                record.shadow.completed_at = sealed_at
                record.shadow.error = "封存时影子请求仍在途，迟到响应已丢弃"
        round_obj.ready_to_switch = round_obj.can_switch()

    def current_round(self) -> Optional[Round]:
        with self.lock:
            return self.rounds.get(self.current_round_id or "")

    def get_round(self, round_id: str) -> Optional[dict]:
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is None:
                return None
            return self._round_dict_locked(round_obj)

    def list_rounds(self) -> dict:
        with self.lock:
            return {
                "current_round_id": self.current_round_id,
                "inflight": len(self._inflight),
                "max_inflight": self.max_inflight,
                "rounds": [
                    self._round_summary_locked(self.rounds[rid])
                    for rid in reversed(self.round_order)
                ],
            }

    def state(self) -> dict:
        with self.lock:
            round_obj = self.rounds.get(self.current_round_id or "")
            return {
                "current_round_id": self.current_round_id,
                "max_inflight": self.max_inflight,
                "inflight": len(self._inflight),
                "shadow_delay_ms": self.shadow_delay_ms,
                "current_round": (
                    self._round_dict_locked(round_obj) if round_obj else None
                ),
                "rounds": [
                    self._round_summary_locked(self.rounds[rid])
                    for rid in reversed(self.round_order)
                ],
            }

    # ------------------------------------------------------------- settings
    def set_shadow_delay(self, delay_ms: int) -> int:
        with self.lock:
            self.shadow_delay_ms = delay_ms
            return self.shadow_delay_ms

    def snapshot_settings(self) -> dict:
        with self.lock:
            return {
                "max_inflight": self.max_inflight,
                "inflight": len(self._inflight),
                "shadow_delay_ms": self.shadow_delay_ms,
            }

    # ------------------------------------------------------------- requests
    def begin_request(
        self,
        round_id: str,
        query: str,
        limit: int,
        legacy_doc_ids: Optional[List[str]] = None,
        legacy_results: Optional[List[dict]] = None,
        legacy_error: Optional[str] = None,
    ) -> Optional[SearchRecord]:
        """Append a record to the round and try to reserve a shadow slot.

        Returns the record when it was recorded, and ``None`` when the round is
        unknown or already sealed (the submission must be rejected).
        """
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is None or round_obj.sealed:
                return None
            self._request_seq += 1
            record = SearchRecord(
                id=f"{round_id}-q{self._request_seq}",
                seq=len(round_obj.requests) + 1,
                query=query,
                submitted_at=utcnow(),
                limit=limit,
                delay_ms=self.shadow_delay_ms,
                legacy_doc_ids=legacy_doc_ids,
                legacy_results=legacy_results,
                legacy_error=legacy_error,
            )
            round_obj.requests.append(record)

            if legacy_error is not None:
                record.shadow.status = FAILED
                record.shadow.error = "旧版请求失败，未发起影子请求"
                record.shadow.completed_at = utcnow()
                return record

            if len(self._inflight) >= self.max_inflight:
                record.shadow.status = SKIPPED
                record.shadow.error = (
                    f"在途影子请求已达上限 {self.max_inflight}，本次跳过"
                )
                record.shadow.completed_at = utcnow()
                return record

            self._inflight[record.id] = round_id
            return record

    def register_task(self, round_id: str, task: object) -> None:
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is not None:
                round_obj.tasks.add(task)

    def finish_task(self, round_id: str, task: object) -> None:
        with self.lock:
            round_obj = self.rounds.get(round_id)
            if round_obj is not None:
                round_obj.tasks.discard(task)

    def release_shadow(self, request_id: str) -> None:
        with self.lock:
            self._inflight.pop(request_id, None)

    def complete_shadow(
        self,
        round_id: str,
        request_id: str,
        status: str,
        candidate_doc_ids: Optional[List[str]] = None,
        candidate_results: Optional[List[dict]] = None,
        detail: Optional[List[dict]] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Record a shadow result unless the round has been sealed.

        The in-flight slot is always released.  Returns ``True`` when the
        result was applied, ``False`` when it arrived late and was discarded.
        """
        with self.lock:
            self._inflight.pop(request_id, None)
            round_obj = self.rounds.get(round_id)
            if round_obj is None:
                return False
            if round_obj.sealed:
                return False
            record = next(
                (item for item in round_obj.requests if item.id == request_id), None
            )
            if record is None or record.shadow.status != PENDING:
                return False
            record.shadow.status = status
            record.shadow.candidate_doc_ids = candidate_doc_ids
            record.shadow.candidate_results = candidate_results
            record.shadow.detail = detail
            record.shadow.error = error
            record.shadow.completed_at = utcnow()
            return True

    # ---------------------------------------------------------- serializing
    def _round_summary_locked(self, round_obj: Round) -> dict:
        counts: Dict[str, int] = {status: 0 for status in STATUS_LABELS}
        for record in round_obj.requests:
            counts[record.status] = counts.get(record.status, 0) + 1
        return {
            "id": round_obj.id,
            "seq": round_obj.seq,
            "opened_at": round_obj.opened_at,
            "sealed_at": round_obj.sealed_at,
            "state": "sealed" if round_obj.sealed else "open",
            "is_current": round_obj.id == self.current_round_id,
            "request_count": len(round_obj.requests),
            "pending_count": round_obj.pending_count(),
            "counts": counts,
            "can_switch": (
                round_obj.ready_to_switch
                if round_obj.sealed
                else round_obj.can_switch()
            ),
        }

    def _round_dict_locked(self, round_obj: Round) -> dict:
        payload = self._round_summary_locked(round_obj)
        records = sorted(round_obj.requests, key=lambda item: item.seq)

        blockers: List[str] = []
        if not records:
            blockers.append("本轮尚无搜索请求，至少需要 1 条")
        for record in records:
            if record.status != MATCHED:
                blockers.append(
                    f"第 {record.seq} 条「{record.query}」"
                    f"{STATUS_LABELS.get(record.status, record.status)}"
                )

        payload.update(
            {
                "request_count": len(records),
                "blockers": blockers,
                "requests": [record.to_dict() for record in records],
            }
        )
        if round_obj.sealed:
            payload["can_switch"] = round_obj.ready_to_switch
        return payload
