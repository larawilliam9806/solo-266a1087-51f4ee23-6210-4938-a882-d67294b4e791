"""End-to-end tests for the shadow comparison gateway.

The gateway is driven through its real HTTP routes; the two search services are
replaced with an httpx MockTransport so latency and failures can be scripted
deterministically.
"""
import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gateway"))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402

DOCS = [
    "DOC-001", "DOC-002", "DOC-003", "DOC-004", "DOC-005",
    "DOC-006", "DOC-007", "DOC-008", "DOC-009", "DOC-010",
]

LEGACY_HOST = "http://legacy.test"
CANDIDATE_HOST = "http://candidate.test"


class FakeSearch:
    """Scriptable stand-in for both search deployments."""

    def __init__(self):
        self.candidate_delay = 0.0
        self.legacy_delay = 0.0
        self.candidate_override = None  # callable(query) -> list[str] | Exception
        self.legacy_override = None
        self.calls = []

    def _results(self, doc_ids):
        return [{"doc_id": d, "title": d, "score": 1.0} for d in doc_ids]

    async def handle(self, request: httpx.Request) -> httpx.Response:
        q = request.url.params.get("q", "")
        host = request.url.host
        self.calls.append((host, q, request.url.params.get("delay_ms")))
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        if host == "legacy.test":
            if self.legacy_delay:
                await asyncio.sleep(self.legacy_delay)
            if self.legacy_override is not None:
                outcome = self.legacy_override(q)
                if isinstance(outcome, Exception):
                    raise outcome
                return httpx.Response(200, json={"query": q, "results": self._results(outcome)})
            return httpx.Response(
                200, json={"query": q, "results": self._results(DOCS)}
            )
        # candidate
        if self.candidate_delay:
            await asyncio.sleep(self.candidate_delay)
        if self.candidate_override is not None:
            outcome = self.candidate_override(q)
            if isinstance(outcome, Exception):
                raise outcome
            return httpx.Response(200, json={"query": q, "results": self._results(outcome)})
        return httpx.Response(200, json={"query": q, "results": self._results(DOCS)})


@pytest.fixture
def fake():
    return FakeSearch()


@pytest.fixture
def app(monkeypatch):
    fake = FakeSearch()
    monkeypatch.setenv("LEGACY_SEARCH_URL", LEGACY_HOST)
    monkeypatch.setenv("CANDIDATE_SEARCH_URL", CANDIDATE_HOST)
    monkeypatch.setenv("SHADOW_DEFAULT_DELAY_MS", "0")
    monkeypatch.setenv("SHADOW_TIMEOUT_MS", "2000")
    monkeypatch.setenv("LEGACY_TIMEOUT_MS", "2000")
    transport = httpx.MockTransport(fake.handle)
    application = create_app(Settings.from_env(), transport=transport)
    application.state.fake = fake
    return application


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def wait_for(client, predicate, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = client.get("/api/state").json()
        if predicate(state):
            return state
        time.sleep(0.02)
    raise AssertionError("condition not reached in time")


def test_initial_round_is_open_and_not_switchable(client):
    state = client.get("/api/state").json()
    assert state["current_round"]["state"] == "open"
    assert state["current_round"]["request_count"] == 0
    assert state["current_round"]["can_switch"] is False
    assert state["max_inflight"] == 4


def test_search_returns_legacy_without_waiting_for_candidate(client, app):
    app.state.fake.candidate_delay = 1.5
    started = time.time()
    res = client.post("/api/search", json={"query": "部署"})
    elapsed = time.time() - started
    assert res.status_code == 200
    body = res.json()
    assert body["legacy"]["doc_ids"] == DOCS
    assert body["shadow"]["launched"] is True
    assert elapsed < 1.0, f"legacy response was blocked for {elapsed:.2f}s"

    state = wait_for(
        client, lambda s: s["current_round"]["counts"]["matched"] == 1
    )
    assert state["current_round"]["can_switch"] is True


def test_mismatch_is_reported_per_rank(client, app):
    reversed_ids = list(reversed(DOCS))
    app.state.fake.candidate_override = lambda q: reversed_ids
    client.post("/api/search", json={"query": "缓存"})
    state = wait_for(
        client, lambda s: s["current_round"]["counts"]["mismatch"] == 1
    )
    record = state["current_round"]["requests"][0]
    assert record["status"] == "mismatch"
    positions = record["comparison"]["positions"]
    assert len(positions) == 10
    assert positions[0]["legacy_doc_id"] == "DOC-001"
    assert positions[0]["candidate_doc_id"] == "DOC-010"
    assert all(not p["match"] for p in positions)
    assert state["current_round"]["can_switch"] is False


def test_cap_of_four_skips_extra_shadows(client, app):
    app.state.fake.candidate_delay = 0.6
    for i in range(7):
        res = client.post("/api/search", json={"query": f"查询{i}"})
        assert res.status_code == 200
        launched = res.json()["shadow"]["launched"]
        assert launched is (i < 4), f"request {i} launched={launched}"

    state = client.get("/api/state").json()
    assert state["inflight"] == 4
    counts = state["current_round"]["counts"]
    assert counts["skipped"] == 3
    assert counts["pending"] == 4

    state = wait_for(
        client,
        lambda s: s["current_round"]["counts"]["matched"] == 4
        and s["inflight"] == 0,
    )
    assert state["current_round"]["counts"]["skipped"] == 3
    assert state["current_round"]["can_switch"] is False


def test_sealed_round_rejects_new_submissions(client):
    rid = client.get("/api/state").json()["current_round_id"]
    sealed = client.post(f"/api/rounds/{rid}/seal").json()
    assert sealed["round"]["state"] == "sealed"
    assert sealed["new_round"]["id"] != rid
    res = client.post("/api/search", json={"query": "部署", "round_id": rid})
    assert res.status_code == 409


def test_late_response_cannot_change_sealed_round(client, app):
    app.state.fake.candidate_delay = 0.5
    rid = client.get("/api/state").json()["current_round_id"]
    res = client.post("/api/search", json={"query": "日志"}).json()
    assert res["shadow"]["status"] == "pending"

    sealed = client.post(f"/api/rounds/{rid}/seal").json()["round"]
    assert sealed["requests"][0]["status"] == "incomplete"
    assert sealed["can_switch"] is False

    def frozen_view(payload):
        return {
            key: payload[key]
            for key in (
                "sealed_at", "state", "requests", "counts", "can_switch", "blockers"
            )
        }

    frozen = json.dumps(frozen_view(sealed), sort_keys=True)

    time.sleep(1.0)  # let the candidate answer arrive late
    after = client.get(f"/api/rounds/{rid}").json()
    assert json.dumps(frozen_view(after), sort_keys=True) == frozen
    assert after["requests"][0]["shadow"]["candidate_doc_ids"] is None


def test_late_response_frees_slot_for_new_round(client, app):
    app.state.fake.candidate_delay = 0.5
    rid = client.get("/api/state").json()["current_round_id"]
    client.post("/api/search", json={"query": "a"})
    sealed = client.post(f"/api/rounds/{rid}/seal").json()
    new_round = sealed["new_round"]
    assert sealed["state"]["inflight"] == 1  # still occupied until it finishes

    # All four slots belong to the sealed round now; new round cannot launch.
    for _ in range(3):
        client.post("/api/search", json={"query": "b"})
    blocked = client.post("/api/search", json={"query": "c"}).json()
    assert blocked["shadow"]["launched"] is False
    assert blocked["shadow"]["status"] == "skipped"

    wait_for(client, lambda s: s["inflight"] == 0)
    launched = client.post("/api/search", json={"query": "d"}).json()
    assert launched["round_id"] == new_round["id"]
    assert launched["shadow"]["launched"] is True


def test_candidate_failure_marks_failed(client, app):
    app.state.fake.candidate_override = lambda q: httpx.ConnectError("boom")
    client.post("/api/search", json={"query": "故障"})
    state = wait_for(
        client, lambda s: s["current_round"]["counts"]["failed"] == 1
    )
    record = state["current_round"]["requests"][0]
    assert record["shadow"]["status"] == "failed"
    assert "无法连接" in record["shadow"]["error"]
    assert state["current_round"]["can_switch"] is False


def test_legacy_failure_is_recorded_and_no_shadow_launched(client, app):
    app.state.fake.legacy_override = lambda q: httpx.ConnectError("down")
    res = client.post("/api/search", json={"query": "坏查询"})
    assert res.status_code == 200
    body = res.json()
    assert body["legacy"]["error"]
    assert body["shadow"]["launched"] is False
    assert body["round"]["requests"][0]["status"] == "failed"
    assert app.state.fake.calls == [
        ("legacy.test", "坏查询", None)
    ]


def test_shadow_delay_is_forwarded_to_candidate(client, app):
    client.post("/api/shadow-delay", json={"delay_ms": 250})
    client.post("/api/search", json={"query": "延迟"})
    wait_for(client, lambda s: s["current_round"]["counts"]["matched"] == 1)
    candidate_calls = [c for c in app.state.fake.calls if c[0] == "candidate.test"]
    assert candidate_calls and candidate_calls[0][2] == "250"


def test_delay_validation(client):
    assert client.post("/api/shadow-delay", json={"delay_ms": -1}).status_code == 422
    assert client.post("/api/shadow-delay", json={"delay_ms": 60001}).status_code == 422


def test_rounds_are_listed_newest_first(client):
    first = client.get("/api/state").json()["current_round_id"]
    client.post("/api/rounds")
    rounds = client.get("/api/rounds").json()["rounds"]
    assert len(rounds) == 2
    assert rounds[0]["id"] != first
    assert rounds[-1]["id"] == first


def test_empty_query_rejected(client):
    assert client.post("/api/search", json={"query": "   "}).status_code == 422
    assert client.post("/api/search", json={"query": ""}).status_code == 422


def test_seal_requires_at_least_one_request_to_switch(client):
    rid = client.get("/api/state").json()["current_round_id"]
    sealed = client.post(f"/api/rounds/{rid}/seal").json()["round"]
    assert sealed["can_switch"] is False
    assert sealed["blockers"] == ["本轮尚无搜索请求，至少需要 1 条"]


def test_index_page_served(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "影子对照网关" in res.text


# --------------------------------------------------------------------------
# Sealing while legacy replies are still outstanding.
# --------------------------------------------------------------------------

def wait_for_record(client, round_id, predicate, timeout=3.0):
    """Poll a single round document until ``predicate(record)`` holds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = client.get(f"/api/rounds/{round_id}").json()
        records = payload.get("requests") or []
        if records and predicate(records[0]):
            return payload
        time.sleep(0.02)
    raise AssertionError("record condition not reached in time")


def search_in_background(client, query):
    return client.post("/api/search", json={"query": query})


def test_waiting_legacy_does_not_block_new_round(client, app):
    """A seal while legacy is outstanding must not stop new searches."""
    app.state.fake.legacy_delay = 0.8
    rid = client.get("/api/state").json()["current_round_id"]

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(search_in_background, client, "慢查询")
        # The submission is accepted (recorded as waiting) before legacy ends.
        sealed_view = wait_for_record(
            client, rid, lambda r: r["status"] == "waiting"
        )
        assert sealed_view["sealed_at"] is None

        # Another on-call operator seals the round mid-flight.
        sealed = client.post(f"/api/rounds/{rid}/seal").json()["round"]
        new_round = client.post("/api/rounds").json()  # can always open one
        assert new_round["round"]["id"] != rid

        # A fresh round accepts and completes searches immediately.
        app.state.fake.legacy_delay = 0.0
        fresh = client.post("/api/search", json={"query": "新查询"})
        assert fresh.status_code == 200
        assert fresh.json()["round_id"] == new_round["round"]["id"]
        assert fresh.json()["shadow"]["launched"] is True

        late = pending.result(timeout=5)

    # The late legacy reply is still returned to its submitter and tagged
    # with the round it belonged to.
    assert late.status_code == 200
    body = late.json()
    assert body["round_id"] == rid
    assert body["sealed"] is True
    assert body["legacy"]["doc_ids"] == DOCS
    assert body["shadow"]["launched"] is False

    # The sealed round kept it, marked incomplete, and the detail is frozen.
    after = client.get(f"/api/rounds/{rid}").json()
    assert after["sealed_at"] == sealed["sealed_at"]
    assert after["requests"][0]["status"] == "incomplete"
    assert after["requests"][0]["legacy"]["doc_ids"] == []
    assert after["requests"][0]["shadow"]["candidate_doc_ids"] is None

    # No shadow was sent for the sealed round, and the sealed round is never
    # switch-ready.
    candidates = [c for c in app.state.fake.calls if c[0] == "candidate.test"]
    assert [c[1] for c in candidates] == ["新查询"]
    assert after["can_switch"] is False


def test_waiting_legacy_blocks_switch_hint(client, app):
    """A round with an outstanding legacy reply must never say 可切换."""
    app.state.fake.legacy_delay = 1.0
    rid = client.get("/api/state").json()["current_round_id"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(search_in_background, client, "慢查询")
        state = wait_for_record(client, rid, lambda r: r["status"] == "waiting")
        assert state["can_switch"] is False
        assert state["waiting_legacy_count"] == 1
        assert "旧版" in " ".join(state["blockers"])

        # Even the candidate-side record must never look switchable while the
        # legacy reply is outstanding.
        summary = client.get("/api/rounds").json()["rounds"][0]
        assert summary["can_switch"] is False
        assert summary["switch_blocked_by_waiting"] is True

        pending.result(timeout=5)

    state = wait_for(
        client,
        lambda s: s["current_round"]["counts"]["matched"] == 1,
    )
    assert state["current_round"]["waiting_legacy_count"] == 0
    assert state["current_round"]["can_switch"] is True


def test_waiting_legacy_does_not_take_shadow_slot(client, app):
    """Requests waiting on legacy are not shadows and consume no slot."""
    app.state.fake.legacy_delay = 0.3
    app.state.fake.candidate_delay = 0.6

    # Four searches all occupy slots after their legacy replies return; the
    # fifth waits on legacy first.  While it waits, the shadow budget is full
    # only because of the four actual shadow calls.
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(search_in_background, client, "第五")
        wait_for_record(
            client,
            client.get("/api/state").json()["current_round_id"],
            lambda r: r["status"] == "waiting",
        )
        # A waiting request is not a shadow: in-flight stays 0.
        assert client.get("/api/state").json()["inflight"] == 0
        slow.result(timeout=5)

    state = wait_for(client, lambda s: s["current_round"]["request_count"] == 1)
    assert state["current_round"]["counts"]["pending"] == 1


def test_sealed_round_keeps_all_waiting_requests(client, app):
    """Every request outstanding at seal time stays in the round, incomplete."""
    app.state.fake.legacy_delay = 0.7
    rid = client.get("/api/state").json()["current_round_id"]
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(search_in_background, client, f"查询{i}") for i in range(3)]
        wait_for(
            client,
            lambda s: s["current_round"]["waiting_legacy_count"] == 3,
        )
        sealed = client.post(f"/api/rounds/{rid}/seal").json()["round"]

    assert sealed["request_count"] == 3
    assert sealed["counts"]["incomplete"] == 3
    assert sealed["can_switch"] is False

    # Sealed first, so an explicit submission to it is rejected.
    res = client.post("/api/search", json={"query": "再试", "round_id": rid})
    assert res.status_code == 409

    for future in futures:
        body = future.result(timeout=5).json()
        assert body["sealed"] is True
        assert body["round_id"] == rid
    after = client.get(f"/api/rounds/{rid}").json()
    assert after["requests"] == sealed["requests"]
    assert after["counts"]["incomplete"] == 3


def test_late_legacy_error_is_not_written_into_sealed_round(client, app):
    app.state.fake.legacy_delay = 0.5
    app.state.fake.legacy_override = lambda q: httpx.ConnectError("旧版挂了")
    rid = client.get("/api/state").json()["current_round_id"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search_in_background, client, "坏查询")
        wait_for_record(client, rid, lambda r: r["status"] == "waiting")
        client.post(f"/api/rounds/{rid}/seal")
        body = future.result(timeout=5).json()

    assert body["sealed"] is True
    assert body["legacy"]["error"]
    sealed = client.get(f"/api/rounds/{rid}").json()
    assert sealed["requests"][0]["status"] == "incomplete"
    assert sealed["requests"][0]["legacy"]["error"] is None


def test_sealed_round_never_gets_second_shadow_attempt(client, app):
    """A late legacy reply must not trigger a shadow after the round was sealed."""
    app.state.fake.legacy_delay = 0.5
    rid = client.get("/api/state").json()["current_round_id"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search_in_background, client, "延迟影子")
        wait_for_record(client, rid, lambda r: r["status"] == "waiting")
        client.post(f"/api/rounds/{rid}/seal")
        future.result(timeout=5)

    time.sleep(1.0)
    candidates = [c for c in app.state.fake.calls if c[0] == "candidate.test"]
    assert candidates == []
