"""HTTP client used by the gateway to reach the two search deployments."""
from __future__ import annotations

from typing import Optional

import httpx

from .config import Settings
from .state import utcnow


class SearchBackendError(Exception):
    """Raised when a search service cannot be reached or answers badly."""


class HttpSearchBackend:
    """Talks to the legacy and candidate search services over HTTP."""

    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self._settings = settings
        self._client = client

    def _base_url(self, version: str) -> str:
        if version == "legacy":
            return self._settings.legacy_url
        if version == "candidate":
            return self._settings.candidate_url
        raise ValueError(f"unknown search version: {version}")

    async def search(
        self,
        version: str,
        query: str,
        limit: int,
        delay_ms: int = 0,
        timeout_s: Optional[float] = None,
    ) -> dict:
        base = self._base_url(version)
        params = {"q": query, "limit": limit}
        if version == "candidate" and delay_ms:
            params["delay_ms"] = delay_ms
        timeout = timeout_s or (
            self._settings.legacy_timeout_ms / 1000.0
            if version == "legacy"
            else self._settings.shadow_timeout_ms / 1000.0
        )
        try:
            response = await self._client.get(
                f"{base}/search", params=params, timeout=timeout
            )
        except httpx.TimeoutException as exc:
            raise SearchBackendError("请求超时") from exc
        except httpx.HTTPError as exc:
            raise SearchBackendError(f"无法连接搜索服务: {exc}") from exc

        if response.status_code != 200:
            raise SearchBackendError(
                f"搜索服务返回 HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SearchBackendError("搜索服务返回内容不是合法 JSON") from exc
        if not isinstance(payload, dict) or not isinstance(
            payload.get("results"), list
        ):
            raise SearchBackendError("搜索服务响应缺少 results 列表")
        return payload

    async def health(self) -> dict:
        out = {}
        for version in ("legacy", "candidate"):
            base = self._base_url(version)
            try:
                response = await self._client.get(
                    f"{base}/health", timeout=2.0
                )
                out[version] = {
                    "ok": response.status_code == 200,
                    "status_code": response.status_code,
                    "checked_at": utcnow(),
                }
            except httpx.HTTPError as exc:
                out[version] = {
                    "ok": False,
                    "error": str(exc),
                    "checked_at": utcnow(),
                }
        return out


def extract_doc_ids(payload: dict) -> list:
    """Pull ordered doc ids out of a search response, validating the shape."""
    results = payload.get("results")
    if not isinstance(results, list):
        raise SearchBackendError("搜索服务响应缺少 results 列表")
    doc_ids = []
    for item in results:
        if not isinstance(item, dict) or not isinstance(item.get("doc_id"), str):
            raise SearchBackendError("搜索结果项缺少 doc_id")
        doc_ids.append(item["doc_id"])
    return doc_ids
