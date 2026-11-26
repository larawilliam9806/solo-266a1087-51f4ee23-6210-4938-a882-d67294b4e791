# 站内文档搜索 · 影子对照网关

换版前的值班核对工具：同一个搜索词同时打给**旧版**和**新版**搜索服务。
浏览器提交后**立刻拿到旧版响应**，新版请求在后台以「影子」方式发出，结果只用于逐条比对。

```
浏览器 ──► gateway (FastAPI, :8000)
              ├─► legacy-search    :8000  旧版排序，即时应答
              └─► candidate-search :8000  新版排序，可调延迟，影子调用
```

## 启动

```bash
docker compose up --build
```

打开 <http://localhost:8000/>。三个服务：

| 服务 | 宿主端口 | 说明 |
| --- | --- | --- |
| `gateway` | 8000 | 对照网关 + 网页 |
| `legacy-search` | 8101 | 旧版排序算法 |
| `candidate-search` | 8102 | 新版排序算法（compose 中基准延迟 600ms） |

新版延迟可以在网关上随时调整（0–60000ms），对**之后发起**的影子请求生效；
compose 里的 `SEARCH_DELAY_MS` 只是新版服务的基准延迟，网关的调节值会叠加在它之上。

## 使用流程

1. 页面打开时已自动开启第 1 轮；点「开启一轮对照」可随时换成新一轮（旧轮自动封存）。
2. 输入搜索词提交 —— 页面立即显示旧版返回的文档编号顺序，新版在后台比对。
3. 每条请求的状态：**等待旧版 / 进行中 / 一致 / 不一致 / 失败 / 跳过 / 未完成**。
   前 10 个文档编号按排名逐位并排展示，不一致的行标红。
4. 「封存本轮并另开一轮」把当前轮结果冻结，并立即开启新一轮。
   封存时仍在等待旧版或仍在跑影子请求的搜索记为**未完成**，留在原轮；
   之后到达的旧版/新版响应不会改动已封存明细。若旧版响应还没回来，
   它仍会返回给发起提交的浏览器，并在提交区注明所属轮次。
5. 当本轮**至少 1 条请求且全部一致**（且没有请求还在等旧版）时，顶部提示「可以切换」。

## 规则实现

- **在途上限 4**：所有轮次合计最多 4 个在途影子请求（`MAX_INFLIGHT_SHADOWS`）。
  已达上限时新提交照常拿到旧版响应，但影子请求被跳过，记为**跳过**并计入本轮。
  封存轮次的在途影子请求仍占用名额，直到它们真正结束（结束后才释放）。
- **等待旧版不计名额**：提交先记入本轮（状态**等待旧版**），旧版返回后才决定是否
  发影子请求；等待期间不占用 4 个名额。因此封存并开启新轮后，新轮可立即搜索。
- **封存与在途**：旧版响应还没回来时也可以封存并开启新轮。这些搜索留在原轮并标为
  **未完成**；旧版结果/错误仍返回给原提交方（响应含所属 `round_id` 与 `sealed: true`），
  但不会写入已封存明细，也不会再为它们发起影子请求（包括影子名额不足时）。
- **迟到响应**：影子结果写回前检查轮次是否已封存；封存后一律丢弃，只释放名额。
- **切换判定**：本轮至少 1 条请求、全部一致，且没有请求仍在等待旧版；封存轮按封存
  时刻的结果固定判定。`null`/空轮次永远不可切换。
- **请求与阈值**：比对固定取前 `COMPARISON_LIMIT`（默认 10）条；超过长度的列表也会逐位标出差异。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页 |
| GET | `/api/state` | 当前轮 + 全部轮次摘要 + 在途数 |
| GET | `/api/rounds` | 轮次摘要列表（新→旧） |
| GET | `/api/rounds/{id}` | 单轮完整明细 |
| POST | `/api/rounds` | 封存当前轮（如有）并开启新一轮 |
| POST | `/api/rounds/{id}/seal` | 封存指定轮次；若为当前轮则同时开新轮（`?open_new=false` 可关闭） |
| POST | `/api/shadow-delay` | `{"delay_ms": 0..60000}` |
| POST | `/api/search` | `{"query": "...", "round_id": "可选"}`，立即返回旧版结果 |
| GET | `/api/health` | 网关与两版搜索服务的连通性 |

`POST /api/search` 响应示例：

```json
{
  "round_id": "r1-79f905a9",
  "seq": 1,
  "sealed": false,
  "legacy": { "doc_ids": ["DOC-002", "DOC-001"], "results": [], "error": null },
  "shadow": { "status": "pending", "launched": true, "reason": null }
}
```

向已封存轮次提交返回 `409`。如果本轮在旧版响应返回前被其他值班员封存，
响应仍为 `200`，`sealed` 为 `true`、`round_id` 是原轮次，`shadow.launched`
为 `false`，`shadow.reason` 说明原因；原轮该条保持**未完成**。

## 本地开发 / 测试

```bash
pip install -r gateway/requirements.txt fastapi 'uvicorn[standard]' httpx pytest
pytest tests -q
```

不依赖 Docker 直接跑（三个终端）：

```bash
cd services/search_service && SEARCH_RANK_VARIANT=legacy    uvicorn app.main:app --port 8101
cd services/search_service && SEARCH_RANK_VARIANT=candidate SEARCH_DELAY_MS=600 uvicorn app.main:app --port 8102
cd gateway && LEGACY_SEARCH_URL=http://127.0.0.1:8101 CANDIDATE_SEARCH_URL=http://127.0.0.1:8102 uvicorn app.main:app --port 8000
```

## 说明

- 状态保存在网关进程内存里，重启即清空；`uvicorn` 必须以单 worker 运行（Dockerfile 已固定）。
- 两版搜索服务共用 `services/search_service/app/data/documents.json`，
  排序算法由 `SEARCH_RANK_VARIANT` 区分，所以同一批词会稳定地出现一致 / 不一致两类结果。
