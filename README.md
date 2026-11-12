# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 多入口汇入去重（汇入台）

「汇入去重」页把多个**本机变更入口**（`change_entries`，各绑定一份 JSONL 夹具）拉进同一
待处理集合，按实体键裁决后，保留条进入共享应用通道（`Partner Merge CDC`），走原有连续序
Apply / 缺口隔离规则，并写回批次清单与对账报告。

- 入口：`/fan-in` 页面可勾选入口、设置每入口拉取上限、触发批次、重置入口游标，查看清单与对账。
- 固定裁决规则（报告与 UI 同源展示）：
  `revision 高 > event_ts 新 > 入口登记 id 小 > 源 seq 大`，每组 `entity_key` 仅保留一条。
- 去重丢弃是**汇入阶段**终态：`dedupe_status=dropped`，不写账本、不占 apply 序、不改 Sink；
  清单中标记 `apply_status=dropped_at_fan_in` 与 `retained_candidate_id`。
- 保留条若随后因入口源序缺口被链路拦下，是另一种状态：写账本 `quarantine_gap` +
  隔离区 `source_seq_gap_after_fan_in`，清单为 `apply_status=quarantined`；缺口之后的同入口
  保留条为 `blocked_gap`（未提交、未写账本）。
- 对账数字全部从库内回读：入口拉取数、候选总数、保留/丢弃、applied/quarantined/blocked、
  账本行、Sink 行、通道游标、入口 pull/consumed 游标，并在报告中给出逐项一致性校验
  （含“丢弃条未静默写入 Sink”“清单与账本逐条可追溯”）。
- 批次与清单持久化在 `fan_in_batches` / `fan_in_candidates`，刷新页面后仍可查。

演示数据：

| 入口 | 夹具 | 内容 |
| --- | --- | --- |
| Partner A 分片 | `partner_a_cdc.jsonl` | ORD-3001 rev2 / ORD-3002 rev2 / ORD-3003 较旧 event_ts |
| Partner B 分片 | `partner_b_cdc.jsonl` | ORD-3001 rev3 / ORD-3002 rev1 / ORD-3003 新 event_ts / ORD-3005 跨缺口 |

跑一遍的结果应为：候选 7、保留 4、丢弃 3、applied 3、quarantined 1；Sink 中
ORD-3001/3002/3003 各一行，且通道游标 0 → 3，全部对账校验通过。

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口

当前仓库已实现「连续序幂等应用」主链，以及 `fan_in_dedupe_key` 的多入口汇入去重。
缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等仍在
`backend/app/modules/*` 留空包，按题单逐题换主链路实现。
