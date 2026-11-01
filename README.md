# tts-pipeline-argo

**一条完整的数据管线案例：批量生成 → 独立回检 → 失败修复 → 对账收口，用 Argo Workflows 编排；
编排本身已框架化——同一个声明式 runner 能跑 TTS 管线，也能跑与 TTS 无关的 ETL。**

设计来自我在内容生产团队做批量 TTS 管线的真实经验，重构为可复现的开源参考实现：
把"跑完"变成"跑对"——任务编排、断点续跑、幂等、质量校验、对账、告警六个环节都有代码。

```
书籍 JSON ─► generate(按书扇出) ─► verify(ASR 回检三态判定) ─► fix(仅失败时) ─► reconcile(对账)
                                                                          │
                                                              onExit: notify(成败都收口)
```

## 解决什么问题

批量任务最贵的故障不是"报错"，是**静默失败**：任务退出码 0、日志全绿、文件生成了，
但内容不对（漏读、截断、错位）。本项目用一套机制把这类故障变成**可量化、可回归、可阻断**的信号：

| 能力 | 做法 |
|---|---|
| 断点续跑 | 输入 sha256 指纹 + 产物存在性判定，未变则跳过（`resume.py`） |
| 幂等 | 指纹即幂等键；任务层保证"再跑一次"安全（编排层只负责重试） |
| 质量校验 | ASR 回检 + 句子级三态判定（FOUND / UNSURE / MISSING），阈值按分布标定 |
| 对账 | expected vs actual：漏生成 / 未过检 / 重复文件 / 内容重复，四类检查 |
| 结果登记 | item 级状态表（run_id 贯穿全程，可追溯） |
| 告警 | onExit 统一收口，webhook 可插拔 |

## 快速开始

```bash
cd app
pip install -r requirements.txt
cp ../.env.example .env        # 填 OPENAI_API_KEY

# 生成（断点续跑默认开启，二次跑会跳过未变的章节）
python main.py production books/book_a.json

# 校验漏读（三态判定 + 报告 + 退出码，可直接接 CI）
python verify_audio.py output/book_a --min-accuracy 0.90

# 对账：expected vs actual，漏数/未过检/重复 → 非零退出码
python summary.py --state-db state.db --books books/book_a.json --voice cedar

# 测试（9 个用例，不需要 API key 与集群）
cd .. && python3 tests/test_resume_state.py
```

## Argo Workflows 编排

`k8s/workflow.yaml`（单次批量）与 `k8s/cronworkflow.yaml`（每日定时）给出完整编排：

- **DAG**：`generate → verify → fix(仅 when 失败) → reconcile`
- **扇出**：`withParam` 按书展开，一个 Pod 一本书（失败影响面最小）
- **重试分层**：`retryStrategy: OnTransientError`（限流/网络）给生成；校验 `limit: "0"`
  （数据问题重试无意义，走修复分支）；容器内重试 `TTS_MAX_RETRIES=1` 避免两层相乘
- **超时分层**：task `timeout` < workflow `activeDeadlineSeconds`
- **状态外置**：`tts_item` 落 PVC（Pod 之间不能靠 emptyDir 共享状态）
- **清理**：`podGC` / `ttlStrategy` / `artifactGC`
- **互斥**：`synchronization.semaphores` 防止同一本书并发跑
- **输出契约**：业务结果写 `/argo/outputs/parameters/*`，供下游 `when` 分支消费

```bash
kubectl create configmap tts-book-lock --from-literal=book_a=1
kubectl apply -f k8s/workflow.yaml
argo submit -n data-platform k8s/workflow.yaml -p book-slugs=book_a,book_b --watch
```

## 设计决策（重点看这里）

1. **状态必须落在 Pod 之外**。Argo 每个 task 是独立 Pod，emptyDir 随 Pod 消失；
   SQLite（单机）→ MySQL/PostgreSQL（集群），迁移成本只剩连接串。
2. **指纹即幂等键**。生成时已落盘 `.input.txt` 原文快照，那本身就是输入指纹，
   改造只是把这个隐含机制显式化。
3. **重试要分失败类型**。环境性失败（限流/5xx/网络）才重试；
   确定性失败（内容缺失/格式错）重试只是重复烧钱，应进隔离区走修复分支。
4. **"跑完"不等于"跑对"**。编排层只保证 Pod 退出码 0，不保证业务上这个 item 处理完了；
   必须有独立对账节点做 expected vs actual，差异不为空就让工作流失败。
5. **口径统一是隐蔽的坑**。产物文件名用 JSON 文件名 stem，而数据模型的 slug 由标题派生，
   两者经常不同——对账用错口径会 100% 误报"漏生成"。（这个 bug 在开发中真实踩到并修复。）

## 框架化：从"TTS 管线"到"可复用的批量任务引擎"

第二版把领域逻辑从编排里剥出去，同一个 runner 现在能跑**与 TTS 无关**的管线：

```
声明式定义(YAML) ─► spec.py 解析 ─► runner.py 执行 ─► checks.py 质量闸门 ─► state.py 登记对账
                                      │
                                      └─ 断点续跑 / 依赖语义 / 重试策略 / 超时，全部在框架层
```

| 能力 | 原实现（TTS 专用） | 框架化后 |
|---|---|---|
| 编排 | `pipeline.py` 写死四段 | YAML 声明 stage + dependencies |
| 分区 | (book, chapter, voice) 硬编码 | (dataset, partition, variant) 通用，支持 list/range/discover |
| 质量检查 | ASR 三态判定写死在 verify_audio.py | 插件注册表；ASR 只是其中一个 check |
| 断点续跑 | 在 tts_client 里（任务层） | **框架层**：已 verified 的 (stage×partition) 直接跳过 |
| 对账 | 从书籍 JSON 推导 expected | expected 由管线定义给出，按 dataset 分组 |

**两个实例证明普适性**（都不含对方的知识）：
- `pipelines/tts.yaml` — TTS 生成 + ASR 回检（`asr_three_state` 插件）
- `pipelines/etl-demo.yaml` — extract → transform → load，用 `json_schema`/`row_count` 通用检查

```bash
cd app
python3 runner.py ../pipelines/etl-demo.yaml --state-db state.db --workdir /tmp/run
python3 runner.py ../pipelines/etl-demo.yaml --state-db state.db --workdir /tmp/run   # 第二次：全部 resume 跳过
```

### 框架化过程中真实踩到并修掉的 4 个坑

1. **item 主键缺 stage 维度**：多 stage 管线里，后一个 stage 的状态会覆盖前一个，
   "生成成功但校验没过"就查不出来 → 主键加入 stage。
2. **对账用错 dataset**：用管线名而不是 `partition.dataset` 去对账，导致 `expected=0`、
   假绿——正是本项目要防的那类故障 → 按 dataset 分组对账。
3. **依赖语义缺失**：上游失败后下游照样跑，还会用下游的成功掩盖上游失败 → 实现 `dependencies` 阻断。
4. **json_schema 不认 JSONL**：直接 `json.loads` 整个文件会在第二行报 "Extra data" → 逐行解析回退。

## 已验证 / 未验证

**已验证**：9 个离线测试（指纹跳过、输入变化、对账抓漏、幂等写入、两类重复检测）；
断点续跑 E2E（二次跑全部跳过）；对账失败路径（exit 1 + 待处理清单）；YAML 结构与引用一致性。

**未验证**：未在真实 K8s 集群跑过（Pod 调度、PVC、镜像拉取、artifact repository 未实测）；
容器内资源配额为估算；并发与扇出上限需集群数据校准。

## 目录

```
app/            应用代码
  spec.py         声明式管线定义（YAML → Stage/Partition/RetryPolicy）
  runner.py       通用 stage 执行器（断点续跑/依赖/重试/超时/对账）
  checks.py       质量检查插件注册表 + 4 个通用检查
  checks_tts.py   TTS 领域检查（ASR 三态判定，插件方式注入）
  etl_steps.py    ETL demo 的三个步骤（证明框架普适性）
  resume.py       输入指纹与跳过判定
  state.py        item 状态表 + 对账 + 重复检测
  argo_io.py      Argo 输出参数/artifact
  summary.py      对账 CLI
  notify.py       批次收口告警
  pipeline.py     生成编排（断点续跑 + 状态登记 + 单书 CLI）
  tts_client.py   TTS 客户端（指纹跳过 + 重试可配）
  verify_audio.py 校验 CLI（三态判定 + 状态登记 + Argo outputs）
  tts_verify/     校验核心包
pipelines/      声明式管线定义（tts.yaml / etl-demo.yaml）
k8s/            Argo 编排（Workflow / CronWorkflow）
tests/          离线测试（23 个用例：框架 + 断点续跑 + 对账）
docs/           架构与校验机制说明
```

## English Summary

A reference implementation of a batch data pipeline with **Argo Workflows** orchestration:
batch generation → independent ASR re-verification (three-state sentence matching) →
conditional repair → reconciliation, with checkpoint-resume (input fingerprint),
item-level state registry, and exit-handler alerting. Built from real production
experience in content pipelines. See `docs/` for the verification methodology and
`k8s/` for the full workflow specs.

## License

MIT
