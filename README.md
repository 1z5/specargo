# tts-pipeline-argo

![unit-tests](https://github.com/1z5/tts-pipeline-argo/actions/workflows/unit-tests.yml/badge.svg)
<!-- unit-tests：79 个离线用例（框架 33 + 断点续跑/状态 9 + 服务·存储·指标 14 + P0 回归 23），Python 3.11 + 3.12 -->
![k8s smoke](https://github.com/1z5/tts-pipeline-argo/actions/workflows/k8s-smoke.yml/badge.svg)

**一个声明式批量数据管线框架：定义一次，本地可跑、也能编译成 Argo CRD 上集群。**
覆盖「清洗 → 去重 → 分片 → 模型推理接入 → 质量校验 → 交付」整条链路，
用插件化质量闸门和 manifest 把"跑完"变成"跑对、可对账、可复现"。

框架与业务解耦：**TTS 只是其中一个领域插件示例**（`pipelines/tts.yaml`），
通用能力全部在框架层——声明式 runner、spec→Argo 编译器、断点续跑、确定性分片、
隔离区、对象存储抽象、Prometheus 指标、常驻处理服务。

> **边界（说清楚，不含糊）**：`spec → Argo CRD` 的编译器已落地
> （`app/spec2argo.py`，CI 提交编译产物跑集群），覆盖 container 命令 + checks +
> artifacts + 参数/扇出/检查配置映射，并对编译产物做"零残留模板变量"自检；
> script 型 template、synchronization、suspend、onExit 尚未映射，
> 仍有覆盖缺口——见 [`docs/ROADMAP.md`](docs/ROADMAP.md)。

设计来自我在内容生产团队做批量 TTS 管线的真实经验，重构为可复现的开源参考实现：
把"跑完"变成"跑对"——任务编排、断点续跑、幂等、质量校验、对账、告警六个环节都有代码。

```
书籍 JSON ─► generate(按书扇出) ─► verify(ASR 回检三态判定) ─► fix(仅失败时) ─► reconcile(对账)
                                                                          │
                                                              onExit: notify(成败都收口)
```

## 三条示例管线（同一套框架，不同业务形态）

| 管线 | 链路 | 演示的框架能力 |
|---|---|---|
| [`pipelines/dataset-build.yaml`](pipelines/dataset-build.yaml) | 原始数据 → **schema 校验 / 去重 / 规范化** → **确定性分片** → **manifest** | 清洗链路、隔离区、分片可复现、下游对账 |
| [`pipelines/etl-demo.yaml`](pipelines/etl-demo.yaml) | 按分区扇出 → extract / transform / load → 质量闸门 | DAG 编排、动态扇出、跨 Pod 数据契约 |
| [`pipelines/tts.yaml`](pipelines/tts.yaml) | 批量生成 → ASR 回检（三态判定）→ 仅失败时修复 → 对账 | 领域插件接入、run 级断点续跑、大模型静默截断检测 |

三条管线共用同一份 `spec.py` 语义与 `runner`/`spec2argo` 两个执行目标，**只换 YAML，不换框架**。

### 数据集交付：manifest + 确定性分片

`app/manifest.py` 解决的是训练/推理侧的硬约束：**同一份输入必须能切出同一份分片**——
断点续训、数据并行、可复现评测都依赖这一点，随机分片重跑就切不回同一份。

- 分片规则：`shard = sha256(id) % N`（不用内置 `hash()`，它受 `PYTHONHASHSEED` 影响）
- 每次产出附 manifest：条数、分片清单、逐片 sha256、输入指纹
- 双层校验：`shard` stage 命令内跑 `manifest.py verify` 做深度自校验（条数合计/校验和/分片齐全）；
  集群 gate 只做 manifest 结构校验（`json_schema`），因为它能跨 Pod 取回的只有单文件 artifact

本地全链路命令（seed → clean → shard → verify，零外部依赖、实测可跑）见「[快速开始](#快速开始)」第 1 步。

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

## 能力矩阵

| 能力域 | 实现 |
|---|---|
| **任务编排** | `spec.py` + `runner.py`：声明式 stage / dependencies / retry / timeout，分区支持 list、range、discover（动态发现）三种来源 |
| **可靠性与恢复** | `runner.py` 框架级断点续跑（已到成功终态的 stage×partition 直接跳过）、`resume.py` 输入 sha256 指纹、`retryStrategy` 分层（瞬时错误才重试）、`dependencies` 阻断语义 |
| **常驻处理服务** | `service.py`：标准库 HTTP 常驻服务，`/healthz` `/readyz` 两种探针语义、`/process` 同步与 `/tasks` 异步、`POST /tasks/{id}/retry` 失败重处理、Processor 扩展点（当前仅内置 `echo` 示例）、SIGTERM 四步优雅停机 |
| **数据质量** | `checks.py` 插件注册表（4 个通用检查）+ `tts_verify/` 句子级三态判定 + `summary.py` expected vs actual 对账（漏生成/未过检/重复文件/内容重复） |
| **状态与追溯** | `state.py` item 级状态表（dataset/partition/variant/stage 四元组主键）、run_id 全程贯穿日志与产物 |
| **可观测性** | `metrics.py` Prometheus text 格式指标（执行数/耗时分布/积压/对账缺失/inflight；`stage_progress` 已定义但当前未接线）、`notify.py` onExit 收口告警 |
| **存储抽象** | `artifacts.py` 对象存储契约 + 本地目录 / S3 兼容双实现，key 目录穿越防护；`digest` 仅本地目录实现提供、应用代码暂未调用 |
| **容器与编排** | `Dockerfile.ci` + `k8s/object-store.yaml` + `spec2argo` 编译产物；CI 在 kind + Argo Workflows v3.7.18 上实测跑通（31/31 Pod：1 discover + 6 × 5 分区 = 3 个 stage + 3 个质量闸门）。`k8s/workflow.yaml` / `cronworkflow.yaml` 是文档级示例（未在 CI 部署，需自备镜像/存储）；编译产物暂无 onExit |
| **多模态处理** | TTS/ASR 链路（音频批量生成 + 回检），`docs/VERIFICATION.md` 记录了模型静默截断这类无告警故障的定位方法 |
| **成本与效率** | 并发与重试参数化、输入指纹避免重复计算、`--force` 受控大批量重处理、stage 级 requests/limits 编译进 container；`nodeSelector`/`tolerations` 编译器尚未映射（跨节点只用手写 smoke 的 `podAntiAffinity` 软偏好） |

## 快速开始

以下命令都不依赖集群与 API key（TTS 领域示例除外，见本节末尾）。

```bash
# 1) 数据集构建链路：seed → clean → shard → manifest（零外部依赖，实测可跑）
python3 app/dataset_steps.py seed  --partition 1 --out out/raw/1.jsonl
python3 app/dataset_steps.py clean --in out/raw/1.jsonl --out out/clean/1.jsonl --quarantine out/quarantine/1.jsonl
python3 app/dataset_steps.py shard --in out/clean/1.jsonl --out-dir out/shards/1 \
        --shards 2 --manifest out/manifest/1.json --dataset raw_partition --version v1 --base-dir out
python3 app/manifest.py verify --manifest out/manifest/1.json --base-dir out

# 2) 通用 ETL 管线（本地 runner，不需要 Argo 集群；--workdir 会自动创建）
python3 app/runner.py pipelines/etl-demo.yaml --state-db /tmp/etl-run/state.db --workdir /tmp/etl-run
# 第二次：已完成的 stage×partition 全部 resume 跳过
python3 app/runner.py pipelines/etl-demo.yaml --state-db /tmp/etl-run/state.db --workdir /tmp/etl-run

# 3) 离线测试：全部套件（framework / resume-state / service-storage / P0 回归）
#    不需要 API key 与集群；CI 在 Python 3.11 / 3.12 上自动跑
for f in tests/test_*.py; do python3 "$f"; done

# 4) 本地 lint Argo YAML（需自行安装 argo CLI；离线，无需集群）
argo lint --offline k8s/workflow-etl-smoke.yaml

# 5) 集群冒烟（CI 自动跑；也可手动触发）
gh workflow run k8s-smoke -R 1z5/tts-pipeline-argo
```

### TTS 领域示例（需要额外的密钥/依赖，不在离线测试范围内）

`pipelines/tts.yaml` 是框架的一个**领域插件示例，不是框架主线**：需要自备书籍 JSON 与
`OPENAI_API_KEY`（另需 `app/requirements.txt` 里的 `openai` / `faster-whisper`）。
**该参考实现当前待归档修复**——历史上 `app/main.py` 从未入库、仓库也不含 `books/` 样例数据，
所以 `python main.py ...` 这类老命令是失效的，**入口不是 `main.py`**；修复前请只把
`pipelines/tts.yaml` 当作文档参考。

```bash
cd app
pip install -r requirements.txt        # 含 openai / faster-whisper 等重依赖
cp ../.env.example .env                # 填 OPENAI_API_KEY

# 真实入口是 pipeline.py（--book 指向自备的书籍 JSON）
python3 pipeline.py --book /path/to/your_book.json --voice cedar
# 校验与对账（依赖上一步产出与 faster-whisper 模型）
python3 verify_audio.py output/your_book --min-accuracy 0.90
python3 summary.py --state-db state.db --books /path/to/your_book.json --voice cedar
```

## Argo Workflows 编排（TTS 示例）

`k8s/workflow.yaml`（单次批量）与 `k8s/cronworkflow.yaml`（每日定时）是 TTS 示例的
**文档级编排**：引用 `registry.example.com` 镜像与三个 PVC，需要自备镜像/存储/密钥才能跑，
CI 只对它们做 `argo lint` schema 校验（未在 CI 部署）。已在集群实测的是
`pipelines/etl-demo.yaml` 的编译产物（见「已验证」一节）。

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
# 前置：自备镜像 registry.example.com/data/tts:v1、PVC tts-*-pvc、Secret tts-secrets
# 锁 ConfigMap 要为每个提交的 slug 建 key（semaphore 按 slug 取 key）
kubectl create configmap tts-book-lock --from-literal=book_a=1 --from-literal=book_b=1
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

**多个实例证明普适性**（互不包含对方的知识）：
- `pipelines/tts.yaml` — TTS 生成 + ASR 回检（`asr_three_state` 插件；领域示例，当前待归档修复）
- `pipelines/etl-demo.yaml` — extract → transform → load，用 `json_schema`/`row_count` 通用检查
- `pipelines/dataset-build.yaml` — 清洗 / 去重 / 确定性分片 / manifest，用 `json_schema`/`file_nonempty` 通用检查，深度自校验在 stage 命令内

运行方式见「快速开始」第 2 步（`--workdir` 由 runner 自动创建）。

### 框架化过程中真实踩到并修掉的 4 个坑

1. **item 主键缺 stage 维度**：多 stage 管线里，后一个 stage 的状态会覆盖前一个，
   "生成成功但校验没过"就查不出来 → 主键加入 stage。
2. **对账用错 dataset**：用管线名而不是 `partition.dataset` 去对账，导致 `expected=0`、
   假绿——正是本项目要防的那类故障 → 按 dataset 分组对账。
3. **依赖语义缺失**：上游失败后下游照样跑，还会用下游的成功掩盖上游失败 → 实现 `dependencies` 阻断。
4. **json_schema 不认 JSONL**：直接 `json.loads` 整个文件会在第二行报 "Extra data" → 逐行解析回退。

## 已验证 / 未验证

**已验证（79 个离线用例 + 集群 E2E）**：
- 框架 `tests/test_framework.py`（33 例）：spec 解析与严格渲染校验、四类通用检查、
  runner 断点跳过/依赖阻断/重试/attempt 计数、对账按 variant 分组抓假绿、
  `spec2argo` 编译产物结构/确定性/gate/discover 输出
- 断点续跑与状态 `tests/test_resume_state.py`（9 例）：指纹跳过与输入变化、
  输出缺失重生成、item 状态幂等 upsert、对账漏生成/未过检、重复文件与内容重复检测
- 服务/存储/指标 `tests/test_service_and_storage.py`（14 例）：**真实起服务进程**验证探针、
  带 `--state-db` 时 readyz 与状态落库、异步任务全生命周期、暂停期拒新任务、
  SIGTERM 优雅退出（退出码 0）、retry 用原始 payload；
  metrics 格式与原子落盘；objstore 本地/目录往返、目录穿越拒绝、CLI 退出码
- P0 回归 `tests/test_p0_regressions.py`（23 例）：把审计中实测到的静默失败/假绿固化为断言
  （spec 写错必须报错、无质量闸门 stage 的 resume 与对账、编译产物的参数/扇出/检查配置、
  集群 gate 取数走对象存储）
- E2E：ETL 管线 9 verified / 二次跑 9 resume-skipped / 指标文件内容正确 / 服务冒烟含优雅停机

**已在真实集群跑通（kind 多节点 + Argo Workflows v3.7.18，CI 每次 push 自动验证）**。
以下为一次 CI run 的实测输出（commit `b7c5b35`）：

```
工作流最终状态: Succeeded
Pod 节点成功数: 31/31              # 1 discover + 6 × 5 分区（3 个 stage + 3 个质量闸门）
discover 发现并扇出的分区数: 5
Pod 分布到的节点数: 2
对象存储中的 artifact 对象数: 15   # 5 分区 × (raw + clean + marker)
```

**跑的是编译产物**：CI 用 `spec2argo` 把 `pipelines/etl-demo.yaml` 编译成 Argo CRD 再提交，
不是手写的第二份定义——本地 runner 与集群执行因此共享同一份 spec。

集群拓扑与验证点：

| 能力 | 怎么验的 | 结果 |
|---|---|---|
| **多节点集群 + 跨节点调度** | kind 1 控制面 + 2 worker；模板加 `podAntiAffinity` 软偏好 | Pod 分布到 2 个 worker（17 + 14，合计 31）；CI 断言只要求 ≥ 2 个节点 |
| **动态工作流** | `discover` 任务扫描 ConfigMap 输入产出 JSON 数组，下游 6 个扇出任务（3 个 stage + 3 个质量闸门）用 `withParam` 引用其输出扇出 | 跑哪些分区由数据决定，YAML 里无写死分区 |
| **对象存储 artifact** | 集群内 S3 兼容端点 + `objstore.py` 按路径契约 `runs/<run_id>/<partition>/<stage>` 传递 | 15 个对象真实落盘；跨 Pod 取数不依赖任何共享卷 |
| **质量闸门** | 每个 stage 后跟一个 `gate-<stage>` 任务，在集群里跑 `checks.py` 插件 | 不过检则 task 失败（`limit: 0` 不重试） |
| **一份定义两个目标** | `spec2argo` 把 `pipelines/etl-demo.yaml` 编译成 Argo CRD，CI 提交编译产物 | 本地 runner 与集群不再各写一份 |
| **重试分层** | extract `limit 2 / OnTransientError`；transform、load `limit 0`；质量闸门 `gate-<stage>` 同样 `limit 0` | 语义按失败类型区分（编译产物里没有独立的 `validate` 阶段） |
| **onExit 收口** | 历史手写版 `k8s/workflow-etl-smoke.yaml`（CI 现已不提交该文件）无论成败都上传 `run.done` | 收口对象在 bucket 内；编译产物尚未生成 onExit（见 `docs/ROADMAP.md`） |

```bash
# 本地 lint（离线，无需集群）
argo lint --offline k8s/workflow-etl-smoke.yaml

# 手动触发集群冒烟
gh workflow run k8s-smoke -R 1z5/tts-pipeline-argo
```

### 架构决策：为什么用路径契约而不是 artifact 引用

`{{tasks.extract.outputs.artifacts.raw}}` 这种引用依赖**扇出索引匹配**：上下游分区列表
必须完全一致且顺序相同，Argo 才能按 index 配对。CI 里实测过它的脆弱性
（`Unable to resolve: tasks.extract.outputs.artifacts.raw`）。所以改为显式路径契约：

```
契约 key = runs/<run_id>/<partition>/<stage>.<ext>
```

上下游只约定 key 怎么算，不依赖编排器的配对行为——大规模扇出、部分失败重试、
上下游分区列表不同都不会断。这也是许多生产平台的常见做法。

### 集群实测中修掉的 4 个问题

1. **多节点下 hostPath 不共享**：extract 在 worker2 写 `/output`，transform 在 worker
   读不到 → 大量 exit 1。跨 Pod 数据必须走对象存储或 RWX PVC。
2. **`moto_server` 默认只绑 127.0.0.1**：集群内探针连接被拒 → 显式 `-H 0.0.0.0`。
3. **artifact-repositories 缺注解**：v3.x 靠 `workflows.argoproj.io/default-artifact-repository`
   注解指定默认仓库，缺了控制器不加载任何仓库，executor 报
   "You need to configure artifact storage"（exit 64）。
4. **bucket 需显式创建**：moto 启动为空，不像 MinIO 在启动命令里 mkdir。

**仍未实测**：千级 Pod 规模的 controller 限流与积压、ReadWriteMany PVC、
生产级 MinIO/OSS 的真实吞吐、多副本高可用。这些需要更大集群与真实数据量。

## 目录

```
Dockerfile      TTS 领域镜像（历史版；按仓库根构建会失败，通用镜像用 Dockerfile.ci）
Dockerfile.ci   CI 通用镜像（moto server + app/）
app/            应用代码
  service.py      Python 常驻处理服务（探针/同步异步/优雅停机/版本）
  artifacts.py    对象存储抽象（本地目录 / S3 兼容双实现）
  metrics.py      Prometheus 指标（Counter/Gauge/Histogram + textfile 输出）
  spec.py         声明式管线定义（YAML → Stage/Partition/RetryPolicy）
  runner.py       通用 stage 执行器（断点续跑/依赖/重试/超时/对账）
  spec2argo.py    编译器：PipelineSpec → Argo Workflow CRD（CI 提交编译产物）
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
pipelines/      声明式管线定义（dataset-build.yaml / etl-demo.yaml / tts.yaml）
k8s/            Argo 编排（Workflow / CronWorkflow）
tests/          离线测试（79 例：框架 33 / 断点续跑与状态 9 / 服务·存储·指标 14 / P0 回归 23）
docs/           架构、校验机制、ROADMAP（下一步计划）
```

镜像构建：通用镜像用 `docker build -f Dockerfile.ci -t tts-argo:ci .`
（CI 与 `k8s/` 清单都按 `tts-argo:ci` 这个 tag 引用）。根目录 `Dockerfile` 是 TTS 领域历史镜像，
按仓库根构建会失败（`COPY requirements.txt` 而文件在 `app/`），归档前不要使用。

## English Summary

A reference implementation of a batch data pipeline with **Argo Workflows** orchestration:
batch generation → independent ASR re-verification (three-state sentence matching) →
conditional repair → reconciliation, with checkpoint-resume (input fingerprint),
item-level state registry, and exit-handler alerting. Built from real production
experience in content pipelines. See `docs/` for the verification methodology and
`k8s/` for the full workflow specs.

## License

MIT
