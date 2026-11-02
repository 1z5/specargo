# specargo

![unit-tests](https://github.com/1z5/specargo/actions/workflows/unit-tests.yml/badge.svg)
<!-- unit-tests：100 个离线用例（框架 96 + 领域示例 4），Python 3.11 + 3.12 -->
![k8s smoke](https://github.com/1z5/specargo/actions/workflows/k8s-smoke.yml/badge.svg)

**一个声明式批量数据管线框架：定义一次，本地可跑，也能编译成 Argo CRD 上集群。**

管线用一份 YAML 描述（stage / 依赖 / 分区 / 重试 / 超时 / 质量闸门 / 产物契约），
框架提供两个执行目标：

- **本地**：`app/runner.py` 直接执行——断点续跑、依赖阻断、重试分层、质量闸门、对账收口；
- **集群**：`app/spec2argo.py` 把同一份 spec 编译成 Argo Workflow CRD，本地与集群共享一份定义。

**框架层不含任何领域逻辑**：`app/` 不 import 领域代码，领域能力通过 `plugins:` 显式加载
（旧前缀字符串只出现在两个兼容文件 `envcompat.py` / `legacy_state_migration.py`，
CI 的纯净度守卫对它们例外）。仓库自带一个来自真实生产的领域示例
[`examples/tts-asr/`](examples/tts-asr/README.md)（TTS 生成 + ASR 回检 + 对账），
只作为"如何写领域插件"的参考，不是框架主线。

> **边界（说清楚，不含糊）**：编译器覆盖 container 命令 + checks + artifacts +
> 参数/扇出/检查配置映射，并对编译产物做"零残留模板变量"自检；集群侧另有**可选的
> 状态日志 + onExit 对账**（`k8s.journal: true` 开启）。script 型 template、
> synchronization、suspend 尚未映射；集群 gate 只支持单文件 artifact 的跨 Pod 校验；
> 集群侧断点续跑（skip）仍未映射。详见 [`docs/ROADMAP.md`](docs/ROADMAP.md)。

设计来自我在真实内容生产环境做批量 TTS/ASR 管线的经验：先有领域管线，再把可复用的部分
剥成框架。领域实现没有删除，而是归档为 `examples/tts-asr/`，用来对照"框架契约 vs 领域知识"的边界。

```
YAML spec ─► spec.py 解析/校验 ─► runner.py 本地执行 ─► checks.py 质量闸门 ─► state.py 登记对账
                                  └─ spec2argo.py 编译 ─► Argo Workflow CRD ─► 集群执行
```

## 三条示例管线（同一套框架，不同形态）

| 管线 | 链路 | 演示的框架能力 |
|---|---|---|
| [`pipelines/dataset-build.yaml`](pipelines/dataset-build.yaml) | 原始数据 → **schema 校验 / 去重 / 规范化** → **确定性分片** → **manifest** | 清洗链路、隔离区、分片可复现、下游对账 |
| [`pipelines/etl-demo.yaml`](pipelines/etl-demo.yaml) | 按分区扇出 → extract / transform / load → 质量闸门 | DAG 编排、动态扇出（discover）、跨 Pod 数据契约 |
| [`examples/tts-asr/pipelines/tts.yaml`](examples/tts-asr/pipelines/tts.yaml) | 批量生成 → ASR 回检（三态判定）→ 对账 | 领域插件加载、外部 API 接入、领域级质量检查 |

三条管线共用同一份 `spec` 语义与 `runner` / `spec2argo` 两个执行目标，**只换 YAML，不换框架**。
第三条的领域检查（`asr_three_state`）不在框架里：它在
[`examples/tts-asr/checks_tts.py`](examples/tts-asr/checks_tts.py) 里注册，由 spec 顶层
`plugins:` 加载（见「契约命名与插件」）。

### 数据集交付：manifest + 确定性分片

`app/manifest.py` 解决训练/推理侧的硬约束：**同一份输入必须能切出同一份分片**——
断点续训、数据并行、可复现评测都依赖这一点，随机分片重跑就切不回同一份。

- 分片规则：`shard = sha256(id) % N`（不用内置 `hash()`，它受 `PYTHONHASHSEED` 影响）
- 每次产出附 manifest：条数、分片清单、逐片 sha256、输入指纹
- 双层校验：`shard` stage 命令内跑 `manifest.py verify` 做深度自校验（条数合计/校验和/分片齐全）；
  集群 gate 只做 manifest 结构校验（`json_schema`），因为它能跨 Pod 取回的只有**单文件** artifact

完整命令（seed → clean → shard → verify，零外部依赖）见「[快速开始](#快速开始)」第 1 步。

## 解决什么问题

批量任务最贵的故障不是"报错"，是**静默失败**：任务退出码 0、日志全绿、文件生成了，
但内容不对（漏数、截断、错位）。框架用一套机制把这类故障变成**可量化、可回归、可阻断**的信号：

| 能力 | 做法 |
|---|---|
| 断点续跑 | 输入 sha256 指纹 + 产物存在性判定，未变则跳过（`resume.py`）；框架按"成功终态"判定 Stage×Partition |
| 幂等 | 指纹即幂等键；任务层保证"再跑一次"安全（编排层只负责重试） |
| 质量校验 | `checks.py` 插件注册表 + 4 个通用检查（`file_nonempty` / `json_schema` / `row_count` / `command`）；领域检查通过 `plugins:` 注入 |
| 对账 | expected vs actual：按 dataset / variant 分组对账，漏数 / 未过检即非零退出码 |
| 结果登记 | item 级状态表（`pipeline_run` / `pipeline_item`），run_id 贯穿日志与产物 |
| 告警 | 本地 runner 收口输出；集群侧可用编译生成的 `onExit` 对账 + webhook（`k8s.journal: true`，复用 `notify.py`） |

## 能力矩阵

| 能力域 | 实现 |
|---|---|
| **任务编排** | `spec.py` + `runner.py`：声明式 stage / dependencies / retry / timeout，分区支持 list、range、discover（动态发现）三种来源；`spec.py` 做严格校验（未知字段 / 依赖成环 / 类型错误在加载期报错） |
| **可靠性与恢复** | 框架级断点续跑（有质量闸门的 stage 只认 `verified`，无闸门的 `generated` 即终态）、`resume.py` 输入 sha256 指纹、`retryStrategy` 分层（瞬时错误才重试）、`dependencies` 阻断级联 |
| **常驻处理服务** | `service.py`：标准库 HTTP 常驻服务，`/healthz` `/readyz`、`/process` 同步、`/tasks` 异步、`POST /tasks/{id}/retry`、Processor 扩展点（当前仅内置 `echo` 示例）、SIGTERM 四步优雅停机 |
| **数据质量** | `checks.py` 插件注册表（4 个通用检查）+ `spec.plugins` 显式加载领域检查；本地 runner 与集群 gate 共用同一套检查实现 |
| **状态与追溯** | 本地：`state.py` 的 `pipeline_run` / `pipeline_item` 表（四元组主键 + run_id，旧库原地迁移）；集群：`journal.py` 把状态写成对象存储 key（`runs/<run>/_status/...`），不同 Pod 并发写不依赖共享盘 |
| **集群侧收口（可选）** | `k8s.journal: true` 时编译产物自动加：每个 stage/gate 结束写状态、discover 写分区清单、`onExit` 调 `pipeline_cli.py reconcile-journal` 对账（差异非空则工作流失败）+ webhook 告警 |
| **插件机制** | `plugins.py`：spec 顶层 `plugins: [模块名]`，按 spec 目录 / 父目录定位模块后 `importlib` 加载；加载失败启动期报 `SpecError`；框架**不 import** 领域代码 |
| **可观测性** | `metrics.py` Prometheus text 格式指标（执行数 / 耗时分布 / 积压 / 对账缺失 / inflight；`stage_progress` 已定义但当前未接线） |
| **存储抽象** | `artifacts.py` 对象存储契约 + 本地目录 / S3 兼容双实现，key 目录穿越防护；`objstore.py` 是集群内 put/get CLI；跨 Pod 走路径契约 `runs/<run_id>/<partition>/<stage>.<ext>` |
| **容器与编排** | 根目录 `Dockerfile`（moto S3 + `app/`）+ `k8s/object-store.yaml` / `artifact-repository.yaml`；CI 在 kind + Argo Workflows v3.7.18 上实测跑通（见「已验证」）。`k8s/workflow-etl-smoke.yaml` 是历史手写对照 |
| **成本与效率** | 并发与重试参数化、输入指纹避免重复计算、`--force` 受控重处理、stage 级 requests/limits 编译进 container；`nodeSelector`/`tolerations` 编译器尚未映射 |

## 快速开始

依赖：Python 3.11+。框架层只需要 PyYAML（示例的重依赖在各自 `examples/*/requirements.txt`）。

```bash
python3 -m pip install -r app/requirements.txt

# 1) 数据集构建链路：seed → clean → shard → manifest（零外部依赖）
python3 app/dataset_steps.py seed  --partition 1 --out out/raw/1.jsonl
python3 app/dataset_steps.py clean --in out/raw/1.jsonl --out out/clean/1.jsonl --quarantine out/quarantine/1.jsonl
python3 app/dataset_steps.py shard --in out/clean/1.jsonl --out-dir out/shards/1 \
        --shards 2 --manifest out/manifest/1.json --dataset raw_partition --version v1 --base-dir out
python3 app/manifest.py verify --manifest out/manifest/1.json --base-dir out

# 2) 本地跑一条完整管线（不需要集群；--workdir 会自动创建）
python3 app/runner.py pipelines/etl-demo.yaml --state-db /tmp/etl-run/state.db --workdir /tmp/etl-run
# 第二次：已到成功终态的 stage×partition 全部 resume 跳过
python3 app/runner.py pipelines/etl-demo.yaml --state-db /tmp/etl-run/state.db --workdir /tmp/etl-run

# 3) 编译成 Argo CRD（lint 需要自行安装 argo CLI，可选）
mkdir -p /tmp/compiled
python3 app/spec2argo.py pipelines/etl-demo.yaml -o /tmp/compiled/etl-demo.yaml
argo lint --offline /tmp/compiled/etl-demo.yaml

# 4) 离线测试：100 例（框架 96 + 领域示例 4），不需要 API key 与集群；CI 在 3.11 / 3.12 上自动跑
for f in tests/test_*.py examples/tts-asr/tests/test_*.py; do python3 "$f"; done

# 5) 集群冒烟（CI 自动跑；也可手动触发）
gh workflow run k8s-smoke -R 1z5/specargo
```

环境变量样例：框架级见 [`.env.example`](.env.example)，领域示例见
[`examples/tts-asr/.env.example`](examples/tts-asr/.env.example)。
领域示例的**生成/校验链路**需要 `OPENAI_API_KEY` 与 `faster-whisper` 模型，
不在离线套件内；运行方式见 [`examples/tts-asr/README.md`](examples/tts-asr/README.md)。

## 契约命名与插件

框架契约统一用中性命名；历史 `TTS_*` 环境变量保留**一个版本**的兼容读取
（`app/envcompat.py`：新名优先、旧名回退）。

| 契约 | 框架命名 | 兼容 / 迁移 |
|---|---|---|
| 运行与路径环境变量 | `PIPELINE_RUN_ID` / `PIPELINE_STATE_DB` / `PIPELINE_ARGO_OUTPUTS` / `PIPELINE_ARGO_PARAM_DIR` / `PIPELINE_ARGO_ARTIFACT_DIR`（以及 workflow 内的 `PIPELINE_INPUT_DIR`） | `TTS_RUN_ID` / `TTS_STATE_DB` / `TTS_ARGO_*` 通过 `envcompat.py` 回退读取（一个版本后删除）；workflow 内的 `PIPELINE_INPUT_DIR` 是编排本地变量，不做回退 |
| 镜像与集群 | 镜像 tag `specargo:ci`；kind 集群名 `specargo` | — |
| 状态库 | 表 `pipeline_run` / `pipeline_item`；列 `dataset` / `partition` / `variant` | 旧库 `tts_run` / `tts_item` 在 `connect()` 时原地迁移（`legacy_state_migration.py`，`ALTER TABLE RENAME`，幂等） |
| 对象存储 key | `runs/<run_id>/<partition>/<stage>.<ext>` | run_id 取 `PIPELINE_RUN_ID`（编译器注入 `{{workflow.name}}`） |
| 领域插件 | spec 顶层 `plugins: [模块名]` | `app/plugins.py` 把 spec 目录及其父目录加入 `sys.path` 后 `importlib` 加载；失败启动期 `SpecError` |

插件是**单向依赖**：`examples → framework`，框架层永远不反向 import 领域代码。
本地 runner 与编译出的集群 gate **加载同一份声明**：编译器把 `plugins` 注入 gate 容器
（`PIPELINE_PLUGINS` + `PIPELINE_PLUGIN_PATH`），插件模块需在镜像内，路径由
spec 的 `k8s.plugin_path` 指定；加载失败时 gate 启动即报错。示例：

```yaml
# examples/tts-asr/pipelines/tts.yaml（片段）
name: tts-example
plugins: ["checks_tts"]      # 加载 examples/tts-asr/checks_tts.py，注册 asr_three_state 检查
```

## 集群侧语义（可选开启）

同一份 spec 在集群侧默认只做"编排 + 质量闸门"（CI 就这么跑）。如果部署环境提供
对象存储且需要和本地一致的收口语义，在 spec 里开启 journal：

```yaml
k8s:
  journal: true          # 开启状态日志 + onExit 对账
  notify_webhook: ""     # 可选：onExit 对账结果发送到钉钉/飞书/Slack
  objstore_env: {OBJSTORE_BUCKET: argo-artifacts}   # 可选：对象存储连接参数
```

编译产物会多出三类行为（全部复用框架同一套实现）：

1. **状态登记**：每个 stage 完成后 `pipeline_cli.py state put-remote` 把一个 JSON 写到
   `runs/<run_id>/_status/<stage>/<hash>.json`；gate 通过后覆盖为 `verified`。
2. **分区清单**：discover 类型管线的 discover 任务把分区清单写到
   `runs/<run_id>/_run/partitions.json`；list/range 类型在编译期直接内嵌。
3. **onExit 对账**：`pipeline_cli.py reconcile-journal` 读取状态日志，用与本地 runner
   **同一个** `evaluate_items()` 判定 expected vs actual；有漏数/未过检就让工作流失败，
   可选发送 webhook。分区清单缺失时的策略：**工作流已失败**才跳过对账（不掩盖原失败）；
   **成功路径**却缺清单会直接失败（无法证明数据完整，防止假绿）。

> 集群侧**断点续跑（skip）**尚未实现——Argo 的 retry/重跑仍会重新执行 task；
> 状态日志目前只用于 onExit 对账，不作为 resume 依据（见 ROADMAP）。

## 已验证 / 未验证

**已验证（100 个离线用例 = 框架 96 + 领域示例 4，+ 集群 E2E）**：

- 框架 `tests/test_framework.py`（32 例）：spec 解析与严格校验、四类通用检查、runner 断点跳过 /
  依赖阻断 / 重试 / attempt 计数、对账按 variant 分组抓假绿、`spec2argo` 编译产物结构 /
  确定性 / gate / discover 输出、框架不反向 import 领域插件
- 断点续跑与状态 `tests/test_resume_state.py`（8 例）：指纹跳过与输入变化、输出缺失重生成、
  通用 item 状态幂等 upsert、对账漏数 / 未过检、旧库 schema 自动迁移
- 服务 / 存储 / 指标 `tests/test_service_and_storage.py`（14 例）：**真实起服务进程**验证探针、
  带 `--state-db` 时 readyz 与状态落库、异步任务全生命周期、暂停期拒新任务、
  SIGTERM 优雅退出（退出码 0）、retry 用原始 payload；metrics 格式与原子落盘；
  objstore 本地 / 目录往返、目录穿越拒绝、CLI 退出码、S3 前缀语义
- P0 回归 `tests/test_p0_regressions.py`（27 例）：把审计中实测到的静默失败 / 假绿固化为断言
  （spec 写错必须报错、无质量闸门 stage 的 resume 与对账、编译产物的参数 / 扇出 / 检查配置、
  集群 gate 取数走对象存储）；完整清单见 [`docs/ROADMAP.md`](docs/ROADMAP.md)「P0 修复」
- 集群侧语义 `tests/test_cluster_semantics.py`（15 例）：gate CLI（内置/领域插件）、
  SQLite/对象存储状态登记、journal 对账 CLI（PASS/FAIL 与退出码）、编译产物
  onExit/状态写入结构、**按 Pod 语义端到端模拟**（stage → gate → onExit；缺数时 onExit 失败）
- 领域示例 `examples/tts-asr/tests/test_tts_state.py`（4 例）：书籍口径 ↔ 通用状态表映射、
  领域对账、重复产物检测（不装 openai / faster-whisper）
- E2E（本地）：ETL 管线全链路对账全绿、二次跑全部 resume-skip；dataset-build 全链路通过
- E2E（模拟集群）：每个 task-item 使用独立目录、只共享对象存储，模拟跨 Pod 取数；
  etl-demo 与 dataset-build 的编译产物均跑通
- 编译产物门禁：CI 对全部编译产物断言"零残留模板变量"（`{{params.*}}` / `{{partition.*}}`）
- CI：`unit-tests` 在 Python 3.11 / 3.12 上跑全部离线套件 + 框架纯净度守卫；
  `k8s-smoke` 在 kind 多节点 + Argo Workflows v3.7.18 上提交 `spec2argo` 编译产物，
  并额外跑一次 `journal: true` 变体验证 onExit 对账（真实集群）

**已在真实集群跑通（kind 1 控制面 + 2 worker + Argo v3.7.18，CI 每次 push 自动验证）**。
以下为一次 CI run 的实测输出：

```
工作流最终状态: Succeeded
Pod 节点成功数: 31/31              # 1 discover + 6 × 5 分区（3 个 stage + 3 个质量闸门）
discover 发现并扇出的分区数: 5
Pod 分布到的节点数: 2              # 实际分布 17 + 14；CI 断言只要求 ≥ 2
对象存储中的 artifact 对象数: 15   # 5 分区 × (raw + clean + marker)
```

集群拓扑与验证点：

| 能力 | 怎么验的 | 结果 |
|---|---|---|
| **多节点集群 + 跨节点调度** | kind 1 控制面 + 2 worker；模板加 `podAntiAffinity` 软偏好 | Pod 分布到 2 个 worker（17 + 14） |
| **动态工作流** | `discover` 任务扫描 ConfigMap 输入产出 JSON 数组，下游 6 个扇出任务（3 个 stage + 3 个质量闸门）用 `withParam` 引用其输出扇出 | 跑哪些分区由数据决定，YAML 里无写死分区 |
| **对象存储 artifact** | 集群内 S3 兼容端点 + `objstore.py` 按路径契约传递 | 15 个对象真实落盘；跨 Pod 取数不依赖任何共享卷 |
| **质量闸门** | 每个 stage 后跟一个 `gate-<stage>` 任务，先 `objstore get` 再跑 `checks.py` 插件 | 不过检则 task 失败（`limit: 0` 不重试） |
| **一份定义两个目标** | `spec2argo` 把 `pipelines/etl-demo.yaml` 编译成 Argo CRD，CI 提交编译产物 | 本地 runner 与集群不再各写一份 |
| **重试分层** | extract `limit 2 / OnTransientError`；transform、load `limit 0`；gate 同样 `limit 0` | 语义按失败类型区分（编译产物里没有独立的 `validate` 阶段） |
| **onExit 收口（可选）** | `k8s.journal: true` 的 spec 编译后生成 `onExit`：从对象存储状态日志对账，差异非空则工作流失败；成功路径缺分区清单也失败 | 本地按 Pod 语义模拟通过；**CI 额外提交 journal=on 变体做真实集群冒烟**（断言 onExit 节点成功 + 状态日志/分区清单落盘） |

**仍未实测**：千级 Pod 规模的 controller 限流与积压、ReadWriteMany PVC、
生产级 MinIO/OSS 的真实吞吐、多副本高可用、gRPC 服务形态。
**未映射**：集群侧断点续跑（skip）、script 型 template、synchronization、suspend；`onExit` 默认关闭，需 `k8s.journal: true` 显式开启。

### 已知边界（P0 修复确认，细节见 ROADMAP）

- 集群 gate 只支持**单文件** artifact 的跨 Pod 校验；目录 / 多产物 artifact 不支持。
- 分区 key / dataset / variant 限定为 `^(?!\.+$)[\w.-]+$`；含空格 / 引号 / `$` 的值会被拒绝。
- 参数值按**文本替换**注入命令：值里的 shell 元字符会被目标 shell 二次解释。
- gate 已改为调用 `pipeline_cli.py gate`（不再内嵌 Python 源码）；其行为在本地可单测。
- 集群 journal 依赖对象存储可写；SQLite 状态表仍只用于本地，集群多写者场景未提供 StateStore 协议。
- local runner 按依赖拓扑排序执行，不要求 YAML 里 stages 的书写顺序是拓扑序。
- **安全边界**：onExit 对账信任对象存储里的状态对象（能写 bucket 的进程可伪造 `verified`，部署侧应用 ACL/最小权限）；`notify_webhook` 仅允许 http/https；`k8s.objstore_env` 不得覆盖框架保留变量（`PIPELINE_RUN_ID` / `OBJSTORE_ENDPOINT` 等）；spec 的 `stage.command`/`discover` 命令本身就是代码，视为作者可信输入。

## Argo Workflows 编排（框架侧）

编译产物：`spec2argo` 把 spec 编译成 Workflow CRD，覆盖 DAG / `withParam` 扇出、
`retryStrategy`、`timeout` / `activeDeadlineSeconds`、`gate-<stage>` 质量闸门、
artifacts 的对象存储路径契约（上游 put、下游 get，key 一致），
以及 `{{workflow.parameters.*}}` 参数注入。开启 `k8s.journal` 时额外生成
`onExit` 模板（状态日志对账）与状态写入步骤，见上文「集群侧语义」。

```bash
# 本地编译 + lint（无需集群，argo CLI 需自行安装）
python3 app/spec2argo.py pipelines/etl-demo.yaml -o /tmp/compiled/etl-demo.yaml
argo lint --offline /tmp/compiled/etl-demo.yaml

# 手动触发 CI 集群冒烟
gh workflow run k8s-smoke -R 1z5/specargo
```

- **CI 基础设施**：`k8s/object-store.yaml`（moto S3 兼容端点）+
  `k8s/artifact-repository.yaml`（v3.x 需要 `default-artifact-repository` 注解才加载默认仓库）。
- **历史对照**：`k8s/workflow-etl-smoke.yaml` 是早期手写版（含 onExit），CI 现在只对它做
  `argo lint`；提交的是编译产物。
- **领域示例的 Argo 路径**在 [`examples/tts-asr/k8s/`](examples/tts-asr/k8s/)——文档级示例，
  引用示例镜像 / PVC / Secret，未在 CI 部署，需自备资源。

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

## 设计决策（重点看这里）

1. **状态必须落在 Pod 之外**。Argo 每个 task 是独立 Pod，emptyDir 随 Pod 消失；
   SQLite（单机）→ 同 schema 的 MySQL/PostgreSQL（集群），迁移成本只剩连接串。
2. **指纹即幂等键**。输入侧指纹（sha256）决定"要不要重算"，任务层保证"再跑一次"安全。
3. **重试要分失败类型**。环境性失败（限流 / 5xx / 网络）才重试；
   确定性失败（内容缺失 / 格式错）重试只是重复烧钱，应进隔离区走修复分支。
4. **"跑完"不等于"跑对"**。编排层只保证 Pod 退出码 0，不保证业务上这个 item 处理完了；
   必须有独立对账节点做 expected vs actual，差异不为空就让工作流失败。
5. **口径统一是隐蔽的坑**。产物命名口径与数据模型口径不一致时，对账会 100% 误报"漏生成"；
   框架对账按 (dataset, partition, variant) 分组，领域侧要保证命名与登记同源。

## 框架化：从领域管线到可复用的批量任务引擎

第一版是 TTS 专用管线（生成 → ASR 回检 → 修复 → 对账），第二版把领域逻辑从编排里剥出来：

| 能力 | 领域管线（原实现） | 框架化后 |
|---|---|---|
| 编排 | 脚本里写死四段 | YAML 声明 stage + dependencies |
| 分区 | (book, chapter, voice) 硬编码 | (dataset, partition, variant) 通用，支持 list/range/discover |
| 质量检查 | ASR 三态判定写在校验 CLI 里 | 插件注册表 + `plugins:` 显式加载；领域检查留在 examples |
| 断点续跑 | 在领域客户端里（任务层） | **框架层**：按成功终态判定 (stage×partition) |
| 对账 | 从领域数据推导 expected | expected 由管线定义给出，按 dataset 分组 |

三条示例管线互不包含对方的知识：`etl-demo` / `dataset-build` 只用通用检查，
`tts` 的 ASR 检查在 `examples/tts-asr/checks_tts.py` 里注册、由 spec 的 `plugins:` 加载。

### 框架化过程中真实踩到并修掉的 4 个坑

1. **item 主键缺 stage 维度**：多 stage 管线里，后一个 stage 的状态会覆盖前一个，
   "生成成功但校验没过"就查不出来 → 主键加入 stage。
2. **对账用错 dataset**：用管线名而不是 `partition.dataset` 去对账，导致 `expected=0`、
   假绿——正是本项目要防的那类故障 → 按 dataset 分组对账。
3. **依赖语义缺失**：上游失败后下游照样跑，还会用下游的成功掩盖上游失败 →
   实现 `dependencies` 阻断并级联跳过。
4. **json_schema 不认 JSONL**：直接 `json.loads` 整个文件会在第二行报 "Extra data" → 逐行解析回退。

## 目录

```
app/                     框架层（无领域知识）
  spec.py                 声明式管线定义（YAML → PipelineSpec）+ 严格校验 + 渲染
  runner.py               本地执行器（依赖拓扑 / 断点续跑 / 重试 / 超时 / 质量闸门 / 对账）
  spec2argo.py            编译器：PipelineSpec → Argo Workflow CRD（CI 提交编译产物）
  plugins.py              显式插件加载（spec.plugins → importlib）
  envcompat.py            PIPELINE_* 新名 + TTS_* 旧名兼容读取（一个版本）
  legacy_state_migration.py  旧状态库（tts_* 表）→ pipeline_* 的原地迁移（兼容层，下一版本删除）
  state.py                pipeline_run / pipeline_item 状态表 + 对账
  checks.py               质量检查插件注册表 + 4 个通用检查
  resume.py               输入指纹与跳过判定
  artifacts.py            对象存储契约（本地目录 / S3 兼容双实现）
  objstore.py             对象存储 CLI（集群内 put/get）
  argo_io.py              Argo outputs / parameters 写入
  metrics.py              Prometheus 指标（Counter/Gauge/Histogram + textfile）
  service.py              标准库 HTTP 常驻服务（探针 / 同步异步 / 优雅停机）
  etl_steps.py            ETL 示例三步
  dataset_steps.py        数据集示例三步（seed / clean / shard）
  manifest.py             确定性分片 + manifest 校验
  requirements.txt        框架层依赖（仅 PyYAML）
examples/tts-asr/         领域插件示例（TTS 生成 + ASR 回检 + 对账）
  README.md               示例定位、依赖、运行方式、与框架的边界
  docs/VERIFICATION.md    句子级漏读三态判定的方法与阈值标定
  pipelines/tts.yaml      示例管线（声明 plugins: [checks_tts]）
  k8s/                    示例的 Argo 编排（文档级）
  data/                   示例书籍数据（无真实业务内容）
pipelines/                通用管线定义（dataset-build.yaml / etl-demo.yaml）
k8s/                      CI 基础设施（object-store / artifact-repository / 历史 smoke）
docs/                     架构（ARCHITECTURE.md）与计划（ROADMAP.md）
tests/                    离线测试（框架 96 例）；领域示例测试在 examples/tts-asr/tests/（4 例）
Dockerfile                通用 CI 镜像（moto S3 + app/）
```

镜像构建：`docker build -f Dockerfile -t specargo:ci .`（构建上下文为仓库根；CI 与
`pipelines/*.yaml` 里的 `k8s.image` 都按 `specargo:ci` 引用）。

## English Summary

**specargo** is a declarative batch data-pipeline framework: define a pipeline once in YAML,
run it locally with the built-in runner, or compile the same spec into an Argo Workflows CRD
for cluster execution. It provides framework-level checkpoint/resume (input fingerprints),
dependency semantics, retry policies, quality gates, item-level state registry and
reconciliation, object-storage artifact contracts, Prometheus metrics, and a zero-dependency
HTTP worker service. The framework layer contains no domain knowledge — domain capabilities
are loaded explicitly through `plugins:`. A real-world TTS/ASR pipeline is kept as a
domain-plugin example under `examples/tts-asr/`. See `docs/` for architecture and roadmap.

## License

MIT
