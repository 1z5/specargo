# 架构与数据流

本文描述 **specargo** 的分层结构、模块职责、插件机制与数据流转。
核心目标：**同一份 YAML spec，本地 runner 可跑，也能编译成 Argo CRD 上集群**；
框架层（`src/specargo/`，pip install 可用）不含任何领域知识，领域能力通过插件显式加载。

## 0. 组成

| 组成 | 入口 | 定位 |
|---|---|---|
| **框架层** | `spec.py` + `runner.py` + `spec2argo.py` | 声明式管线定义 / 本地执行器 / 编译器（spec → Argo CRD） |
| **常驻服务** | `service.py` | 标准库 HTTP 长驻服务（探针 / 同步异步 / 优雅停机 / 失败重处理） |
| **可靠性层** | `resume.py` `state.py` `checks.py` `metrics.py` | 断点续跑 / 状态登记与对账 / 质量闸门 / 指标 |
| **存储层** | `artifacts.py` `objstore.py` `argo_io.py` | 对象存储契约 / 集群内 CLI / Argo 输出 |
| **插件与兼容** | `plugins.py` `envcompat.py` `legacy_state_migration.py` | 领域插件显式加载 / 旧环境变量兼容 / 旧状态库原地迁移（兼容层，一个版本后删除） |
| **通用示例与工具** | `etl_steps.py` `dataset_steps.py` `manifest.py` + `pipelines/*.yaml` | 两条通用示例管线；确定性分片与 manifest |
| **领域示例** | `examples/tts-asr/` | 来自真实生产的领域插件示例（TTS 生成 + ASR 回检 + 对账） |
| **CI 基础设施** | `Dockerfile` + `k8s/*.yaml` | kind smoke 的通用镜像、S3 兼容端点、artifact repository |

同一套可靠性语义（`resume` / `state` / `checks` / `metrics`）被批处理与服务两侧共享。

---

## 1. 总体分层

```
┌─────────────────────────────────────────────────────────────────────┐
│ 定义层   pipelines/*.yaml ── spec.py ──► PipelineSpec                │
│          （stage / dependencies / retry / timeout / checks / 分区）   │
└─────────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┼───────────────────┐
              ▼                ▼                   ▼
┌───────────────────────┐ ┌────────────────┐ ┌───────────────────────────┐
│ 执行层 · 本地批处理     │ │ 执行层 · 服务   │ │ 编译层                     │
│ runner.py             │ │ service.py     │ │ spec2argo.py               │
│ · 依赖拓扑 + 阻断      │ │ · /healthz     │ │ · DAG / withParam 扇出     │
│ · 框架级断点续跑       │ │ · /readyz      │ │ · retryStrategy / timeout  │
│ · retryStrategy 分层   │ │ · /process 同步│ │ · gate-<stage> 质量闸门    │
│ · 超时 + 质量闸门      │ │ · /tasks 异步  │ │ · artifacts 路径契约       │
│ · 对账收口（退出码）   │ │ · SIGTERM 停机 │ │ ──► Argo Workflow CRD      │
└───────────┬───────────┘ └───────┬────────┘ └───────────────────────────┘
            │                     │
            └──────────┬──────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 可靠性层（两侧共享）                                                  │
│  resume.py   输入指纹 → 跳过判定（断点续跑 + 幂等）                    │
│  state.py    pipeline_run / pipeline_item 状态表 + expected vs actual │
│  checks.py   质量检查插件注册表（4 个通用检查 + 插件注册）              │
│  metrics.py  Prometheus text 格式指标                                 │
└─────────────────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 存储层                                                               │
│  artifacts.py  对象存储契约（本地目录 / S3 兼容双实现）                │
│  objstore.py   对象存储 CLI（集群内按路径契约跨 Pod 传数据）            │
│  argo_io.py    Argo outputs / parameters 写入                        │
│  state 的 SQLite（同 schema 可迁 MySQL / PostgreSQL）                  │
└─────────────────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 插件层                                                               │
│  plugins.py    spec 顶层 plugins: [模块名] → importlib 显式加载       │
│  envcompat.py  PIPELINE_* 新名优先、TTS_* 旧名回退（一个版本）         │
└─────────────────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 示例层                                                               │
│  通用：pipelines/etl-demo.yaml、pipelines/dataset-build.yaml          │
│  领域：examples/tts-asr/（自己的 README / docs / k8s / 依赖）          │
└─────────────────────────────────────────────────────────────────────┘
```

**关键原则**：

1. **状态必须落在 Pod / 进程之外**——编排层每个 task 是独立 Pod，服务重启会丢内存；
   所以 item 状态进 SQLite，产物进对象存储。
2. **跨 Pod 数据走对象存储的路径契约**，不用 hostPath（多节点下是节点本地的），
   也不用扇出 artifact 引用（依赖索引匹配，脆弱）。
3. **重试要分失败类型**：环境性失败（限流 / 5xx / 网络）才重试；
   确定性失败（内容缺失 / 格式错）重试只是重复烧钱，应进隔离区走修复分支。
4. **框架与领域是单向依赖**：`examples → framework`；框架层不 import 任何领域模块
   （CI 有 grep 守卫），领域能力只能通过插件清单显式加载。

---

## 2. 模块职责

### 2.1 定义与执行

| 模块 | 职责 | 维护注意 |
|---|---|---|
| `spec.py` | YAML → `PipelineSpec`（Stage / Partition / RetryPolicy）；模板渲染；**严格校验** | 未知字段 / 依赖成环 / 类型错误 / `when` 未支持等在加载期抛 `SpecError`；渲染默认严格模式 |
| `runner.py` | 本地 stage 执行器：依赖拓扑排序与阻断级联、断点续跑、重试、超时、质量闸门、对账收口 | 近似"单机版 workflow-controller"；`--workdir` 自动创建；终态语义见 §5.1 |
| `spec2argo.py` | 编译器：`PipelineSpec` → Argo Workflow CRD | 覆盖 container 命令 + checks + artifacts + 参数 / 扇出 / 检查配置；对产物做"零残留模板变量"自检 |

### 2.2 常驻服务

| 模块 | 职责 | 维护注意 |
|---|---|---|
| `service.py` | 标准库 HTTP 常驻服务：`/healthz` `/readyz`、`/process` 同步、`/tasks` 异步、`/tasks/{id}/retry`、`/version`、SIGTERM 四步优雅停机 | job 记录在内存（落库见 ROADMAP）；Processor 扩展点当前只有内置 `echo` 示例 |

### 2.3 可靠性层（批处理与服务共享）

| 模块 | 职责 | 接口 |
|---|---|---|
| `resume.py` | 输入 sha256 指纹 + 产物存在性判定 | `should_skip(path, text)` / `write_fingerprint()` |
| `state.py` | `pipeline_run` / `pipeline_item` 状态表（dataset/partition/variant/stage 四元组主键）+ 对账；打开旧库时调用兼容层迁移 | `upsert_item()` / `reconcile_generic()` |
| `checks.py` | 检查插件注册表 + 4 个通用检查（`file_nonempty` / `json_schema` / `row_count` / `command`） | `register()` / `run_check()` / `available()` |
| `journal.py` | 集群侧状态日志：每个 (dataset, partition, variant, stage) 写一个对象存储 JSON；分区清单写 `_run/partitions.json`；onExit 读取后用与本地同一个 `evaluate_items()` 对账 | `put_status()` / `load_statuses()` / `put_partitions()` / `get_partitions()` |
| `pipeline_cli.py` | 集群侧调用框架语义的 CLI：`gate` / `state put` / `state put-remote` / `reconcile-journal`；编译产物不再内嵌 Python 片段 | 见 `--help` |
| `notify.py` | 通用收口告警：对账结果渲染 + 可插拔 webhook | `render_summary()` / `send_webhook()` |
| `metrics.py` | Counter/Gauge/Histogram + Prometheus text 输出 | `METRICS` 单例 / `write_file()` |

### 2.4 存储层

| 模块 | 职责 |
|---|---|
| `artifacts.py` | 对象存储**抽象**：`ArtifactStore` 协议 + 本地目录 / S3 兼容双实现；key 目录穿越防护；`digest()` 仅本地实现提供、应用代码暂未调用 |
| `objstore.py` | 对象存储 **CLI**（K8s Pod 内用）：按路径契约 `runs/<run_id>/<partition>/<stage>.<ext>` put/get |
| `argo_io.py` | Argo 输出契约：outputs / parameters 写入（本地联调可用 `PIPELINE_ARGO_*` 环境变量） |

### 2.5 插件与兼容

| 模块 | 职责 |
|---|---|
| `runtime.py` | `RuntimeConfig`：执行目标运行时配置（镜像/命名空间/SA/挂载/对象存储/deadline/podGC/插件路径）；来源优先级 = 内置默认 < spec `runtime:` 节（旧名 `k8s:`）< CLI `--runtime`；固定契约 `PIPELINE_RUN_ID` / `runs` | 
| `plugins.py` | `load(modules, search_dirs=...)`：把 spec 所在目录及其父目录加入 `sys.path`，再 `importlib` 逐个加载；失败抛 `SpecError`（启动期失败，而不是跑到一半发现检查未注册） |
| `envcompat.py` | `getenv(name, legacy, default)`：新名 `PIPELINE_*` 优先、旧名 `TTS_*` 回退；旧前缀集中在此文件（一个版本后删除） |
| `legacy_state_migration.py` | 旧状态库迁移：`tts_run` / `tts_item(book_slug, chapter_num, voice, ...)` → `pipeline_run` / `pipeline_item(dataset, partition, variant, ...)`，原地 `ALTER TABLE RENAME`，幂等（一个版本后删除） |

> **框架纯净度守卫**：CI 对 `app/**` 做关键词 grep，领域字样只允许出现在
> `envcompat.py` / `legacy_state_migration.py` 这两个兼容文件；其余模块必须保持中立。

### 2.6 通用示例与工具

| 模块 | 职责 |
|---|---|
| `etl_steps.py` | 通用 ETL 示例三步（extract / transform / load），与领域无关 |
| `dataset_steps.py` | 数据集构建三步（seed / clean / shard），含隔离区 |
| `manifest.py` | 确定性分片（`sha256(id) % N`）+ manifest 生成与校验 CLI |

### 2.7 领域示例（`examples/tts-asr/`）

领域示例整体归档，框架层不引用其中的任何模块。示例内部模块职责、依赖与运行方式
见 [`examples/tts-asr/README.md`](../examples/tts-asr/README.md)；校验方法见
[`examples/tts-asr/docs/VERIFICATION.md`](../examples/tts-asr/docs/VERIFICATION.md)。
框架与示例的唯一接口是 **插件清单**（`plugins:`）与 **spec 语义 / 环境契约**。

---

## 3. 插件机制

领域检查不写进框架，而是放在示例目录里注册、由 spec 显式声明加载：

```yaml
# examples/tts-asr/pipelines/tts.yaml（片段）
name: tts-example
plugins: ["checks_tts"]
stages:
  - name: verify
    checks: [asr_three_state]
    # ...
```

```python
# examples/tts-asr/checks_tts.py（片段）
from checks import register

@register("asr_three_state")
def asr_three_state(artifact, config, ctx):
    ...
```

加载流程：

```
load_spec(path) ──► spec.plugins = ["checks_tts"]
   └─► plugins.load(spec.plugins, search_dirs=[spec 目录, spec 父目录])
         ├─ 把 search_dirs 临时加入 sys.path
         ├─ importlib.import_module("checks_tts")   # 模块 import 时通过 @register 注册
         └─ 失败 → SpecError（启动期失败）
   └─► runner / gate 从同一个 checks 注册表取实现
```

约束：

- **单向依赖**：`examples → framework`；`app/**` 不得出现领域字样（CI 有 grep 守卫），
  也不得反向 `import examples.*`。
- **显式优于发现**：不做 entry_points / 自动扫描；没在 `plugins:` 里声明的模块不会加载。
- **同一实现两侧共用**：本地 runner 的 `run_check()` 与集群 `gate-<stage>` 跑的是同一个注册表，
  不存在"本地绿、集群红"的两套检查实现。

---

## 4. 契约与兼容

| 契约 | 框架命名 | 兼容 / 迁移 |
|---|---|---|
| 运行与路径环境变量 | `PIPELINE_RUN_ID` / `PIPELINE_STATE_DB` / `PIPELINE_ARGO_OUTPUTS` / `PIPELINE_ARGO_PARAM_DIR` / `PIPELINE_ARGO_ARTIFACT_DIR`（workflow 内另有 `PIPELINE_INPUT_DIR`） | `TTS_RUN_ID` / `TTS_STATE_DB` / `TTS_ARGO_*` 由 `envcompat.py` 回退读取，保留一个版本；`PIPELINE_INPUT_DIR` 是编排本地变量，不做回退 |
| 状态库 | 表 `pipeline_run` / `pipeline_item`；列 `dataset` / `partition` / `variant` / `stage` / `status` / `attempt` / `run_id` | 打开旧库时由 `legacy_state_migration.py` 原地迁移（`ALTER TABLE RENAME`，幂等，数据保留）；`schema_version` 表记录版本 |
| 对象存储 key | `runs/<run_id>/<partition>/<stage>.<ext>` | run_id 取 `PIPELINE_RUN_ID`，编译器注入 `{{workflow.name}}` |
| 镜像 / 集群 | 镜像 `specargo:ci`；kind 集群名 `specargo` | — |
| 领域插件 | spec 顶层 `plugins: [模块名]` | `plugins.py` 按 spec 目录 / 父目录解析 |

---

## 5. 关键数据流

### 5.1 本地批处理流（runner）

```
load_spec(pipelines/x.yaml)                     # 严格校验 + 插件加载
  └─► 按依赖拓扑排序 stages（不要求 YAML 书写顺序）
        for stage in stages:
          for partition in partitions:
            ├─ 依赖判定：上游 failed/skipped（blocked）→ 本 stage 标 skipped，且级联到下游
            ├─ 断点续跑：该 (stage×partition) 已到成功终态 → 跳过
            │    · 有 checks 的 stage：只认 verified
            │    · 无 checks 的 stage：执行成功的 generated 即终态
            ├─ 执行：render(command) → subprocess（timeout + retryStrategy）
            ├─ 质量闸门：run_check() 不过 → failed（阻断交付）
            └─ 状态登记：upsert_item(dataset, partition, variant, stage, status, attempt, run_id)
  └─► 收口：按 (dataset, variant) 分组对账 → 报告 + 指标 + 退出码（漏数/未过检即非零）
```

### 5.2 常驻服务流（service）

```
POST /tasks {payload}
  └─► 落 job 记录（含 payload，供 retry 复用）→ 线程池执行
        └─► processor.process(payload) → 成功登记 verified / 失败登记 failed
POST /tasks/{id}/retry
  └─► 仅 failed 可重试；用**原始 payload** 重新入队（无 payload 则拒绝）

SIGTERM → begin_drain()（/readyz 立刻 503）
        → 等 Endpoints 传播（等价 K8s preStop sleep）
        → 等在途任务完成（有上限）
        → checkpoint → 停止 accept → 退出
```

### 5.3 编译与集群流（Argo）

```
pipelines/etl-demo.yaml ──spec2argo──► Workflow CRD ──► argo submit
  └─► discover（读 ConfigMap 输入 → 输出 JSON 数组；journal 开启时写 _run/partitions.json）
        └─► extract / transform / load（各自 withParam 引用 discover 输出做动态扇出）
              └─► gate-<stage>（objstore get 取回单文件 artifact → pipeline_cli.py gate）
        跨 Pod 数据经对象存储路径契约传递（objstore.py）
        编译产物自检：不得残留 {{params.*}} / {{partition.*}} 等 spec 模板变量
```

**可选集群收口（`k8s.journal: true`）**：

```
stage 完成 ─► pipeline_cli.py state put-remote（写 runs/<run>/_status/...，key 独立、并发安全）
gate 完成  ─► 覆盖同一 key 为 verified/failed
onExit     ─► pipeline_cli.py reconcile-journal（同一 evaluate_items 判定；
              分区清单缺失时仅"工作流已失败"才跳过，成功路径缺清单会失败）
              ├─ 差异为空：正常收口（可发 webhook）
              └─ 差异非空：onExit 非零退出 → 工作流失败（"跑完≠跑对"）
```

> 手写版 `k8s/workflow-etl-smoke.yaml` 是早期实现，已降级为对照参考。
> 集群侧断点续跑（skip）未实现：状态日志当前只用于 onExit 对账，不作为 resume 依据。

---

## 6. 与 Argo Workflows 的对应关系

| 本项目概念 | Argo 概念 |
|---|---|
| `spec.stages` + `dependencies` | DAG `tasks` + `dependencies` |
| `partitions` + 扇出 | `withParam`（discover 输出对象数组） |
| `RetryPolicy`（limit/policy/backoff） | `retryStrategy` |
| `Stage.timeout` / 整批超时 | template `timeout` / `activeDeadlineSeconds` |
| `resume` 终态判定 | 任务幂等（Argo 只保证"再执行一次"，安全性由任务自己保证） |
| `state.upsert_item()` | 外部状态存储（Pod 之间不能靠 emptyDir 共享） |
| `reconcile_generic()` / `evaluate_items()` | DAG 末节点 / `onExit` 对账（状态来源：本地 SQLite 或集群状态日志） |
| `metrics.METRICS` | controller 指标 + 自定义指标 |
| `service.py` 的探针与停机 | liveness / readiness / startup + preStop + terminationGracePeriod |
| `gate-<stage>` | 独立 Argo task + `retryStrategy.limit: 0` |

> **已实现**：`spec2argo` 编译器（`app/spec2argo.py`，CI 提交编译产物跑集群），
> 覆盖 container 命令 + checks + artifacts + 参数 / 扇出 / 检查配置映射；
> `k8s.journal: true` 时额外生成状态写入与 `onExit` 对账模板。
> 未映射 script 型 template、synchronization、suspend。

---

## 7. k8s/ 与镜像

| 文件 | 状态 |
|---|---|
| `object-store.yaml` | **CI 实测**：moto S3 兼容端点（`-H 0.0.0.0` 显式绑定），作为 Argo 的 artifact 后端 |
| `artifact-repository.yaml` | **CI 实测**：v3.x 默认 artifact repository（依赖 `workflows.argoproj.io/default-artifact-repository` 注解） |
| `workflow-etl-smoke.yaml` | **历史 CI 实测跑通**；当前已被编译产物取代，CI 不再提交该文件，仅作 `argo lint` 对照 |
| `examples/tts-asr/k8s/workflow.yaml` / `cronworkflow.yaml` | **领域示例（文档级）**：引用示例镜像 `registry.example.com` 与 PVC / Secret，未在 CI 部署，需自备资源 |

通用镜像（CI 与 `pipelines/*.yaml` 的 `k8s.image` 都按这个 tag 引用）：

```bash
docker build -t specargo:ci .        # 构建上下文为仓库根
```

`Dockerfile` 把 `specargo` 包 `pip install` 进镜像（框架层，纯标准库 + PyYAML）并装 `moto[server]`；
编译产物用 `python3 -m specargo.<module>` 调用框架能力，不依赖镜像里的源文件。
领域示例的镜像在 `examples/tts-asr/Dockerfile`（含 openai / faster-whisper 重依赖），不属于通用镜像。

---

## 8. 已知边界

- **编译器覆盖**：container 命令 + checks + artifacts + 参数 / 扇出 / 检查配置；
  可选 `k8s.journal`（状态日志 + onExit 对账）；script 型 template、synchronization、suspend 未映射。
- **集群 gate**：只支持单文件 artifact 的跨 Pod 校验；目录 / 多产物 artifact 不支持
  （dataset-build 的深度校验因此放在 stage 命令内，gate 只做 manifest 结构校验）。
- **命名约束**：分区 key / dataset / variant 限定 `^(?!\.+$)[\w.-]+$`，含空格 / 引号 / `$` 的值会被拒绝。
- **参数注入**：参数值按文本替换进命令，shell 元字符会被目标 shell 二次解释；含此类字符需在 spec 内自行引号 / 转义。
- **gate 执行**：已改为调用 `pipeline_cli.py gate`（不再内嵌 Python 源码），行为可在本地单测；
  集群 gate 仍只支持单文件 artifact。
- **集群状态**：状态日志依赖对象存储；SQLite 状态表只用于本地。多写者场景未提供 `StateStore` 协议，集群侧断点续跑未实现。
- **本地 vs 集群顺序**：local runner 按依赖拓扑排序，不要求 YAML 书写顺序；编译到 Argo 由 DAG dependencies 保证。
- **安全边界**：onExit 对账信任对象存储状态对象（部署侧应限制 bucket 写入权限，防止伪造 verified）；
  `notify_webhook` 仅支持 http/https；`k8s.objstore_env` 不可覆盖框架保留 env；
  `stage.command` / discover 命令属于 spec 作者代码，视为可信输入。
- **未验证**：千级 Pod 规模、ReadWriteMany PVC、生产级对象存储吞吐、多副本高可用、gRPC 服务形态。
