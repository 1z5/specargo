# 架构与数据流

本文描述项目的分层结构、模块职责与数据流转。项目由三部分组成：

| 组成 | 入口 | 定位 |
|---|---|---|
| **批处理框架** | `spec.py` + `runner.py` | 声明式管线定义 + 单机执行器（含断点续跑/质量闸门/对账） |
| **常驻处理服务** | `service.py` | Python 长驻服务（探针/同步异步/优雅停机/失败重处理） |
| **领域管线** | `pipeline.py` + `verify_audio.py` + `tts_verify/` | TTS 生成 + ASR 回检的具体实现（框架的一个消费者） |

同一套可靠性语义（`resume` / `state` / `checks` / `metrics`）被批处理与服务两侧共享。

---

## 1. 总体分层

```
┌─────────────────────────────────────────────────────────────────┐
│ 定义层  pipelines/*.yaml ── spec.py ──► PipelineSpec             │
│         （stage / dependencies / retry / timeout / checks / 分区）│
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴────────────────┐
              ▼                                ▼
┌──────────────────────────────┐  ┌──────────────────────────────┐
│ 执行层 · 批处理               │  │ 执行层 · 常驻服务             │
│ runner.py（单机执行器）       │  │ service.py（HTTP + 线程池）   │
│  · 依赖语义（上游失败则跳过） │  │  · /healthz /readyz 探针      │
│  · 框架级断点续跑             │  │  · /process 同步 /tasks 异步  │
│  · retryStrategy 分层         │  │  · /tasks/{id}/retry 重处理   │
│  · 超时 + 质量闸门            │  │  · SIGTERM 四步优雅停机       │
└──────────────┬───────────────┘  └──────────────┬───────────────┘
               │                                  │
               └───────────────┬──────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 可靠性层（两侧共享）                                              │
│  resume.py   输入指纹 → 跳过判定（断点续跑 + 幂等）               │
│  state.py    item 级状态表 + expected vs actual 对账              │
│  checks.py   质量检查插件注册表                                   │
│  metrics.py  Prometheus 指标（进度/积压/失败率/对账缺失）          │
│  notify.py   onExit 收口告警                                      │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ 存储层                                                           │
│  artifacts.py   对象存储抽象（本地目录 / S3 兼容双实现）           │
│  objstore.py    对象存储 CLI（K8s 里按路径契约跨 Pod 传数据）      │
│  state 的 SQLite（同 schema 可迁 MySQL/PG）                       │
└─────────────────────────────────────────────────────────────────┘
```

**关键原则**：
1. **状态必须落在 Pod/进程之外**——Argo 每个 task 是独立 Pod，服务重启会丢内存；
   所以 item 状态进 SQLite，产物进对象存储。
2. **跨 Pod 数据走对象存储的路径契约**，不用 hostPath（多节点下是节点本地的），
   也不用扇出 artifact 引用（依赖索引匹配，脆弱）。
3. **重试要分失败类型**：环境性失败（限流/5xx/网络）才重试；
   确定性失败（内容缺失/格式错）重试只是重复烧钱，应进隔离区走修复分支。

---

## 2. 模块职责

### 定义与执行

| 模块 | 职责 | 维护注意 |
|---|---|---|
| `spec.py` | YAML → `PipelineSpec`（Stage/Partition/RetryPolicy）；模板渲染；变量校验 | 渲染默认严格模式，未定义变量抛 `SpecError` |
| `runner.py` | 单机 stage 执行器：依赖语义、断点续跑、重试、超时、质量闸门、对账收口 | 近似"单机版 workflow-controller" |
| `service.py` | 常驻服务：探针、同步/异步接口、失败重处理、优雅停机 | job 记录在内存，状态登记进 SQLite |

### 可靠性层（批处理与服务共享）

| 模块 | 职责 | 接口 |
|---|---|---|
| `resume.py` | 输入 sha256 指纹 + 产物存在性判定 | `should_skip(path, text)` / `write_fingerprint()` |
| `state.py` | item 状态表（dataset/partition/variant/stage 四元组主键）+ 对账 | `upsert_generic_item()` / `reconcile_generic()` |
| `checks.py` | 检查插件注册表 + 4 个通用检查 | `register()` / `run_check()` |
| `checks_tts.py` | ASR 三态判定注册为插件（领域知识不进框架） | `@register("asr_three_state")` |
| `metrics.py` | Counter/Gauge/Histogram + Prometheus text 输出 | `METRICS` 单例 / `write_file()` |
| `notify.py` | run 级汇总 + webhook 告警（可插拔） | `build_summary()` / `send_webhook()` |

### 存储层

| 模块 | 职责 |
|---|---|
| `artifacts.py` | 对象存储**抽象**：`ArtifactStore` 协议 + 本地目录 / S3 兼容双实现；key 目录穿越防护；`digest()` 用于变更检测 |
| `objstore.py` | 对象存储 **CLI**（K8s Pod 内用）：按路径契约 `runs/<run_id>/<partition>/<stage>.<ext>` put/get |

### 领域层（TTS 管线，框架的消费者）

| 模块 | 职责 |
|---|---|
| `pipeline.py` | 生成编排：逐文件判定跳过、限速、状态登记、单书 CLI |
| `tts_client.py` | OpenAI TTS 封装：调用、指数退避、影子文件与输入指纹 |
| `verify_audio.py` | 校验 CLI：扫描 → 转录 → 三态判定 → 修复 → 报告 → 对账登记 |
| `tts_verify/` | 校验核心：`normalizer` / `sentence_matcher` / `comparator` / `reporter` |
| `summary.py` | 对账 CLI：expected vs actual + Argo 输出参数 + 非零退出码 |
| `etl_steps.py` | ETL demo 三步（证明框架与 TTS 无关） |

---

## 3. 关键数据流

### 3.1 批处理流（runner）

```
load_spec(pipelines/x.yaml)          # 严格校验：使用变量 ⊆ 声明参数
  └─► for stage in spec.stages:
        for partition in spec.partitions:
          ├─ 依赖判定：上游 failed/skipped → 本 stage 标 skipped（不被下游成功掩盖）
          ├─ 断点续跑：该 (stage×partition) 已 verified → 跳过
          ├─ 执行：render(command) → subprocess（timeout + retryStrategy）
          ├─ 质量闸门：run_check() 不过 → failed（阻断交付）
          └─ 状态登记：upsert_generic_item(dataset, partition, variant, stage)
  └─► 收口：按 (dataset, variant) 分组对账 → 报告 + 指标 + Argo 输出参数 + 退出码
```

### 3.2 常驻服务流（service）

```
POST /tasks {payload}
  └─► 落 job 记录（含 payload，供 retry 复用）→ 线程池执行
        └─► 处理模块.process(payload) → 成功登记 verified / 失败登记 failed
POST /tasks/{id}/retry
  └─► 仅 failed 可重试；用**原始 payload** 重新入队（无 payload 则拒绝）

SIGTERM → begin_drain()（/readyz 立刻 503）
        → 等 Endpoints 传播（等价 K8s preStop sleep）
        → 等在途任务完成（有上限）
        → checkpoint → 停止 accept → 退出
```

### 3.3 集群流（Argo，CI 提交的是 `spec2argo` 编译产物）

```
pipelines/etl-demo.yaml ──spec2argo──► Workflow CRD ──► argo submit
  └─► discover（读 ConfigMap 输入 → 输出 JSON 数组）
        └─► extract / transform / load（各自 withParam 引用 discover 输出做动态扇出）
              └─► gate-<stage>（跑 checks.py 插件，不过检即失败）
        跨 Pod 数据经对象存储路径契约传递（objstore.py）
```

> 手写版 `k8s/workflow-etl-smoke.yaml` 是早期实现（含 `onExit` 收口），
> 已降级为对照参考；编译产物目前尚未生成 `onExit`（见 `docs/ROADMAP.md`）。

---

## 4. 与 Argo Workflows 的对应关系

| 本项目概念 | Argo 概念 |
|---|---|
| `spec.stages` + `dependencies` | DAG `tasks` + `dependencies` |
| `partitions` + `withParam` 扇出 | `withItems` / `withParam` |
| `RetryPolicy`（limit/policy/backoff） | `retryStrategy` |
| `Stage.timeout` / 整批超时 | template `timeout` / `activeDeadlineSeconds` |
| `resume.should_skip()` | 任务幂等（Argo 只保证"再执行一次"，安全性由任务自己保证） |
| `state.upsert_generic_item()` | 外部状态存储（Pod 之间不能靠 emptyDir 共享） |
| `reconcile_generic()` | DAG 末节点 / `onExit` 对账 |
| `metrics.METRICS` | controller 指标 + 自定义指标 |
| `service.py` 的探针与停机 | liveness / readiness / startup + preStop + terminationGracePeriod |

> **已实现**：把 `PipelineSpec` 编译成 Argo CRD 的 `spec2argo` 编译器
> （`app/spec2argo.py`，CI 提交编译产物跑集群）。覆盖范围 container 命令 +
> checks + artifacts，未覆盖 script 型 template、synchronization、suspend；
> 详见 `docs/ROADMAP.md`。

---

## 5. k8s/ 目录说明

| 文件 | 状态 |
|---|---|
| `workflow-etl-smoke.yaml` | **CI 实测跑通**（kind 多节点 + Argo v3.7.18） |
| `object-store.yaml` / `artifact-repository.yaml` | **CI 实测**（S3 兼容端点 + 默认仓库注解） |
| `workflow.yaml` / `cronworkflow.yaml` | **文档级示例**：TTS 管线的 Argo 版本，引用 `registry.example.com` 镜像与 3 个 PVC，未在 CI 部署（需自备镜像与存储）。仅通过 `argo lint` schema 校验 |
