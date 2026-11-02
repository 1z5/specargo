# ROADMAP

按价值 / 风险排序的后续工作。已完成项见 `README.md` 的「已验证」章节。

## 已完成

### ✅ spec2argo 编译器（同一份 spec，两个执行目标）

`app/spec2argo.py` 把 PipelineSpec 编译成 Argo CRD：

| PipelineSpec | Argo CRD |
|---|---|
| stages + dependencies | dag.tasks + dependencies |
| params | workflow arguments + `{{workflow.parameters.*}}` 注入 |
| partitions: list / range / discover | discover 任务 + `withParam` 引用其对象数组输出扇出 |
| RetryPolicy | retryStrategy（总是输出；limit=0 是显式设计意图） |
| Stage.timeout | template timeout |
| checks / check_config | 独立 `gate-<stage>` 任务（跑 checks.py 插件，limit=0） |
| artifacts / consumes | 自动接线对象存储路径契约（上游 put、下游 get，key 一致） |

验收已达成：CI 编译 `pipelines/etl-demo.yaml` → argo lint 通过 → 提交编译产物跑集群冒烟
（31/31 Pod、5 分区、Succeeded）；编译产物另有"零残留模板变量"断言。
本地 runner 与集群执行自此共享同一份 spec；`k8s/workflow-etl-smoke.yaml` 降级为对照参考。

已知边界：编译器覆盖 container 命令 + checks + artifacts + 参数 / 扇出 / 检查配置映射，
以及可选的 `k8s.journal`（状态日志 + onExit 对账）；script 型 template、
synchronization、suspend 尚未映射；集群侧断点续跑（skip）未实现（见下节「已知边界」）。

### ✅ P0 正确性修复（本轮）

| # | 问题 | 修复 |
|---|---|---|
| 1 | spec 无结构校验：空 / 拼错 YAML 静默全绿、依赖拼错下游照跑、`when` 半个特性 | `spec.py` 新增 `validate_spec()`：必填 / 命名 / 未知字段 / 依赖存在性与成环 / artifact 闭合 / 时长与重试枚举 / `when` 显式拒绝；discover 失败与非法类型都转为 `SpecError` |
| 2 | 编译器只对 `etl-demo` 形状成立：params 不编 arguments、variant/dataset 扇出恒空、check_config 不解析 | `spec2argo.py` 编译全部 params 为 workflow arguments；`{{params.x}}` → `{{workflow.parameters.x}}`；扇出改为 `{key,dataset,variant}` 对象；discover 输出对象数组；gate 配置编译期解析；编译产物做"零残留模板变量"自检 |
| 3 | 集群 gate 读节点本地路径、不 objstore get（靠同节点调度巧合通过） | gate 任务先按同一路径契约 `objstore get` 再跑 checks；脚本加 `set -e` |
| 4 | `service --state-db` 即不可用：readyz 恒 503、状态写库异常被吞 | 连接 `check_same_thread=False` + 单锁串行；readyz 带锁探测；写库失败记录并打日志 |
| 5 | 无 checks 的 stage 永不 resume、对账必失败（隐性强制每 stage 配闸门） | 终态语义：有 checks 只认 `verified`，无 checks 的 `generated` 即成功终态；resume 与对账共用同一判据 |
| 6 | `runner` 要求 `--workdir` 预先存在（新手第一跑就失败） | Runner 自动创建 workdir |
| 7 | 非拓扑序声明 stages 会错序执行且假绿；依赖阻断不级联（a 失败 → b 跳过 → c 照跑） | Runner 按依赖拓扑排序执行；跳过态（blocked）也级联阻断 |
| 8 | 类型错误泄漏原生异常（`retry: 3` → AttributeError、字符串列表被拆字符、`retry.limit: 2.5` 被静默截断等）、consumes 与 dependencies 不交叉校验 | `load_spec` 严格类型校验（列表 / 映射 / 字符串 / 重试结构 / 数值必须是整数）+ 参数名与分区 key 字符集校验 + consumes 的生产者必须是上游依赖；discover 加载期加超时；S3 `list` 保留尾斜杠前缀语义（防 `runs/r1` over-match `runs/r10`） |
| 9 | dataset-build 的 gate 深度校验依赖 shards 目录（不是 artifact、跨 Pod 取不到）→ 集群必红 | 深度自校验移入 `shard` stage 命令；gate 只做可跨 Pod 取回的 manifest 结构校验 |
| 10 | 自动生成的 objstore `--file` 未加引号（路径含空格失败）；gate 未声明 dataset/variant 输入 | 路径统一 `shlex.quote`；gate inputs/args 补齐三个维度 |
| 11 | 文档与实际不符（死命令 / 数字 / 夸大） | README / ARCHITECTURE / ROADMAP 按实测证据校正 |

新增回归：`tests/test_p0_regressions.py`；离线套件合计 **101 例**（框架 97 + 领域示例 4），全部通过。
验证方式：本地四套件 + ETL / dataset 管线 E2E + 编译产物零残留 + 独立 Argo 模拟器
（每个 task-item 独立目录、只共享对象存储）跑通 etl-demo 与 dataset-build 的编译产物；
真实集群侧由 CI 的 kind smoke 继续验证。

**已知边界（P0 确认，未在 P0 内解决）**：

- 集群 gate 只支持**单文件** artifact 的跨 Pod 校验；目录 / 多产物 artifact（如 shards 目录）不支持。
- 分区 key / dataset / variant 限制为 `^(?!\.+$)[\w.-]+$`（Unicode 字母 / 数字 / 下划线 / 点 / 连字符；额外排除纯 `.` / `..` 路径段）；discover 输出在加载期按整行解析、运行期也做同一校验。含空格 / 引号 / `$` 的值会破坏路径与 shell，拒绝。
- artifact key 的空段 / `.` / `..` 校验已下沉到 `artifacts.py`，本地目录与 S3 实现共用同一套规则（此前只有本地实现校验）。
- 参数值按**文本替换**注入命令：值里的 shell 元字符（如 `$`）会被目标 shell 二次解释，含此类字符需在 spec 内自行引号 / 转义。
- gate 已改为调用 `pipeline_cli.py gate`（不再内嵌 Python 源码）。
- local runner 按依赖拓扑排序执行，不要求 YAML 里 stages 的书写顺序是拓扑序；编译到 Argo 由 DAG dependencies 保证。

- 旧状态库迁移边角：旧表与新表并存时只告警、不自动合并（避免误判数据归属）；`schema_version` 只升不降。
- 集群插件加载需要领域镜像内含插件模块，并由 `k8s.plugin_path` 指明路径；通用镜像不带领域插件。

**仍未做**：集群侧断点续跑（skip）；`onExit` 需要 `k8s.journal: true` 显式开启（默认关闭，CI 未覆盖该路径的真集群行为）。

### ✅ 改名 `specargo` + TTS 归档 `examples/tts-asr/`（本轮）

- **仓库改名**：GitHub `1z5/tts-pipeline-argo` → `1z5/specargo`（旧 URL 301 重定向已验证）；
  README 标题、badge、`gh workflow run -R`、本地 remote、仓库描述 / topics 已同步。
- **代码归档**：TTS/ASR 领域实现整体移入 `examples/tts-asr/`（含 `pipelines/tts.yaml`、
  `k8s/workflow.yaml`、`cronworkflow.yaml`、`Dockerfile`、`requirements.txt`、`docs/VERIFICATION.md`、
  领域测试）；框架层 `app/**` 不含领域字样（CI 纯净度守卫，仅 `envcompat.py` /
  `legacy_state_migration.py` 两个兼容文件例外）。
- **契约中性化**：环境变量 `PIPELINE_RUN_ID` / `PIPELINE_STATE_DB` / `PIPELINE_ARGO_*` /
  `PIPELINE_INPUT_DIR`（workflow 本地变量，不回退；旧 `TTS_RUN_ID` / `TTS_STATE_DB` / `TTS_ARGO_*` 由 `envcompat.py` 回退一版）；镜像 / kind 集群名
  `specargo:ci` / `specargo`；状态表 `pipeline_run` / `pipeline_item`，列
  `dataset` / `partition` / `variant`（旧库启动时原地迁移）。
- **插件化**：框架层不再 `import checks_tts`；领域检查由 spec 顶层 `plugins: [模块名]`
  经 `app/plugins.py` 显式加载（`examples → framework` 单向依赖）。

## 下一步（P1）

### 1. 双目标语义对等（已完成一部分）

- ✅ gate / state / reconcile 已编译为对 `pipeline_cli.py` 的调用，不再内嵌 Python 片段；
- ✅ 集群侧可选状态日志（`k8s.journal: true`）+ `onExit` 对账 + webhook；
  ✅ CI kind smoke 额外提交 journal=on 变体，真实集群验证 onExit 节点与状态日志；
- ⬜ 集群侧断点续跑（skip）未实现：需要在 stage 启动时查状态日志并跳过；
- ⬜ 真实部署的并发/规模验证（对象存储状态日志的对象数随 stage×partition 增长）。

### 2. 包化（pip install 可用）

`pyproject.toml` + `src/pipeline/` 包结构 + `[project.scripts]`；extras：`tts` / `s3` / `dev`。
包化后 `$PYTHONPATH` 不再是运行前提，示例可以只当普通用户目录使用。
（当前是单仓库脚本 + `sys.path` 注入的最小形态。）

### 3. `RuntimeConfig`（替代编译期硬编码）

把镜像 / 命名空间 / 挂载 / `run_id` 环境变量名 / objstore endpoint / 整批 deadline 抽成
`RuntimeConfig`（来源：spec `runtime:` 节、`--runtime runtime.yaml` 或 env），
编译器不再持有任何业务默认值（`specargo:ci` 移到 CI 的 runtime 文件）。

### 4. `StateStore` 协议

`state.py` 拆成 `StateStore` Protocol + `SqliteStateStore` 实现 + 泛型对账；
集群写路径先明确"不写"或"单写者"，再谈并发。Postgres / Redis 等后端在有真实多写者需求前不做。

### 5. golden 编译产物测试

用固定 spec + 固定 runtime 生成编译产物快照（golden），CI 断言产物逐字节稳定；
配合 `argo lint` 与"零残留模板变量"断言，把编译器的行为变化变成可 review 的 diff。

## 进行中 / 遗留

### 1. service 的 job 状态落 SQLite

当前 job 记录纯内存：服务重启后 `GET /tasks/{id}` 返回 404，但状态表里还有记录。
应把 job 记录也落 `state.py` 的表（或新建 `service_job` 表），重启后可查、可重试。

## 扩展验证

### 2. 规模测试：千级分区

`partitions: {from: range, start: 1, end: 200}` 跑一轮，观测：
- workflow-controller 的队列深度与 API server QPS
- Pod 调度延迟、`parallelism` 上限
- 对象存储吞吐（当前 moto 是进程内实现，需换真 MinIO 才能测）

产出数据写进 README，补上"千级 Pod 未实测"这一条。

### 3. 失败注入测试

故意让某分区的质量闸门（`gate-<stage>`）失败，验证：
- task 失败 → 下游被依赖语义跳过 → 对账报 missing → 退出码非 0
- `onExit` 仍收口
- 告警通道收到通知

### 4. WorkflowTemplate + 版本化

`k8s/workflowtemplate.yaml` + `workflowTemplateRef`，演示 DAG 改版如何灰度与回滚
（对应"工作流版本管理与发布"）。

## 已知未覆盖

- ReadWriteMany PVC（当前用对象存储绕开，未验证真 RWX）
- 多副本高可用（object-store / workflow-controller 都是单副本）
- 生产级对象存储（MinIO / OSS / S3）的真实吞吐与成本
- gRPC 服务形态（当前只有 HTTP）
- 集群侧断点续跑（状态日志只用于 onExit 对账）
