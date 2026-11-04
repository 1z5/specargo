# ROADMAP

按价值 / 风险排序的后续工作。项目状态快照（已验证 / 未验证 / 边界）见
[`STATUS.md`](STATUS.md)；已完成项的用户向说明见 `README.md` 的「已验证」章节。

## 已完成

### ✅ spec2argo 编译器（同一份 spec，两个执行目标）

`src/specargo/spec2argo.py` 把 PipelineSpec 编译成 Argo CRD：

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
以及可选的 `k8s.journal`（状态日志 + onExit 对账）与 `k8s.resume`（集群侧 task 级断点续跑，
已完成并真集群验证，见 1.2）；script 型 template、synchronization、suspend 尚未映射；
「不拉 Pod」形态的 skip（WorkflowTemplate + `when`）仍未做。

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

新增回归：`tests/test_p0_regressions.py`；离线套件合计 **172 例**（框架 168 + 领域示例 4），全部通过。
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

**仍未做 / 注意**：
- 「不拉 Pod」形态的集群 skip（需要 WorkflowTemplate + `when`）；当前的 `k8s.resume` 是
  **task 级**跳过（stage 启动先查状态日志，终态则 exit 0），Pod 仍会被调度。
- `k8s.journal: true` 默认关闭；CI 已覆盖该路径的真集群行为（journal=on 变体，断言 onExit +
  对账 PASS + 15 个状态对象）。
- 集群 resume 的前提是 **run_id 稳定**：workflow 名固定，跨提交续跑要先删除同名 workflow
  再重提（Argo 默认会 AlreadyExists 挡住第二次提交）；CI 4.6 就是这么做的。
- resume 跳过只依据状态日志，**不校验产物是否仍可取**：若对象存储里上一轮的产物已过期，
  下游 gate 的 `objstore get` 会失败（fail-loud，不会静默错），但报错指向"取数失败"而非
  "状态说完成但产物缺失"，排障时需要注意。

### ✅ 改名 `specargo` + TTS 归档 `examples/tts-asr/`（本轮）

- **仓库改名**：GitHub `1z5/tts-pipeline-argo` → `1z5/specargo`（旧 URL 301 重定向已验证）；
  README 标题、badge、`gh workflow run -R`、本地 remote、仓库描述 / topics 已同步。
- **本地工作副本改名**：`/Users/zephyr/dev/tts-argo` → `/Users/zephyr/dev/specargo`（与 GitHub 同名）。
  editable 安装的 `.pth` / `direct_url.json` 原本指向旧路径，改名后重跑 `pip install -e .` 刷新；
  验证：`import specargo` 解析到新路径、五个 console script 正常、离线 160 例全绿、
  golden 0 漂移、三条管线编译零残留、本地 E2E（dataset 链路 + etl-demo 二次 resume）通过。
- **代码归档**：TTS/ASR 领域实现整体移入 `examples/tts-asr/`（含 `pipelines/tts.yaml`、
  `k8s/workflow.yaml`、`cronworkflow.yaml`、`Dockerfile`、`requirements.txt`、`docs/VERIFICATION.md`、
  领域测试）；框架层 `src/specargo/**` 不含领域字样（CI 纯净度守卫，仅 `envcompat.py` /
  `legacy_state_migration.py` 两个兼容文件例外）。
- **契约中性化**：环境变量 `PIPELINE_RUN_ID` / `PIPELINE_STATE_DB` / `PIPELINE_ARGO_*` /
  `PIPELINE_INPUT_DIR`（workflow 本地变量，不回退；旧 `TTS_RUN_ID` / `TTS_STATE_DB` / `TTS_ARGO_*` 由 `envcompat.py` 回退一版）；镜像 / kind 集群名
  `specargo:ci` / `specargo`；状态表 `pipeline_run` / `pipeline_item`，列
  `dataset` / `partition` / `variant`（旧库启动时原地迁移）。
- **插件化**：框架层不再 `import checks_tts`；领域检查由 spec 顶层 `plugins: [模块名]`
  经 `specargo/plugins.py` 显式加载（`examples → framework` 单向依赖）。

## 下一步（P1）

### 1. 双目标语义对等（已完成一部分）

- ✅ gate / state / reconcile 已编译为对 `pipeline_cli.py` 的调用，不再内嵌 Python 片段；
- ✅ 集群侧可选状态日志（`k8s.journal: true`）+ `onExit` 对账 + webhook；
  ✅ CI kind smoke 额外提交 journal=on 变体，**真实集群已通过**（onExit 1/1、对账 PASS、15 状态对象 + 分区清单）；
- ✅ 集群侧 task 级断点续跑（`k8s.resume`）：stage 启动先 `state check-skip`，终态则跳过；
  CI 4.6 真集群验证（删 workflow → 同名重提 → 所有 stage 打印 `[resume] 已完成，跳过`）；
- ⬜ 「不拉 Pod」形态的 skip（WorkflowTemplate + `when`）：当前跳过发生在 Pod 内部，
  省的是重算不是调度；
- ⬜ 真实部署的并发/规模验证（对象存储状态日志的对象数随 stage×partition 增长）。

### 1.2.1 可选改进：让损坏的状态记录可精确归因

当前状态对象的 key 是 `runs/<run>/_status/<stage>/<sha256(dataset,partition,variant)[:16]>.json`，
**key 不携带身份**，所以两条读路径（末 stage 直读 / 全量读）都无法把一条损坏记录还原到
具体 item 上，只能一致地判为"漏数"。若希望区分"跑了但状态不可信"（未过检）与"没跑过"（漏数），
需要在 key 里编入可解析的身份（如 `<dataset>/<partition>/<variant>.json`，注意路径转义与长度），
或额外维护一份 key→身份 的索引对象。

### 1.5 golden 编译产物 + RuntimeConfig（已完成）

- `tests/golden/` + `tests/test_golden.py`：5 份 golden（普通/journal/podGC-off/领域插件），
  编译器行为变更必须 `python3 tests/update_golden.py` 显式更新，否则 drift 失败；
  drift 检测已用"改一个默认值"实测有效。
- `src/specargo/runtime.py` `RuntimeConfig`：运行时配置从编译器里抽出，优先级
  内置默认 < spec `runtime:`（旧名 `k8s:` 兼容）< CLI `--runtime`；类型/未知字段/
  预留 env 在 load 期校验；`PIPELINE_RUN_ID` / `runs` 为固定契约（常量集中一处）。

### 2. 包化（已完成）

- `pyproject.toml` + `src/specargo/` 包结构 + `[project.scripts]`（`specargo` / `specargo-run` /
  `specargo-compile` / `specargo-gate` / `specargo-service`）；extras：`s3` / `dev`。
- 编译产物与 spec 命令统一为 `python3 -m specargo.<module>`，镜像改为 `pip install`
  （不再 COPY 源文件、不再依赖 `/app/*.py`）。
- `subprocess_env.child_env()` 让本地跑 demo 也不需要手动设 `PYTHONPATH`。
- 测试与 examples 通过 bootstrap 兜底：已安装用包，未安装退回 `src/`。

### 2.5 spec 契约版本与兼容策略（已完成）

spec 不只是配置，它是**给别人写的契约**：没有版本号时，"加一个字段"就等于"所有旧文件一起报错"，
而报错只会说"未知字段"，使用者只能猜。

- 顶层新增可选 `version`（缺省 = 当前版本，老 spec 不用回填、不告警）。
- `SPEC_VERSION_CURRENT` 只在这三种情况下升：语义变更 / 字段必填化 / 拒绝旧写法；
  新增可选字段与只加严格校验**不升**（属于 minor）。
- 兼容由代码强制：`_MIGRATIONS`（N -> N+1）在则自动迁移并告警（`SpecDeprecationWarning`
  继承 `UserWarning`，默认可见——不让旧写法静默通过）；缺迁移实现则明确报错、不猜。
- 版本校验在字段校验**之前**：高版本 spec 的新字段要先被迁移链搬到当前形态，
  否则用户看到的是"未知字段"而不是"该升级 specargo"。
- 常见误写（`spec_version` / `schema_version` / `apiVersion`）直接给正确字段名。
- 版本随产物走：编译出的 Workflow / WorkflowTemplate 带注解 `specargo.dev/spec-version`。
- 回归：`tests/test_spec_version.py`（10 例）——含"模拟框架升级到 v2/v3"的迁移链测试
  与"告警默认可见"的断言（不能出现"加了告警但用户看不到"的假象）。

遗留：v1 是第一版，`_MIGRATIONS` 目前为空；等真正引入 v2 时按同一模式补一条迁移即可。

### 2.6 扇出并发上限 + CLI 错误呈现（已完成）

两个都是"用户第一次跑就会撞上"的问题，不是理论缺陷。

**并发上限**（`runtime.parallelism`）：扇出管线（N 分区 × M 个 stage/gate）在提交瞬间
会一次性向 controller 投出 N×M 个 Pod 请求——真实瓶颈不是"跑得慢"，而是队列被打爆、
调度排队、kind 单节点直接拒绝。

- 可选、workflow 级（`spec.parallelism`），不写 = Argo 默认不限（不替用户做主）；
- 非法值（0 / 负数 / 字符串 / bool / 小数）与拼错键名全部报错，不允许"配了没生效"。

**CLI 错误呈现**：`SpecError` 是"你写的东西不对"，不是"跑挂了"，两者必须从退出码上分开：

- spec / 版本 / 文件不存在 → **退出码 2** + 一行可行动 stderr（不再吐 20 行 traceback）；
- 管线跑失败 / 对账不过 / 检查不过 → **退出码 1**（沿用原语义）；
- 顺带修掉统一入口的 P0：`main()` 不接受参数而 `cli.main` 传了参数，导致
  `specargo run x.yaml` 直接 `TypeError`；现在各 `main(argv=None)` 都接受 argv，
  `specargo gate --help` 也能透传到 gate 自己的解析器。

回归：`tests/test_cli_errors.py`（8 例，子进程级）+ `tests/test_parallelism.py`（7 例）。

### 3. `StateStore` 协议 + 集群 resume（已完成）

- `state.py` 拆出 `StateStore` 协议 + `SqliteStateStore` / `JournalStateStore` 两个实现
  （item 读写语义一致：四元组主键 + 同状态值；run 级记录在 journal 实现里退化为内存记录）。
- `resume_policy.py`：终态判定唯一实现，本地 runner 与集群编译产物共用
  （有闸门只认 verified，无闸门 generated 即终态）。
- 集群 resume：`k8s.resume: true` 时每个 stage 前插 `state check-skip`
  （退出码 0=已终态跳过 / 1=需执行 / 2=用法错误）；读状态失败按未完成处理。
- 跳过发生在 task 内部（Argo 仍拉 Pod）；要省 Pod 需 WorkflowTemplate + `when`（后续）。
- 集群 resume 已有**真集群 CI 冒烟**：第一轮跑通 → 删除 workflow（保留对象存储状态）
  → 同名重提 → 断言所有 stage 打印 `[resume] 已完成，跳过`。
- Postgres / Redis 等后端在有真实多写者需求前不做。

## 进行中 / 遗留

### 1. service 的 job 状态落 SQLite

当前 job 记录纯内存：服务重启后 `GET /tasks/{id}` 返回 404，但状态表里还有记录。
应把 job 记录也落 `state.py` 的表（或新建 `service_job` 表），重启后可查、可重试。

## 扩展验证

### 2.1 状态日志的规模/并发（已完成，2026-11-04）

`ci/scale_probe.py`（非 CI，手工跑）+ `tests/test_journal_scale.py`（CI 常驻，5 例）。
后者起一个**真实 HTTP 的 S3 兼容端点**（moto server + boto3，走完整网络栈），
断言规模与并发的四条不变量；前者在 200/500/1000 分区上量出成本曲线，已写进 README：

| 分区数 N | 状态对象数 | 写入（8 并发） | 末 stage 直读 | 全量读（旧路径） | 倍数 |
|---|---|---|---|---|---|
| 200 | 600 | 2.58s | 1.15s | 3.52s | 3.1× |
| 500 | 1500 | 6.74s | 2.95s | 8.94s | 3.0× |
| 1000 | 3000 | 13.86s | 6.02s | 18.19s | 3.0× |

结论：**对象数 = stage×分区 + 1 个分区清单**（规模上限由这个公式决定，key 不重复，
所以并发写不丢不覆，8 线程实测无丢失）；直读确实把往返降到 1/3；对账判定是纯内存
计算，与 N 无关。`unit-tests` CI 从此安装 boto3 + moto[server]——S3 路径此前只在
k8s-smoke 的集群里被覆盖，离线套件里一次都没跑过。

**仍缺（需要真集群，本机没有 docker/kind/argo）**：
- `partitions: {from: range, start: 1, end: 200}` 真跑一轮，观测 controller 队列深度、
  API server QPS、Pod 调度延迟与 `runtime.parallelism` 的实际限流效果
  （注意先设 parallelism，否则测到的是 pod storm 而不是吞吐）；
- 对象存储换真 MinIO/OSS/S3 后的 RTT（上表是 localhost 量级，按每次 GET 的实际
  RTT 线性放大属推断，未实测）。

### 3. 失败注入测试

故意让某分区的质量闸门（`gate-<stage>`）失败，验证：
- task 失败 → 下游被依赖语义跳过 → 对账报 missing → 退出码非 0
- `onExit` 仍收口
- 告警通道收到通知

### 4. WorkflowTemplate + 版本化（已完成）

- `specargo-compile --as-template` → `WorkflowTemplate`（spec 部分与 Workflow 完全一致，
  只是 CRD 类型不同）；`--reference NAME` → 引用该模板的最小 Workflow。
- CI 真集群验证：`kubectl apply` 模板 → 提交引用 → 断言 Succeeded 且节点由模板展开。
- 灰度/回滚做法：模板名不变、内容更新（`kubectl apply` 新版本），新提交的 Workflow
  自动用新版本；回滚即重新 apply 旧版本。已在 README「工作流版本化」写明。

## 规模与性能（已完成）

- 千级分区实测（`tests/test_scale.py`）：1000 分区 × 3 stage = 3000 个状态对象，
  写 0.7s、末 stage 直读 0.18s、对账 0ms、**GET 次数 = 1000（不是 3000）**、LIST 0 次。
- 直读路径与"全量读 + 按 final_stage 过滤"结论逐条一致（含缺失/损坏/非终态），
  漏数与未过检在报告里分开。
- 未验证：真实 S3/MinIO 上的网络往返量级、万级分区、controller 限流。

## 已知未覆盖

- ReadWriteMany PVC（当前用对象存储绕开，未验证真 RWX）
- 多副本高可用（object-store / workflow-controller 都是单副本）
- 生产级对象存储（MinIO / OSS / S3）的真实吞吐与成本
- gRPC 服务形态（当前只有 HTTP）

