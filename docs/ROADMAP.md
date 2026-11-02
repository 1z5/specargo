# ROADMAP

按价值/风险排序的后续工作。已完成项见 `README.md` 的「已验证」章节。

## 已完成

### ✅ spec2argo 编译器（同一份 spec，两个执行目标）

`app/spec2argo.py` 把 PipelineSpec 编译成 Argo CRD：

| PipelineSpec | Argo CRD |
|---|---|
| stages + dependencies | dag.tasks + dependencies |
| partitions: discover | discover 任务 + withParam 引用其 JSON 数组输出 |
| RetryPolicy | retryStrategy（总是输出；limit=0 是显式设计意图） |
| Stage.timeout | template timeout |
| checks | 独立 gate-<stage> 任务（跑 checks.py 插件，limit=0） |
| artifacts / consumes | 自动接线对象存储路径契约（上游 put、下游 get，key 一致） |

验收已达成：CI 编译 pipelines/etl-demo.yaml -> argo lint 通过 -> 提交编译产物
跑集群冒烟（31/31 Pod、5 分区、Succeeded）。本地 runner 与集群执行自此共享
同一份 spec；k8s/workflow-etl-smoke.yaml 降级为对照参考。

已知边界：编译器覆盖 container 命令 + checks + artifacts + 参数/扇出/检查配置映射；
script 型 template、synchronization、suspend、onExit 等尚未映射（见「P0 修复进度」）。

## 已决定、未执行（本轮及下一轮）

### 1. 项目改名：`tts-pipeline-argo` → `specargo`

决策已定，**尚未执行**。本轮（PR-A/PR-B）只修正确性 + 文档事实，不改名；
改名与构建/部署身份中性化（README 标题与 badge、`gh workflow run -R`、本地 remote、
镜像 `tts-argo:ci`、kind 集群名 `tts-argo`）在下一轮统一执行。
回滚方式：反向命名 + revert 对应提交。

### 2. TTS 领域代码归档到 `examples/tts-asr/`

决策已定，**尚未执行**：`pipeline.py` / `verify_audio.py` / `tts_client.py` /
`edge_tts_client.py` / `tts_verify/` / `checks_tts.py` / `book_reader.py` /
`text_composer.py` / `models.py` 等领域代码移入 `examples/tts-asr/`；
`k8s/workflow.yaml` / `cronworkflow.yaml` 一并作为示例归档。框架层不再保留 TTS 命名。
在此之前，TTS 示例入口（`pipelines/tts.yaml` 引用的 `main.py`）是失效的，
不要把它当作可运行示例（详见 README 的「TTS 领域示例」）。

### 3. P0 修复进度（本轮已完成代码部分）

| # | 问题 | 修复 |
|---|---|---|
| 1 | spec 无结构校验：空/拼错 YAML 静默全绿、依赖拼错下游照跑、`when` 半个特性 | `spec.py` 新增 `validate_spec()`：必填/命名/未知字段/依赖存在性与成环/artifact 闭合/时长与重试枚举/`when` 显式拒绝；discover 失败与非法类型都转为 `SpecError` |
| 2 | 编译器只对 `etl-demo` 形状成立：params 不编 arguments、variant/dataset 扇出恒空、check_config 不解析 | `spec2argo.py` 编译全部 params 为 workflow arguments；`{{params.x}}`→`{{workflow.parameters.x}}`；扇出改为 `{key,dataset,variant}` 对象；discover 输出对象数组；gate 配置编译期解析；编译产物做"零残留模板变量"自检 |
| 3 | 集群 gate 读节点本地路径、不 objstore get（靠同节点调度巧合通过） | gate 任务先按同一路径契约 `objstore get` 再跑 checks；脚本加 `set -e` |
| 4 | `service --state-db` 即不可用：readyz 恒 503、状态写库异常被吞 | 连接 `check_same_thread=False` + 单锁串行；readyz 带锁探测；写库失败记录并打日志 |
| 5 | 无 checks 的 stage 永不 resume、对账必失败（隐性强制每 stage 配闸门） | 终态语义：有 checks 只认 `verified`，无 checks 的 `generated` 即成功终态；resume 与对账共用同一判据 |
| 6 | `runner` 要求 `--workdir` 预先存在（新手第一跑就失败） | Runner 自动创建 workdir |
| 7 | 非拓扑序声明 stages 会错序执行且假绿；依赖阻断不级联（a 失败→b 跳过→c 照跑） | Runner 按依赖拓扑排序执行；跳过态（blocked）也级联阻断 |
| 8 | 类型错误泄漏原生异常（`retry: 3`→AttributeError、字符串列表被拆字符、`retry.limit: 2.5` 被静默截断等）、consumes 与 dependencies 不交叉校验 | `load_spec` 严格类型校验（列表/映射/字符串/重试结构/数值必须整数）+ 参数名与分区 key 字符集校验 + consumes 的生产者必须是上游依赖；discover 加载期加超时；S3 `list` 保留尾斜杠前缀语义（防 `runs/r1` over-match `runs/r10`） |
| 9 | dataset-build 的 gate 深度校验依赖 shards 目录（不是 artifact、跨 Pod 取不到）→ 集群必红 | 深度自校验移入 `shard` stage 命令；gate 只做可跨 Pod 取回的 manifest 结构校验 |
| 10 | 自动生成的 objstore `--file` 未加引号（路径含空格失败）；gate 未声明 dataset/variant 输入 | 路径统一 `shlex.quote`；gate inputs/args 补齐三个维度 |
| 11 | 文档与实际不符（死命令/数字/夸大） | README/ARCHITECTURE/ROADMAP 已按实测证据校正 |

新增回归：`tests/test_p0_regressions.py`（23 例）；离线套件合计 79 例，全部通过。
验证方式：本地四套件 + ETL/dataset 管线 E2E + 编译产物零残留 + 独立 Argo 模拟器
（每个 task-item 独立目录、只共享对象存储）跑通 etl-demo 19/19 与 dataset-build 16/16；
真实集群侧由 CI 的 kind smoke 继续验证。

**已知边界（本轮确认，未在 P0 内解决）**：
- 集群 gate 只支持**单文件** artifact 的跨 Pod 校验；目录/多产物 artifact（如 shards 目录）不支持。
- 分区 key/dataset/variant 限制为 `^(?!\.+$)[\w.-]+$`（Unicode 字母/数字/下划线/点/连字符；额外排除纯 `.`/`..` 路径段）；discover 输出在加载期按整行解析、运行期也做同一校验。含空格/引号/`$` 的值会破坏路径与 shell，拒绝。
- artifact key 的空段/`.`/`..` 校验已下沉到 `artifacts.py`，本地目录与 S3 实现共用同一套规则（此前只有本地实现校验）。
- 参数值按**文本替换**注入命令：值里的 shell 元字符（如 `$`）会被目标 shell 二次解释，含此类字符需在 spec 内自行引号/转义。
- gate 的检查配置/产物路径会被内嵌进 Python 源码（Argo 文本替换后执行）：路径参数值含**反斜杠/换行/单引号**时 gate 会失败。当前用字符集+文档约束规避；彻底做法是 gate 改成从 env/argv 读取路径（后续）。
- local runner 按依赖拓扑排序执行，不要求 YAML 里 stages 的书写顺序是拓扑序；编译到 Argo 由 DAG dependencies 保证。

**仍未做**：脚本已改但未在真实集群重跑（等 CI）；集群状态落库/对账/onExit 仍未映射；
TTS 示例归档与改名在下一轮。

## 进行中 / 下一步

### 1. runner 回传真实 attempt 次数（已完成）

_execute() 现返回 (ok, out, attempts)，状态表 attempt 列是真实重试次数
（有测试锁定：前两次失败第三次成功 -> attempts == 3）。

### 2. service 的 job 状态落 SQLite

当前 job 记录纯内存：服务重启后 `GET /tasks/{id}` 返回 404，但状态表里还有记录。
应把 job 记录也落 `state.py` 的表（或新建 `service_job` 表），重启后可查、可重试。

## 扩展验证

### 3. 规模测试：千级分区

`partitions: {from: range, start: 1, end: 200}` 跑一轮，观测：
- workflow-controller 的队列深度与 API server QPS
- Pod 调度延迟、`parallelism` 上限
- 对象存储吞吐（当前 moto 是进程内实现，需换真 MinIO 才能测）

产出数据写进 README，补上"千级 Pod 未实测"这一条。

### 4. 失败注入测试

故意让某分区的质量闸门（`gate-<stage>`）失败，验证：
- task 失败 → 下游被依赖语义跳过 → 对账报 missing → 退出码非 0
- `onExit` 仍收口
- 告警通道收到通知

### 5. WorkflowTemplate + 版本化

`k8s/workflowtemplate.yaml` + `workflowTemplateRef`，演示 DAG 改版如何灰度与回滚
（对应"工作流版本管理与发布"）。

## 已知未覆盖

- ReadWriteMany PVC（当前用对象存储绕开，未验证真 RWX）
- 多副本高可用（object-store / workflow-controller 都是单副本）
- 生产级对象存储（MinIO/OSS/S3）的真实吞吐与成本
- gRPC 服务形态（当前只有 HTTP）
