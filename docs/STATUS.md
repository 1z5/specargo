# specargo 项目状态

> 单一事实来源：**哪些能力已经实测通过、哪些只是设计上成立、哪些没有覆盖**。
> 维护规则只有一条：写进"已验证"的每一条都必须能在仓库里指出证据（测试文件 / CI run / 命令输出）；
> 只能由设计或代码推断、但没有实测证据的，一律写进"未验证"。
> 计划与取舍见 [`ROADMAP.md`](ROADMAP.md)，架构与模块边界见 [`ARCHITECTURE.md`](ARCHITECTURE.md)。

**快照**：基线提交 `31da39b`（`main`，本地 == `origin/main`，状态日志规模/并发真实 HTTP 实测），
文档状态更新于 2026-11-04。该提交的 CI：`unit-tests` 41856027612（success，含新增的
`test_journal_scale.py`）；`k8s-smoke` **未触发**——它只改了 tests/ docs/ ci/ 与
`unit-tests.yml`，不在 k8s-smoke 的 `paths:` 过滤范围内；最近一次 k8s-smoke 为
`e8ce15c` 的 41840935534（success）。

---

## 1. 一句话结论

框架层（spec 解析 → 本地 runner / Argo 编译器 → 质量闸门 → 状态登记 → 对账）功能闭环，
**172 个离线用例 + 本地 E2E + kind 真实集群 E2E 三侧都有实测证据**；
剩下的缺口集中在"生产级环境的非功能性验证"（真对象存储吞吐、千级 Pod、多副本 HA）
和两类未映射的 Argo 特性（script 型 template、synchronization/suspend，外加「不拉 Pod」
形态的集群 skip——当前 `k8s.resume` 已是 task 级跳过并真集群验证过）。

## 2. 已验证

### 2.1 离线套件：172 例全绿（框架 168 + 领域示例 4）

| 套件 | 例数 | 覆盖要点 |
|---|---|---|
| `tests/test_framework.py` | 32 | spec 解析与严格校验、四类通用检查、runner 断点跳过 / 依赖阻断 / 重试 / attempt 计数、对账按 variant 分组抓假绿、编译产物结构 / 确定性 / gate / discover、框架不反向 import 领域插件 |
| `tests/test_p0_regressions.py` | 27 | 审计中实测到的静默失败 / 假绿全部固化为断言（spec 写错必须报错、无闸门 stage 的 resume 与对账、编译产物参数 / 扇出 / 检查配置、集群 gate 取数走对象存储） |
| `tests/test_cluster_semantics.py` | 16 | gate CLI（内置 / 领域插件）、SQLite / 对象存储状态登记、journal 对账 CLI（PASS/FAIL 与退出码）、编译产物 onExit / 状态写入结构、按 Pod 语义端到端模拟（缺数时 onExit 失败） |
| `tests/test_service_and_storage.py` | 14 | 真实起服务进程验证探针、`--state-db` 时 readyz 与落库、异步任务全生命周期、暂停期拒新任务、SIGTERM 优雅退出（退出码 0）、objstore 本地往返 / 目录穿越拒绝 / S3 前缀语义 |
| `tests/test_runtime.py` | 10 | `RuntimeConfig` 三级优先级与严格校验（内置默认 < spec `runtime:` < CLI `--runtime`） |
| `tests/test_resume_state.py` | 8 | 指纹跳过与输入变化、输出缺失重生成、item 状态幂等 upsert、对账漏数 / 未过检、旧库 schema 自动迁移 |
| `tests/test_state_store.py` | 9 | `StateStore` 协议下两个实现的 item / run 读写语义一致 |
| `tests/test_scale.py` | 7 | 千级分区规模与耗时上界、直读 == 全量读（含损坏状态记录的分类一致性）、GET 次数硬断言、check-skip O(1) |
| `tests/test_journal_scale.py` | 5 | 状态日志规模/并发（真实 S3 HTTP）：对象数 = stage×分区 + 1 分区清单、末 stage 直读 GET == N 且 LIST 0 次、全量读 = LIST 1 + 3N GET、8 线程并发写不丢不覆、缺末 stage 状态算"漏数" |
| `tests/test_cli_errors.py` | 11 | CLI 契约：spec 写错 → 退出码 2 + 一行提示（无 traceback）、管线跑挂 → 退出码 1、统一入口 `specargo <cmd>` 可用、子命令 `--help` 透传、未知命令拒绝、**YAML 语法错误 → 一行提示 + 退出码 2（三个入口均无 traceback）** |
| `tests/test_docs_numbers.py` | 3 | 文档数字守卫：每个套件的例数与 README / STATUS 表格一致，总例数与 README 4 处 + ROADMAP 1 处 + STATUS 2 处一致——漏改文档即本套件失败 |
| `tests/test_parallelism.py` | 7 | 扇出并发上限：默认不写、`runtime.parallelism` 落到 `spec.parallelism`、旧名 `k8s:` 同样生效、`--runtime` 覆盖、模板携带、非法值与拼错键名拒绝 |
| `tests/test_spec_version.py` | 10 | spec 契约版本：缺省不告警、高版本报"升级 specargo"、误写字段名给正确提示、迁移链按序执行且告警默认可见、缺迁移实现明确报错、编译产物带 `specargo.dev/spec-version` 注解 |
| `tests/test_golden.py` | 4 | golden 编译产物 drift 检测（5 份 golden：普通 / journal / podGC-off / 领域插件 / etl） |
| `tests/test_template.py` | 5 | `--as-template` / `--reference` 编译形态与引用最小 Workflow |
| `examples/tts-asr/tests/test_tts_state.py` | 4 | 书籍口径 ↔ 通用状态表映射、领域对账、重复产物检测（不装 openai / faster-whisper） |

表格与总数由 `tests/test_docs_numbers.py` 强制校验：加一套用例忘了更新文档，本套件直接失败。

复现（零外部依赖，约 1 分钟）：

```bash
for f in tests/test_*.py examples/tts-asr/tests/test_*.py; do python3 "$f"; done
```

### 2.2 编译器与门禁（本地实测）

- `python3 tests/update_golden.py --diff` → **0 个 golden 变更**（编译器行为无 drift）。
- 三条管线 `specargo-compile` 成功，且产物**零残留模板变量**（`{{params.` / `{{partition.` / `{{artifact}}` 均无）。
- 框架纯净度守卫：`src/` 内无领域词泄漏（CI 同一条 grep，仅 `envcompat.py` / `legacy_state_migration.py` 例外）。
- spec 契约版本：加载期强制（缺省=当前版本、高版本拒绝、旧版本走迁移链并可见告警），
  编译产物带 `specargo.dev/spec-version` 注解——版本信息活到线上，而不只停在文档里。

### 2.3 真集群规模冒烟（本地 kind，2026-11-04 实测）

环境：kind 3 节点（colima VM 6 核 / 8 GB）+ Argo Workflows v3.7.18 + 单副本 moto
对象存储 + 框架镜像 `specargo:ci`。变体：`etl-demo` + 200 分区 + `parallelism: 20`
+ `journal: true` + `resume: true`（`pod_gc_strategy: ''` 以便断言）。

| 指标 | 实测 |
|---|---|
| 最终状态 / onExit 对账 | Succeeded / **PASS**（expected=200、verified=200、missing=0） |
| 墙钟 | 1466s（1202 个 Pod，平均 0.8 Pod/s） |
| 峰值并发 Pod | **20 == parallelism 设定**（时间戳口径与实时采样双口径印证） |
| 状态日志对象数 | 600 = 3 stage × 200（+1 分区清单；run 前缀共 1201） |
| 集群内对象存储 GET RTT | p50 1.7ms / p95 4.2ms / max 11.1ms（moto 单副本，200 次） |
| 保留的 Pod 对象 | 1233（`pod_gc_strategy: ''` 的规模代价，别当默认） |

复现：`ci/local_scale_smoke.sh up && ci/local_scale_smoke.sh scale 200 20`。
口径纪律：并发必须数**非终态** Pod；按 namespace 全部 Pod 数会把保留的已完成 Pod
算成并发（200 分区时量出 1236 这种累计值），结论会写反。

### 2.4 状态日志的规模与并发（真实 S3 HTTP，2026-11-04 实测）

`ci/scale_probe.py` + `tests/test_journal_scale.py`（CI 常驻）。端点是本机 moto server
（真实 TCP/HTTP + boto3 序列化，与生产的差别只是 RTT 量级），8 线程并发写各自 key：

| 分区数 N | 状态对象数 | 写入(8 并发) | 末 stage 直读 | 全量读(旧路径) | 倍数 |
|---|---|---|---|---|---|
| 200 | 600 | 2.58s | 1.15s | 3.52s | 3.1× |
| 500 | 1500 | 6.74s | 2.95s | 8.94s | 3.0× |
| 1000 | 3000 | 13.86s | 6.02s | 18.19s | 3.0× |

- **对象数 = stage × 分区 + 1 个分区清单**（规模上限公式；key 独立 → 并发安全，实测无丢失）
- 末 stage 直读：GET == N、LIST == 0；全量读：LIST 1 + 3N GET（3.0× 差距的硬证据）
- 对账判定（`evaluate_items`）是纯内存计算，与 N 无关
- 口径纪律：本地文件系统（1000 分区写 0.7s / 读 0.18s）与真实 HTTP 是两套数字，
  README 两处都标了口径，不混读

### 2.5 本地 E2E（从 `pip install -e .` 后的新路径实测）

```
dataset 链路：seed 25 条 → clean 21 合格 / 4 隔离（去重 2）→ 2 片 + manifest 自校验通过 → manifest verify PASS
etl-demo  ：specargo-run 首跑 汇总 verified=9，对账 ok=True
             第二次跑 汇总 skipped=9（断点续跑全部命中）
```

### 2.6 真实集群（kind 1 控制面 + 2 worker + Argo Workflows v3.7.18，CI 每次 push 自动验）

最近一次**改了集群侧代码**（src/ pipelines/ k8s/）的提交是 `e8ce15c`：`k8s-smoke`
41840935534 success——真集群零退化（31/31 Pod、onExit 1/1、对账 PASS、15 状态对象、
模板展开 31 节点）。其后的提交只动 tests/ docs/ ci/（`31da39b` 等），按 `paths:` 过滤
不触发 k8s-smoke，`unit-tests` 41856027612 success。

主工作流（`spec2argo` 编译产物直接提交）：

```
工作流最终状态: Succeeded
Pod 节点成功数: 31/31        # 1 discover + 6 × 5 分区（3 stage + 3 质量闸门）
Pod 分布到的节点数: 2        # 实际 17 + 14
对象存储中的 artifact 对象数: 15
```

journal 变体（`k8s.journal: true`，同一 job 紧接着再提交一次）：

```
journal 工作流最终状态: Succeeded
onExit 节点成功数: 1/1
批次 etl-demo-journal 对账结果: PASS
状态日志对象数: 15    # 3 stage × 5 分区
分区清单对象数: 1
```

集群 resume（`k8s.resume: true`）：第一轮跑通 → 删除 workflow（保留对象存储状态）→ 同名重提 →
所有 stage 打印 `[resume] 已完成，跳过`（CI 断言）。
WorkflowTemplate 版本化：`--as-template` 产出模板 → `--reference` 引用 → 真集群 Succeeded 且节点由模板展开。

### 2.7 规模与性能（本地实测，`tests/test_scale.py`）

1000 分区 × 3 stage = 3000 个状态对象：

| 指标 | 值 |
|---|---|
| 末 stage 状态直读 | 1000 次 GET / 0.18s / **LIST 0 次**（对账只需要末 stage 的 N 个状态） |
| 写入 3000 个状态对象 | 0.7s |
| 对账耗时 | 0ms |
| `check-skip` 单点读 | O(1)，≤2 次 GET、不 LIST |

漏数（没有状态对象）与未过检（跑了但没过检）在报告里分开，两类故障不会混为一类。

## 3. 已知边界（设计上如此，不是待修 bug）

- 集群 gate 只支持**单文件** artifact 的跨 Pod 校验；目录 / 多产物 artifact（如 shards 目录）不支持
  —— 所以 dataset-build 的深度自校验放在 `shard` stage 的命令里，gate 只做 manifest 结构校验。
- 分区 key / dataset / variant 限定 `^(?!\.+$)[\w.-]+$`；含空格 / 引号 / `$` 的值在加载期被拒绝
  （会破坏路径与 shell）。参数值按文本替换注入命令，含 shell 元字符需在 spec 内自行引号 / 转义。
- local runner 按依赖拓扑排序执行，YAML 里 stages 的书写顺序不必是拓扑序；Argo 侧由 DAG dependencies 保证。
- 旧状态库迁移边角：旧表与新表并存时只告警、不自动合并；`schema_version` 只升不降。
- 集群插件加载要求领域镜像内含插件模块，并由 `k8s.plugin_path` 指明路径；通用镜像不带领域插件。
- `onExit` 对账默认关闭，需 `k8s.journal: true` 显式开启。
- 跳过发生在 task 内部（Argo 仍会拉 Pod）；要省 Pod 需 WorkflowTemplate + `when`。

## 3.5 交付就绪度（"能交出去"与"能上生产"是两件事）

| 维度 | 现状 | 结论 |
|---|---|---|
| **功能闭环** | spec 解析 → 本地执行 / 编译上集群 → 质量闸门 → 状态登记 → 对账 → 常驻服务 | ✅ 可交付 |
| **安装可复现** | 全新克隆 + `pip install -e .` + 快速开始五步，已在干净 venv 逐步验证 | ✅ 可交付 |
| **部署文档** | [`DEPLOYMENT.md`](DEPLOYMENT.md)：Argo 安装、artifact 仓库、镜像、RBAC、配额、升级回滚、验收清单 | ✅ 可交付 |
| **运维手册** | [`RUNBOOK.md`](RUNBOOK.md)：Pending / Failed / 卡住 / 漏数 / resume 不生效 / 对象存储故障 / 状态损坏 | ✅ 可交付 |
| **权限与配额** | `k8s/rbac.yaml`（SA + 最小 Role）、`k8s/quota.yaml`（ResourceQuota + LimitRange），kubectl client 校验通过 | ✅ 可交付（示例值需按环境调） |
| **版本与变更** | `CHANGELOG.md` + git tag `v0.1.0` | ✅ 可交付 |
| **生产环境验证** | 真实对象存储吞吐、千级 Pod、多副本 HA、RWX PVC | ❌ 未验证（见 §4） |
| **安全加固** | 镜像非 root、Secret 外置（Vault/KMS）、网络策略、多租户审计 | ❌ 未做 |
| **发布与灰度** | 无 Helm chart、无金丝雀/流量切换（批处理侧影响小，模型版本灰度未做） | ❌ 未做 |

**一句话**：这套东西可以**交给另一个工程师在他自己的集群上跑起来并运维**（有部署手册、
运维手册、权限配额示例、版本与变更记录、172 例离线测试 + 真集群 CI）；
但**不等于已经在生产上跑过**——§4 的环境相关验证和上面的安全加固是它成为
"生产系统"之前必须补的，且只能在真实环境补。

## 4. 未验证（需要真实环境，代码无法自证）

- **生产级对象存储**：当前集群冒烟用进程内 moto 自建 S3 端点，MinIO / OSS / S3 的真实吞吐、
  网络往返量级与成本未测；2.5 的规模数字是本地文件系统上的结论。
- **千级 Pod / 万级分区（编排侧）**：`parallelism` 的限流效果已在本地 kind 3 节点 +
  200 分区上实测（峰值并发 == 设定值 20，见 §2.3）；未测的是千级分区规模、controller
  限流/积压曲线、API server QPS，以及多副本 controller/对象存储的 HA 形态。
- **对象存储侧已测、但仅限本机端点**：状态日志的规模/并发/成本曲线已在真实 S3 HTTP 上实测
  （见 §2.4），但端点是本机 moto server，RTT 是 localhost 量级；换真 MinIO/OSS/S3 后的
  往返量级只能按每次 GET 的 RTT 线性放大推断，**未实测**。
- **多副本高可用**：object-store / workflow-controller 均单副本；ReadWriteMany PVC 未验证（当前用对象存储绕开）。
- **gRPC 服务形态**：当前只有 HTTP。
- **失败注入**：故意让某分区 gate 失败，验证"下游依赖跳过 → 对账报 missing → 退出码非 0 →
  onExit 仍收口 → 告警通道收到通知"这条链路尚未作为固定测试固化。

## 5. 未映射（功能缺口）

| 缺口 | 说明 |
|---|---|
| 「不拉 Pod」形态的集群 skip | **task 级 skip 已实现并有真集群冒烟**（`k8s.resume` + `state check-skip`，Pod 仍会被调度）；要省 Pod 需 WorkflowTemplate + `when` |
| script 型 template | spec 的 script 形态尚未映射到 Argo |
| synchronization / suspend | 未映射 |
| service job 记录落库 | 当前 job 记录纯内存：重启后 `GET /tasks/{id}` 404，但状态表仍有记录（ROADMAP「进行中/遗留」） |

## 6. 下一步候选（按价值 / 风险排序）

1. **失败注入测试**（可离线 + 现有模拟集群完成，成本最低、收益直接）：把第 4 节最后一条链路固化。
2. **真集群上跑一轮 200 分区 + 显式 `parallelism`**（需要集群配额）：对象存储侧的成本曲线
   已在真实 HTTP 上量到（§2.4），缺的是编排侧——controller 队列深度、API QPS、Pod 调度
   延迟与 `parallelism` 的实际限流效果；注意先设并发上限，否则测到的是 pod storm 而不是吞吐。
3. **service job 记录落库**（本地可验）：补 `service_job` 表或复用 `state.py`，让重启后可查可重试。
4. **WorkflowTemplate + `when` 省 Pod 的 resume**（需要真集群验证）：把"task 内跳过"升级为"不拉 Pod"。

> ✅ 原第 5 条"文档数字自检"已落地：`tests/test_docs_numbers.py` 强制 8 处文档数字与真实用例数一致，
> 漏改即红；它上线当天就抓到 README 两处过期例数与两个从未在 README 出现的套件。

## 7. 复现与 CI 触发规则

- 离线全绿：`for f in tests/test_*.py examples/tts-asr/tests/test_*.py; do python3 "$f"; done`
- 编译器 drift：`python3 tests/update_golden.py --diff`
- 纯净度守卫：CI `unit-tests` 的第 4 步（`grep -rniE ... src/`），本地可手工重跑同一条命令。
- 真实集群冒烟：`gh workflow run k8s-smoke -R 1z5/specargo`（或 push 到 `main` 自动触发）。
- **CI 触发规则**：两个 workflow 的 `paths:` 只含 `src/ examples/ pipelines/ tests/ k8s/ Dockerfile` 与
  workflow 自身；**纯 `docs/` 提交不会触发 CI**（这是预期，不是漏跑）。
