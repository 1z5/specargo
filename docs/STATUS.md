# specargo 项目状态

> 单一事实来源：**哪些能力已经实测通过、哪些只是设计上成立、哪些没有覆盖**。
> 维护规则只有一条：写进"已验证"的每一条都必须能在仓库里指出证据（测试文件 / CI run / 命令输出）；
> 只能由设计或代码推断、但没有实测证据的，一律写进"未验证"。
> 计划与取舍见 [`ROADMAP.md`](ROADMAP.md)，架构与模块边界见 [`ARCHITECTURE.md`](ARCHITECTURE.md)。

**快照**：基线提交 `cdc5a66`（`main`，本地 == `origin/main`），文档状态更新于 2026-11-03。
最后一条**代码**提交是 `5278c0c`（其后两个提交只改 `docs/`，不触发 CI，见文末"CI 触发规则"）。

---

## 1. 一句话结论

框架层（spec 解析 → 本地 runner / Argo 编译器 → 质量闸门 → 状态登记 → 对账）功能闭环，
**135 个离线用例 + 本地 E2E + kind 真实集群 E2E 三侧都有实测证据**；
剩下的缺口集中在"生产级环境的非功能性验证"（真对象存储吞吐、千级 Pod、多副本 HA）
和三个未映射的 Argo 特性（集群 skip、script template、synchronization/suspend）。

## 2. 已验证

### 2.1 离线套件：135 例全绿（框架 131 + 领域示例 4）

| 套件 | 例数 | 覆盖要点 |
|---|---|---|
| `tests/test_framework.py` | 32 | spec 解析与严格校验、四类通用检查、runner 断点跳过 / 依赖阻断 / 重试 / attempt 计数、对账按 variant 分组抓假绿、编译产物结构 / 确定性 / gate / discover、框架不反向 import 领域插件 |
| `tests/test_p0_regressions.py` | 27 | 审计中实测到的静默失败 / 假绿全部固化为断言（spec 写错必须报错、无闸门 stage 的 resume 与对账、编译产物参数 / 扇出 / 检查配置、集群 gate 取数走对象存储） |
| `tests/test_cluster_semantics.py` | 16 | gate CLI（内置 / 领域插件）、SQLite / 对象存储状态登记、journal 对账 CLI（PASS/FAIL 与退出码）、编译产物 onExit / 状态写入结构、按 Pod 语义端到端模拟（缺数时 onExit 失败） |
| `tests/test_service_and_storage.py` | 14 | 真实起服务进程验证探针、`--state-db` 时 readyz 与落库、异步任务全生命周期、暂停期拒新任务、SIGTERM 优雅退出（退出码 0）、objstore 本地往返 / 目录穿越拒绝 / S3 前缀语义 |
| `tests/test_runtime.py` | 10 | `RuntimeConfig` 三级优先级与严格校验（内置默认 < spec `runtime:` < CLI `--runtime`） |
| `tests/test_resume_state.py` | 8 | 指纹跳过与输入变化、输出缺失重生成、item 状态幂等 upsert、对账漏数 / 未过检、旧库 schema 自动迁移 |
| `tests/test_state_store.py` | 9 | `StateStore` 协议下两个实现的 item / run 读写语义一致 |
| `tests/test_scale.py` | 6 | 千级分区规模与耗时上界、直读 == 全量读、GET 次数硬断言、check-skip O(1) |
| `tests/test_golden.py` | 4 | golden 编译产物 drift 检测（5 份 golden：普通 / journal / podGC-off / 领域插件 / etl） |
| `tests/test_template.py` | 5 | `--as-template` / `--reference` 编译形态与引用最小 Workflow |
| `examples/tts-asr/tests/test_tts_state.py` | 4 | 书籍口径 ↔ 通用状态表映射、领域对账、重复产物检测（不装 openai / faster-whisper） |

复现（零外部依赖，约 1 分钟）：

```bash
for f in tests/test_*.py examples/tts-asr/tests/test_*.py; do python3 "$f"; done
```

### 2.2 编译器与门禁（本地实测）

- `python3 tests/update_golden.py --diff` → **0 个 golden 变更**（编译器行为无 drift）。
- 三条管线 `specargo-compile` 成功，且产物**零残留模板变量**（`{{params.` / `{{partition.` / `{{artifact}}` 均无）。
- 框架纯净度守卫：`src/` 内无领域词泄漏（CI 同一条 grep，仅 `envcompat.py` / `legacy_state_migration.py` 例外）。

### 2.3 本地 E2E（从 `pip install -e .` 后的新路径实测）

```
dataset 链路：seed 25 条 → clean 21 合格 / 4 隔离（去重 2）→ 2 片 + manifest 自校验通过 → manifest verify PASS
etl-demo  ：specargo-run 首跑 汇总 verified=9，对账 ok=True
             第二次跑 汇总 skipped=9（断点续跑全部命中）
```

### 2.4 真实集群（kind 1 控制面 + 2 worker + Argo Workflows v3.7.18，CI 每次 push 自动验）

最近一次代码提交 `5278c0c` 的两个 job 均 success：`unit-tests` 41610427203（38s）、
`k8s-smoke` 41610427080（9m44s）。

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

### 2.5 规模与性能（本地实测，`tests/test_scale.py`）

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

## 4. 未验证（需要真实环境，代码无法自证）

- **生产级对象存储**：当前集群冒烟用进程内 moto 自建 S3 端点，MinIO / OSS / S3 的真实吞吐、
  网络往返量级与成本未测；2.5 的规模数字是本地文件系统上的结论。
- **千级 Pod / 万级分区**：controller 限流、API server QPS、调度延迟、`parallelism` 上限未观测
  （`partitions: {from: range, end: 200}` 一轮即可开始，见 ROADMAP「扩展验证」）。
- **多副本高可用**：object-store / workflow-controller 均单副本；ReadWriteMany PVC 未验证（当前用对象存储绕开）。
- **gRPC 服务形态**：当前只有 HTTP。
- **失败注入**：故意让某分区 gate 失败，验证"下游依赖跳过 → 对账报 missing → 退出码非 0 →
  onExit 仍收口 → 告警通道收到通知"这条链路尚未作为固定测试固化。

## 5. 未映射（功能缺口）

| 缺口 | 说明 |
|---|---|
| 集群侧断点续跑（skip） | 已有 `state check-skip` + task 内跳过 + 真集群冒烟；"不拉 Pod"的版本需要 WorkflowTemplate + `when` |
| script 型 template | spec 的 script 形态尚未映射到 Argo |
| synchronization / suspend | 未映射 |
| service job 记录落库 | 当前 job 记录纯内存：重启后 `GET /tasks/{id}` 404，但状态表仍有记录（ROADMAP「进行中/遗留」） |

## 6. 下一步候选（按价值 / 风险排序）

1. **失败注入测试**（可离线 + 现有模拟集群完成，成本最低、收益直接）：把第 4 节最后一条链路固化。
2. **service job 记录落库**（本地可验）：补 `service_job` 表或复用 `state.py`，让重启后可查可重试。
3. **扩展验证一轮 200 分区**（需要集群配额，产出数据回填 README）：补"千级 Pod 未实测"这一条。
4. **WorkflowTemplate + `when` 省 Pod 的 resume**（需要真集群验证）：把"task 内跳过"升级为"不拉 Pod"。

## 7. 复现与 CI 触发规则

- 离线全绿：`for f in tests/test_*.py examples/tts-asr/tests/test_*.py; do python3 "$f"; done`
- 编译器 drift：`python3 tests/update_golden.py --diff`
- 纯净度守卫：CI `unit-tests` 的第 4 步（`grep -rniE ... src/`），本地可手工重跑同一条命令。
- 真实集群冒烟：`gh workflow run k8s-smoke -R 1z5/specargo`（或 push 到 `main` 自动触发）。
- **CI 触发规则**：两个 workflow 的 `paths:` 只含 `src/ examples/ pipelines/ tests/ k8s/ Dockerfile` 与
  workflow 自身；**纯 `docs/` 提交不会触发 CI**（这是预期，不是漏跑）。
