# ROADMAP

按价值/风险排序的后续工作。已完成项见 `README.md` 的「已验证」章节。

## 进行中 / 下一步

### 1. `spec2argo` 编译器（最高价值）

**问题**：目前本地 runner 与集群 workflow 是两份定义——
`pipelines/etl-demo.yaml`（3 分区）与 `k8s/workflow-etl-smoke.yaml`（5 分区），
retry 配置、质量闸门写法都各自维护，**必然漂移**。

**方案**：写一个把 `PipelineSpec` 编译成 Argo CRD 的编译器：

| PipelineSpec | Argo CRD |
|---|---|
| `stages` + `dependencies` | `dag.tasks` + `dependencies` |
| `partitions` | `withItems` / `withParam`（含 discover 动态扇出） |
| `RetryPolicy` | `retryStrategy` |
| `Stage.timeout` | template `timeout` |
| `checks` | 独立的 `validate` 阶段（跑 `checks.py` 插件） |
| `artifacts` 声明 | 对象存储路径契约（`objstore.py`） |

**验收**：CI 编译 `pipelines/etl-demo.yaml` → `argo lint` 通过 → **用编译产物**跑集群冒烟，
替换手写 YAML。这样才真正兑现"同一份 spec 本地/集群都执行"。

**规模**：约 200–300 行 + 测试。**风险**：中（新增一层，但可先只覆盖 ETL demo 子集）。

### 2. runner 回传真实 attempt 次数

`runner.py` 里 `attempts = 0` 是硬编码的，`_execute()` 内部的重试计数没有回传，
导致状态表 `attempt` 列只是"被 upsert 过几次"，不是真实重试次数。
改为让 `_execute()` 返回 `(ok, out, attempts)`。

### 3. service 的 job 状态落 SQLite

当前 job 记录纯内存：服务重启后 `GET /tasks/{id}` 返回 404，但状态表里还有记录。
应把 job 记录也落 `state.py` 的表（或新建 `service_job` 表），重启后可查、可重试。

## 扩展验证

### 4. 规模测试：千级分区

`partitions: {from: range, start: 1, end: 200}` 跑一轮，观测：
- workflow-controller 的队列深度与 API server QPS
- Pod 调度延迟、`parallelism` 上限
- 对象存储吞吐（当前 moto 是进程内实现，需换真 MinIO 才能测）

产出数据写进 README，补上"千级 Pod 未实测"这一条。

### 5. 失败注入测试

故意让某分区 `validate` 失败，验证：
- task 失败 → 下游被依赖语义跳过 → 对账报 missing → 退出码非 0
- `onExit` 仍收口
- 告警通道收到通知

### 6. WorkflowTemplate + 版本化

`k8s/workflowtemplate.yaml` + `workflowTemplateRef`，演示 DAG 改版如何灰度与回滚
（对应"工作流版本管理与发布"）。

## 已知未覆盖

- ReadWriteMany PVC（当前用对象存储绕开，未验证真 RWX）
- 多副本高可用（object-store / workflow-controller 都是单副本）
- 生产级对象存储（MinIO/OSS/S3）的真实吞吐与成本
- gRPC 服务形态（当前只有 HTTP）
