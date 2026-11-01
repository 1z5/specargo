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

已知边界：编译器目前覆盖 ETL demo 用到的特性子集（container 命令 + checks +
artifacts）；script 型 template、synchronization、suspend 等尚未映射。

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

故意让某分区 `validate` 失败，验证：
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
