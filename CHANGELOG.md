# 变更日志

本文件记录**对外可见的行为变化**。内部重构不列；破坏性变更会标 ⚠️。

## v0.1.0 — 首个可交付版本

框架层功能闭环：**一份 spec，两个执行目标**（本地 runner / Argo CRD），
可靠性语义由两侧共享。

### 能力

- **声明式定义**：stage / 依赖 / 分区（list·range·discover）/ 变体 / 重试 / 超时 /
  质量闸门 / 产物契约；解析期严格校验（未知字段、类型、依赖成环、artifact 不闭合、
  参数未声明、版本过高）。
- **本地执行**：依赖拓扑排序与级联阻断、框架级断点续跑（输入指纹）、retry 分层、
  超时、质量闸门、item 状态登记、expected vs actual 对账（退出码收口）。
- **集群执行**：编译为 Argo Workflow CRD——DAG + `withParam` 动态扇出、
  `gate-<stage>` 质量闸门、artifacts 路径契约（上游 put / 下游 get）、
  可选 `journal`（状态日志 + `onExit` 对账）与 `resume`（task 级断点续跑）、
  workflow 级 `parallelism`。
- **常驻服务**：`/healthz` `/readyz` `/process`（同步）/ `/tasks`（异步）`/metrics`
  `/version`；SIGTERM 优雅停机（摘流量 → 等在途 → 退出）。
- **存储抽象**：本地目录 / S3 兼容双实现；确定性分片 + 数据集 manifest（条数、
  分片清单、逐片 sha256、输入指纹）。
- **工程约束**：golden 编译产物锁行为、编译产物零残留模板变量断言、
  文档用例数守卫、框架层纯净度守卫（领域词不得回漏）。
- **交付物**：Dockerfile、`k8s/rbac.yaml`、`k8s/quota.yaml`、
  `docs/DEPLOYMENT.md`、`docs/RUNBOOK.md`。

### 验证

离线套件 172 例（框架 168 + 领域示例 4），CI 双 Python 版本；kind 三节点集群冒烟
（31/31 Pod、journal 对账、resume 跳过）；200 分区 + `parallelism=20` 规模冒烟；
状态日志在真实 S3 HTTP 端点上的读写与并发。详见 `docs/STATUS.md`。

### 已知边界（v0.1.0 不承诺）

- 未映射的 Argo 特性：script 型 template、`synchronization`、`suspend`、
  「不拉 Pod」形态的 skip（需 WorkflowTemplate + `when`）。
- 集群 gate 仅支持单文件 artifact 的跨 Pod 校验。
- 生产环境相关未验证：真实对象存储吞吐、千级 Pod、多副本 HA、RWX PVC、多租户审计。
- 未提供：镜像非 root、Secret 外置（Vault/KMS）、网络策略、灰度发布。
