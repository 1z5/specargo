# 部署指南（从零到一个能跑生产的安装）

> 目标读者：拿到这个仓库、要在自己的 K8s 集群上把它跑起来的人。
> 本文只写**已验证**的路径；没验证的部分在文末「生产就绪度」里逐条列出，不粉饰。

## 0. 前置条件

| 项 | 要求 | 说明 |
|---|---|---|
| Kubernetes | 1.25+ | Argo Workflows v3.7.x 的支持范围 |
| Argo Workflows | v3.7.x | 本文按 v3.7.18 验证；**v4 未验证**——v4 改了 CronWorkflow schema（`spec.schedule` → 新版字段），用 v4 CLI `argo lint` 旧 CronWorkflow 会报 unknown field，属版本差异 |
| 对象存储 | S3 兼容（MinIO / OSS / S3） | 跨 Pod 传数据走它，见 `artifacts.py` |
| Python | 3.11+ | 只用于本地编译/校验；集群内用镜像 |
| 镜像仓库 | 可推送 | 集群要能拉取 |

## 1. 安装 Argo Workflows

```bash
kubectl create namespace argo
kubectl apply -n argo -f \
  https://github.com/argoproj/argo-workflows/releases/download/v3.7.18/install.yaml
# 等 controller 就绪
kubectl -n argo rollout status deploy/workflow-controller
```

> 生产建议用官方 Helm chart 并固定版本；`install.yaml` 的 `latest` 不要用在生产。

## 2. 配置 artifact 仓库（关键一步）

框架的产物走对象存储路径契约，Argo 侧需要知道用哪个仓库。**缺这一步会报
`You need to configure artifact storage`（exit 64）。**

```bash
# 1) 凭据
kubectl -n argo create secret generic argo-artifacts \
  --from-literal=accessKey="$ACCESS_KEY" \
  --from-literal=secretKey="$SECRET_KEY"

# 2) 仓库定义（注意 annotation：没有它控制器不认默认仓库）
kubectl apply -f k8s/artifact-repository.yaml
# 改完必须重启控制器，否则配置不生效
kubectl -n argo rollout restart deploy/workflow-controller
```

`k8s/artifact-repository.yaml` 里的 `endpoint` / `bucket` / `region` 按你的存储改。
**`insecure: true` 只在测试端点（http）时用**，生产走 TLS 必须去掉。

## 3. 构建并推送镜像

```bash
docker build -f Dockerfile -t <registry>/specargo:v0.1.0 .
docker push <registry>/specargo:v0.1.0
```

镜像里装了框架包（`pip install /app`）和 S3 兼容服务端（仅用于 CI 冒烟）。
生产镜像**不要带 moto**：用 `examples/tts-asr/Dockerfile` 那种只装业务依赖的写法，
或自行裁剪 `Dockerfile` 里 `moto[server]` 那一行。

## 4. 准备运行时配置（不改 spec）

```yaml
# runtime.prod.yaml
image: <registry>/specargo:v0.1.0
namespace: argo
service_account: argo-workflow          # 见 k8s/rbac.yaml
objstore_endpoint: https://oss.example.com
objstore_env:                            # 注入到容器的环境变量名（不是值）
  OBJSTORE_ACCESS_KEY: ARGO_ACCESS_KEY
  OBJSTORE_SECRET_KEY: ARGO_SECRET_KEY
parallelism: 20                          # workflow 级并发上限（防扇出打爆控制器）
active_deadline_seconds: 7200
pod_gc_strategy: OnWorkflowSuccess
journal: true                            # 状态日志 + onExit 对账
resume: true                             # 集群侧 task 级断点续跑
```

优先级：内置默认 < spec 的 `runtime:` 段 < `--runtime` 文件。所以同一份 spec
可以在测试与生产之间切换，不需要改管线定义。

## 5. 编译、校验、提交

```bash
# 编译（本地可先 lint，失败即不提交）
specargo-compile pipelines/dataset-build.yaml \
  --runtime runtime.prod.yaml -o /tmp/wf.yaml
argo lint -n argo /tmp/wf.yaml

# 提交并等待
argo submit -n argo /tmp/wf.yaml --watch
```

提交后按这个顺序验收（对应 `docs/RUNBOOK.md` 的「例行验收」）：

```bash
argo get -n argo @latest -o json | jq '.status.phase, .status.progress'
argo logs -n argo @latest --tail 20
# journal: true 时，onExit 会做整批对账；对账不过则 workflow 失败
kubectl -n argo get pods -l workflows.argoproj.io/workflow=<name>
```

## 6. 资源与权限

```bash
kubectl apply -f k8s/rbac.yaml      # 仅当 Pod 需要读配置/写事件
kubectl apply -f k8s/quota.yaml     # 命名空间配额与默认 requests/limits
```

`parallelism` 是 workflow 级闸；ResourceQuota 是命名空间级闸。两道都要有——
只写 `parallelism` 时，同时提交多个 workflow 仍会叠加。

## 7. 升级与回滚

| 变更 | 做法 | 回滚 |
|---|---|---|
| 业务代码 | 新镜像 tag + 改 `runtime.yaml` 的 `image` | 改回旧 tag，重跑失败分区 |
| 管线定义 | spec 里 `version:` 不降；改 YAML 走评审 | `git revert` + 重编译 |
| 编译器行为 | 有意变更必须更新 `tests/golden/` 并提交 | revert 该提交 |
| 定时任务 | `CronWorkflow` 用 `suspend: true` 暂停 | 改回 `false` |

**升级前先做一件事**：`specargo-compile` 一份当前产物存起来（golden 已覆盖），
升级后对比 `argo lint` 与 diff——编译产物变了就说明集群语义变了。

## 8. 例行验收清单（每次上线后 5 分钟）

- [ ] `status.phase == Succeeded`
- [ ] `status.progress` 的完成数 == 期望分区数 × stage 数（含 gate）
- [ ] `onExit` 对账 PASS（`journal: true` 时）：`expected == verified`，`missing == 0`
- [ ] 对象存储里状态对象数 == 分区数 × stage 数（+1 分区清单）
- [ ] 抽查一个产物：能 `objstore get` 下来且非空
- [ ] 失败分区（若有）已进隔离区并带规则编号

## 9. 生产就绪度（诚实清单）

**已验证**（有 CI 或真集群证据，见 `docs/STATUS.md`）：
- 本地执行、编译产物、质量闸门、对账、断点续跑
- kind 三节点集群：31/31 Pod、5 分区动态扇出、跨节点、journal 对账、resume 跳过
- 200 分区 + `parallelism=20` 规模冒烟（本地 kind，1466s，峰值并发 20）
- 状态日志在真实 S3 HTTP 端点上的读写与并发（本机 moto server）

**未验证 / 需要你在自己环境补**：
- 生产对象存储（MinIO/OSS）的真实吞吐与 RTT 放大
- 千级 Pod / 万级分区的 controller 限流与 API server QPS 曲线
- 多副本高可用（当前 controller/对象存储单副本）、RWX PVC
- 多租户隔离（命名空间级配额已给示例，未做租户级审计）
- 安全加固：镜像非 root、Secret 外置（Vault/KMS）、网络策略
- 灰度发布与流量切换（批处理没有流量概念，但**模型版本灰度**未做）

**结论**：这是一套**语义完整、验证充分、可交付运行**的参考实现；
"生产可用"还差上面这些环境相关的验证与加固——它们不是改代码能补的，
需要在真实集群上跑出来。
