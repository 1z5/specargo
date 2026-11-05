# 运维手册（出问题时按这个查）

> 原则：**先看状态，再看事件，再看日志，最后看资源**。不要一上来就重跑——
> 重跑会掩盖问题，而且本框架的 resume 会让"重跑"看起来什么都没发生。

## 0. 四条固定命令

```bash
# 1) 工作流总览
argo list -n argo
# 2) 节点级状态（谁失败、谁还在跑、耗时多久）
argo get -n argo @latest -o json | jq '.status.nodes
  | to_entries | map({name:.value.displayName, phase:.value.phase,
      dur:.value.resourcesDuration, msg:.value.message})'
# 3) 失败节点的日志
argo logs -n argo @latest --tail 100
# 4) K8s 事件（镜像拉取失败、调度失败、OOM 都在这里）
kubectl -n argo get events --sort-by=.lastTimestamp | tail -30
```

---

## 1. 工作流一直 Pending，不创建 Pod

| 先查 | 命令 | 判据 |
|---|---|---|
| 有没有被配额挡住 | `kubectl -n argo describe quota` | `used` 到 `hard` 上限 |
| 节点能不能调度 | `kubectl describe pod <pod>` 看 Events | `Insufficient cpu/memory`、`node(s) had taint`、`didn't match Pod's node affinity` |
| 镜像拉不下来 | 同上，看 `Failed to pull image` | 私有仓库要 `imagePullSecrets` |

**处理**：调 `parallelism` 或配额；给节点池打 taint 并让模板带 toleration；
补 `imagePullSecrets`。**不要**直接加 `--restart` 硬重启。

## 2. 工作流 Failed / Error

看 `status.nodes[].message`，常见三类：

| 现象 | 含义 | 处理 |
|---|---|---|
| `exit code 1` + 业务日志有异常 | 业务失败（数据/代码） | 修数据或代码；数据类问题进隔离区，不要整批重跑 |
| `exit code 137` / `OOMKilled` | 内存不足 | 提 `memory` limit，或改成分块处理 |
| `Pod was active on node ... for too long` / `DeadlineExceeded` | 超时 | 查是数据量变大还是下游变慢；调 `timeout` 前先确认不是卡死 |

## 3. 任务"卡住"（Pod Running 但很久没进度）

```bash
kubectl -n argo logs <pod> --tail 50          # 是不是在等下游
kubectl -n argo top pod <pod>                 # CPU 是不是 0（卡在 IO/网络）
```
- 卡在 `objstore get/put`：对象存储端点不可达或凭据错 → 见第 6 节
- 卡在业务调用：下游限流/超时 → 查下游；本框架的重试只对瞬时错误生效
- **同时提交太多 workflow**：`parallelism` 只管单个 workflow，多个叠加会排队 →
  看 `kubectl -n argo get pods | grep Pending | wc -l`

## 4. 数据遗漏（对账失败）

`journal: true` 时 `onExit` 会跑整批对账，失败信息形如
`expected=200 verified=197 missing=3`。

```bash
# 定位缺哪几个分区：读分区清单 + 逐条查状态对象
python3 -m specargo.pipeline_cli reconcile-journal \
  --run-id <workflow-name> --final-stage <末 stage> \
  --partitions-file /tmp/partitions.json
```

`missing` = 没有状态对象（没跑/被跳过/状态写失败）；
`not_verified` = 有状态但没到终态（跑了但没过检）。

**处理**：按 run_id 重跑缺失分区（`resume: true` 会跳过已完成的），
不要重新提交整个 workflow 从零跑。

## 5. resume 不生效（该跳过的没跳过）

三个必查：

1. **run_id 是否稳定**：集群 run_id = workflow 名。名字变了（新提交）就查不到旧状态。
   正确做法：**删除旧 workflow → 用同名重新提交**（Argo 默认同名会 AlreadyExists）。
2. **状态是否终态**：有 checks 的 stage 只认 `verified`；没 checks 的 `generated` 才算成功。
3. **状态对象还在不在**：对象存储有生命周期规则时，旧对象可能已被清理。

## 6. 对象存储不可用

```bash
# 在任意 Pod 里验证端点与凭据（不必进业务容器）
kubectl -n argo run oss-check --rm -it --image=<registry>/specargo:v0.1.0 -- \
  python3 -m specargo.objstore list --prefix runs/
```

| 报错 | 原因 | 处理 |
|---|---|---|
| `Connection refused` / `timeout` | 端点错、网络策略、DNS | 查 `objstore_endpoint` 与 NetworkPolicy |
| `403` / `AccessDenied` | 凭据或 bucket 策略 | 查 Secret 注入的环境变量名是否与 `objstore_env` 一致 |
| `NoSuchBucket` | bucket 没建 | 先建 bucket |
| `You need to configure artifact storage`（Argo 侧） | 缺 artifact-repository 或没重启控制器 | 见部署指南第 2 节 |

## 7. 状态对象损坏

`journal` 层读到坏记录会打印 `状态记录损坏（按漏数计入）` 并继续——**不会崩**。
该分区对账时表现为 `missing`。

**处理**：删掉损坏对象后重跑该分区即可（resume 会认为它没跑过）。
注意：**不要**手工往状态对象里塞内容去"补齐"——状态是证据，不是配置。

## 8. 重复数据 / 重跑产生两份

- 数据侧：写入必须幂等（upsert / 唯一键）。本框架的 item 主键是四元组
  `(dataset, partition, variant, stage)`，重跑是覆盖不是追加。
- 产物侧：路径契约按 `runs/<run_id>/<partition>/<stage>` 分层，
  **不同 run_id 不会互相覆盖**；同 run_id 重跑会覆盖，这是预期行为。

## 9. 例行巡检（建议每日）

```bash
argo list -n argo --running                 # 有没有卡住的
kubectl -n argo get pods | grep -c Pending  # 排队数
kubectl -n argo describe quota              # 配额水位
```
指标侧（`metrics.py` 输出的 Prometheus 文本）重点看：
`pipeline_stage_runs_total{status="failed"}`、`pipeline_stage_duration_seconds` 的 P95、
对账缺失数（`journal` 路径由 onExit 汇总）。
