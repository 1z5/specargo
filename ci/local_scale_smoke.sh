#!/usr/bin/env bash
# 编排侧的规模/并发验证：在本地 kind 集群上复现 CI 的冒烟链路，然后跑大规模扇出。
#
# 背景：ROADMAP「扩展验证」里"真实部署的并发/规模验证"缺的是**编排侧**数据——
# controller 排队、Pod 调度延迟、runtime.parallelism 的实际限流效果。这些必须有一个
# 真集群才能测，所以本脚本把 CI（.github/workflows/k8s-smoke.yml）的整套链路搬到本地：
#   kind 3 节点 → Argo v3.7.18 → object-store(moto) → 构建并加载 specargo:ci →
#   编译同一份 pipelines/etl-demo.yaml → 提交 → 断言 → 采集规模数据。
#
# 用法::
#
#   ci/local_scale_smoke.sh up                  # 起集群 + 装 Argo + 部署存储 + 建镜像
#   ci/local_scale_smoke.sh baseline            # 跑 5 分区基线（与 CI 断言一致）
#   ci/local_scale_smoke.sh scale 200 20        # 跑 N=200、parallelism=20 并采集
#   ci/local_scale_smoke.sh report              # 打印采集到的数据汇总
#   ci/local_scale_smoke.sh down                # 删集群（保留 colima VM）
#
# 依赖：colima + docker + kind + kubectl + helm + argo（argo 用 brew 版）。
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CLUSTER="specargo"
NS="argo"
ARGO_VERSION="v3.7.18"
NODE_IMAGE="kindest/node:v1.29.0"
OUT="${OUT:-/tmp/specargo-local-smoke}"
ARGO="argo"          # brew 安装的 argo CLI
mkdir -p "$OUT"

log()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
die()  { printf '\033[31m错误: %s\033[0m\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null || die "缺少命令: $1（brew install $2）"; }

require_tools() {
  need colima colima; need docker docker; need kind kind
  need kubectl kubectl; need argo argo
}

docker_ready() { docker info >/dev/null 2>&1; }

ensure_docker() {
  if docker_ready; then return 0; fi
  log "启动 colima（6 核 / 8G / 60G）"
  colima start --cpu 6 --memory 8 --disk 60 --runtime docker
  docker_ready || die "colima 起来了但 docker 仍不可用"
}

# ---------------------------------------------------------------- up
cmd_up() {
  require_tools; ensure_docker
  log "写 kind 多节点配置（1 控制面 + 2 worker，与 CI 一致）"
  cat > "$OUT/kind-config.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
YAML

  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "集群 $CLUSTER 已存在，跳过创建"
  else
    log "启动 kind 集群（$NODE_IMAGE）"
    kind create cluster --name "$CLUSTER" --config "$OUT/kind-config.yaml" \
      --image "$NODE_IMAGE" --wait 180s
  fi
  kubectl cluster-info >/dev/null
  NODES=$(kubectl get nodes --no-headers | grep -c Ready || true)
  echo "Ready 节点数: $NODES"
  [ "$NODES" -ge 3 ] || die "期望至少 3 个 Ready 节点"

  log "安装 Argo Workflows $ARGO_VERSION"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -n "$NS" -f \
    "https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/install.yaml"
  kubectl create rolebinding argo-default-admin -n "$NS" \
    --clusterrole=admin --serviceaccount="$NS:default" || true
  kubectl wait --for=condition=available --timeout=300s \
    deployment/argo-server deployment/workflow-controller -n "$NS"

  log "构建 specargo:ci 镜像并加载进 kind"
  (cd "$REPO" && docker build -q -f Dockerfile -t specargo:ci . >/dev/null)
  kind load docker-image specargo:ci --name "$CLUSTER"

  log "部署对象存储（moto）与 artifact repository"
  kubectl apply -f "$REPO/k8s/object-store.yaml"
  kubectl apply -f "$REPO/k8s/artifact-repository.yaml"
  kubectl rollout status deployment/object-store -n "$NS" --timeout=180s
  # artifact-repositories ConfigMap 是 controller 启动时读的，后创建必须重启 controller
  kubectl rollout restart deployment/workflow-controller -n "$NS"
  kubectl rollout status deployment/workflow-controller -n "$NS" --timeout=180s
  kubectl exec -n "$NS" deploy/object-store -- python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:5000',
                  aws_access_key_id='test-access-key',
                  aws_secret_access_key='test-secret-key', region_name='us-east-1')
try:
    s3.create_bucket(Bucket='argo-artifacts'); print('bucket argo-artifacts 已创建')
except Exception as e:
    print('bucket 创建:', e)
print('buckets:', [b['Name'] for b in s3.list_buckets()['Buckets']])
"
  log "集群就绪"
  kubectl get nodes -o wide
  kubectl get pods -n "$NS"
}

# ---------------------------------------------------------------- 输入 ConfigMap
make_input() {  # $1 = 分区数
  local n="$1" args="" i
  for i in $(seq 1 "$n"); do
    args="$args --from-literal=$i.jsonl={\"seed\":$i}"
  done
  # shellcheck disable=SC2086
  kubectl create configmap etl-input-partitions -n "$NS" $args \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "输入分区数: ${n}（ConfigMap etl-input-partitions）"
}

# ---------------------------------------------------------------- baseline
cmd_baseline() {
  require_tools; ensure_docker
  make_input 5
  log "编译并提交 etl-demo（5 分区，与 CI 断言一致）"
  (cd "$REPO" && python3 - <<'PY'
import yaml
from specargo.spec import load_spec
from specargo.spec2argo import compile_workflow
spec = load_spec('pipelines/etl-demo.yaml')
spec.k8s['pod_gc_strategy'] = ''          # 保留 Pod 便于断言
wf = compile_workflow(spec)
open('/tmp/compiled/etl-demo.yaml', 'w', encoding='utf-8').write(
    yaml.safe_dump(wf, allow_unicode=True, sort_keys=False, width=120))
PY
  )
  mkdir -p /tmp/compiled
  $ARGO lint -n "$NS" /tmp/compiled/etl-demo.yaml
  $ARGO submit -n "$NS" /tmp/compiled/etl-demo.yaml --watch || true
  local phase
  phase=$($ARGO get -n "$NS" etl-demo -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['status']['phase'])")
  echo "工作流最终状态: $phase"
  [ "$phase" = "Succeeded" ] || die "基线工作流未成功: $phase"
  kubectl get pods -n "$NS" -l workflows.argoproj.io/workflow=etl-demo --no-headers | wc -l \
    | xargs echo "Pod 数（含 gate）:"
}

# ---------------------------------------------------------------- scale
cmd_scale() {  # $1 = 分区数  $2 = parallelism
  local n="$1" par="$2"
  require_tools; ensure_docker
  make_input "$n"
  log "编译 N=$n / parallelism=$par / journal=on 变体"
  (cd "$REPO" && python3 - "$n" "$par" <<'PY'
import sys, yaml
n, par = int(sys.argv[1]), int(sys.argv[2])
from specargo.spec import load_spec
from specargo.spec2argo import compile_workflow
spec = load_spec('pipelines/etl-demo.yaml')
spec.k8s.update({'journal': True, 'resume': True, 'pod_gc_strategy': '',
                 'parallelism': par})
wf = compile_workflow(spec, name=f'etl-scale-{n}')
text = yaml.safe_dump(wf, allow_unicode=True, sort_keys=False, width=120)
assert wf['spec']['parallelism'] == par, 'parallelism 没进产物'
assert 'on-exit' in [t['name'] for t in wf['spec']['templates']], '缺 on-exit'
open(f'/tmp/compiled/etl-scale-{n}.yaml', 'w', encoding='utf-8').write(text)
print(f'  产物: /tmp/compiled/etl-scale-{n}.yaml（parallelism={par}）')
PY
  )
  mkdir -p /tmp/compiled
  $ARGO lint -n "$NS" "/tmp/compiled/etl-scale-$n.yaml"

  # 口径说明：必须数**非终态** Pod（Pending+Init+Running）。
  # 曾按"namespace 里所有 Pod"数，结果把 pod_gc_strategy='' 保留的已完成 Pod 也算进去，
  # 200 分区时量到 1236——那是累计值不是并发，会把"parallelism 没生效"的结论写反。
  log "后台采样在飞 Pod 数（每 2 秒，只数非终态）"
  (
    while true; do
      printf '%s %s\n' "$(date +%s)" \
        "$(kubectl get pods -n "$NS" --no-headers 2>/dev/null \
            | awk '$3!="Completed" && $3!="Succeeded"' | wc -l | tr -d ' ')" \
        >> "$OUT/inflight-$n.log"
      sleep 2
    done
  ) & local sampler=$!
  trap 'kill $sampler 2>/dev/null || true' EXIT

  log "提交工作流 etl-scale-$n"
  local start end
  start=$(date +%s)
  $ARGO submit -n "$NS" "/tmp/compiled/etl-scale-$n.yaml" --watch || true
  end=$(date +%s)
  kill $sampler 2>/dev/null || true
  trap - EXIT
  echo "墙钟耗时: $((end - start))s"

  log "采集结果"
  $ARGO get -n "$NS" "etl-scale-$n" -o json > "$OUT/wf-$n.json"
  python3 - "$n" "$par" "$start" "$end" <<'PY'
import json, sys, collections
n, par, start, end = (int(a) for a in sys.argv[1:])
wf = json.load(open(f'/tmp/specargo-local-smoke/wf-{n}.json'))
st = wf['status']
print(f"  最终状态: {st.get('phase')}")
print(f"  节点总数: {len(st.get('nodes', {}))}")
# 调度延迟：Pod 创建 -> 开始运行（argo 节点的 startedAt - 容器创建时间不可得，
# 用 argo 节点自身的 startedAt/creationTimestamp 差近似，并对 stage 节点统计分布）
lat = []
for node in st['nodes'].values():
    if node.get('type') != 'Pod':
        continue
    a, b = node.get('creationTimestamp'), node.get('startedAt')
    if a and b:
        import datetime as dt
        pa = dt.datetime.fromisoformat(a.replace('Z', '+00:00'))
        pb = dt.datetime.fromisoformat(b.replace('Z', '+00:00'))
        lat.append((pb - pa).total_seconds())
if lat:
    lat.sort()
    print(f"  Pod 就绪延迟(argo 视角): p50={lat[len(lat)//2]:.1f}s "
          f"p95={lat[int(len(lat)*0.95)]:.1f}s max={lat[-1]:.1f}s n={len(lat)}")
# 每个 stage 的吞吐（完成时间分布）
done_at = [v['finishedAt'] for v in st['nodes'].values() if v.get('finishedAt')]
print(f"  完成节点数: {len(done_at)}  墙钟: {end-start}s")
PY

  log "并发曲线（两个口径互相印证）"
  # 口径 1：实时采样的在飞 Pod 数；口径 2：从事后节点时间戳算的精确最大重叠。
  # 两者一致才敢下"parallelism 生效"的结论——只信一个都曾被坑过。
  python3 - "$n" "$par" <<'PY'
import sys, json, datetime as dt
n, par = int(sys.argv[1]), int(sys.argv[2])
try:
    rows = [l.split() for l in open(f'/tmp/specargo-local-smoke/inflight-{n}.log') if l.strip()]
    vals = [int(v) for _, v in rows]
    print(f"  实时采样: {len(rows)} 点，在飞 Pod 峰值 {max(vals) if vals else 0}（设定 {par}）")
except FileNotFoundError:
    print("  实时采样: 无数据")
wf = json.load(open(f'/tmp/specargo-local-smoke/wf-{n}.json'))
def ts(x):
    return dt.datetime.fromisoformat(x.replace('Z', '+00:00')).timestamp() if x else None
pods = [x for x in wf['status']['nodes'].values() if x.get('type') == 'Pod' and x.get('startedAt')]
iv = [(ts(x['startedAt']), ts(x.get('finishedAt')) or 1e18) for x in pods]
ev = sorted([t for a, b in iv for t in ((a, 1), (b, -1))])
cur = peak = 0
for _, d in ev:
    cur += d
    peak = max(peak, cur)
span = max(b for a, b in iv if b < 1e17) - min(a for a, _ in iv)
print(f"  时间戳口径: {len(pods)} 个 Pod，峰值并发 {peak}，执行跨度 {span:.0f}s，"
      f"平均吞吐 {len(pods)/span:.1f} Pod/s")
print(f"  结论: 峰值并发 {'==' if peak == par else '!='} parallelism({par}) → "
      f"{'限流精确生效' if peak <= par else '超出设定（需排查）'}")
lat = sorted(ts(x['startedAt']) - ts(x['creationTimestamp'])
             for x in pods if x.get('creationTimestamp'))
if lat:
    print(f"  Pod 启动延迟: p50={lat[len(lat)//2]:.1f}s p95={lat[int(len(lat)*.95)]:.1f}s max={lat[-1]:.1f}s")
PY

  log "onExit 对账与对象数"
  local onexit_pod
  onexit_pod=$(kubectl get pods -n "$NS" \
    -l "workflows.argoproj.io/workflow=etl-scale-$n" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep 'on-exit' | head -1)
  echo "  onExit Pod: $onexit_pod"
  [ -n "$onexit_pod" ] && kubectl logs -n "$NS" "$onexit_pod" | tail -12 || true
  kubectl exec -n "$NS" deploy/object-store -- python3 - "$n" <<'PY'
import sys, boto3
n = int(sys.argv[1])
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:5000',
                  aws_access_key_id='test-access-key',
                  aws_secret_access_key='test-secret-key', region_name='us-east-1')
def count(prefix):
    total, token = 0, None
    while True:
        kw = {'Bucket': 'argo-artifacts', 'Prefix': prefix}
        if token: kw['ContinuationToken'] = token
        r = s3.list_objects_v2(**kw)
        total += len(r.get('Contents', []))
        if r.get('IsTruncated'): token = r['NextContinuationToken']
        else: return total
run = f'etl-scale-{n}'
status = count(f'runs/{run}/_status/')
parts = count(f'runs/{run}/_run/')
arts = count(f'runs/{run}/')
print(f"  状态对象数: {status}（期望 3×{n}={3*n}）")
print(f"  分区清单对象数: {parts}")
print(f"  run 前缀总对象数: {arts}")
PY

  log "对象存储 RTT（集群内，200 次 GET 实测）"
  kubectl run rtt-probe --rm -i --restart=Never -n "$NS" --image=specargo:ci -- \
    python3 -c "
import time, boto3, statistics
s3 = boto3.client('s3', endpoint_url='http://object-store.argo.svc.cluster.local:5000',
                  aws_access_key_id='test-access-key', aws_secret_access_key='test-secret-key',
                  region_name='us-east-1')
key = None
for o in s3.list_objects_v2(Bucket='argo-artifacts', MaxKeys=1).get('Contents', []):
    key = o['Key']
if not key:
    print('  (bucket 为空，跳过)'); raise SystemExit
lat = []
for _ in range(200):
    t0 = time.perf_counter(); s3.get_object(Bucket='argo-artifacts', Key=key); lat.append((time.perf_counter()-t0)*1000)
lat.sort()
print(f'  GET RTT: p50={statistics.median(lat):.1f}ms p95={lat[int(len(lat)*0.95)]:.1f}ms max={lat[-1]:.1f}ms')
" 2>/dev/null || echo "  (RTT 探针跳过)"
}

cmd_report() {
  log "采集数据汇总（$OUT）"
  ls -la "$OUT" 2>/dev/null | tail -8 || echo "(无数据，先跑 scale)"
}

cmd_down() {
  log "删除 kind 集群（colima VM 保留；要连 VM 一起删: colima delete）"
  kind delete cluster --name "$CLUSTER" || true
}

case "${1:-}" in
  up)       cmd_up ;;
  baseline) cmd_baseline ;;
  scale)    cmd_scale "${2:?用法: $0 scale <分区数> <parallelism>}" "${3:?用法: $0 scale <分区数> <parallelism>}" ;;
  report)   cmd_report ;;
  down)     cmd_down ;;
  *) echo "用法: $0 {up|baseline|scale <N> <par>|report|down}"; exit 2 ;;
esac
