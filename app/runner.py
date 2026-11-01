"""通用 stage 执行器：声明式定义 + 断点续跑 + 质量闸门 + 对账。

这是整个项目"从 TTS 管线升级为批量任务框架"的那一层：
    - 输入：``PipelineSpec``（YAML）+ 分区清单
    - 对每个 (stage × partition)：跳过判定 → 执行（超时 + 重试）→ 质量检查 → 状态登记
    - 收口：对账（expected vs actual）+ 报告 + Argo 输出参数 + 退出码

与 Argo Workflows 的对应关系：
    本 runner ≈ 单机版的 workflow-controller；上集群后同一份 spec 可由
    Argo 解释执行（每个 stage 一个 template，partition 用 withItems 扇出），
    两者共享同一套 resume/state/checks 语义——这是"定义与执行分离"的价值。
"""
from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from checks import CheckResult, run_check
from metrics import METRICS
from spec import Partition, PipelineSpec, build_context, parse_duration, render
from state import (FAILED, GENERATED, VERIFIED, close_run, connect, get_item,
                   open_run, reconcile_generic, upsert_generic_item)


@dataclass
class StageRun:
    stage: str
    partition: str
    status: str                       # generated / verified / failed / skipped
    attempts: int = 0
    checks: list[tuple[str, CheckResult]] = field(default_factory=list)
    detail: str = ""


@dataclass
class RunResult:
    runs: list[StageRun] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.status in (GENERATED, VERIFIED, "skipped") for r in self.runs)

    def summary(self) -> dict:
        counts: dict[str, int] = {}
        for r in self.runs:
            counts[r.status] = counts.get(r.status, 0) + 1
        return counts


class Runner:
    def __init__(self, spec: PipelineSpec, *, state_db: str | None = None,
                 workdir: str | Path = ".", run_id: str | None = None,
                 dry_run: bool = False, verbose: bool = True,
                 resume: bool = True, force: bool = False):
        self.spec = spec
        self.state = connect(state_db) if state_db else None
        self.workdir = Path(workdir)
        self.run_id = run_id or f"{spec.name}-{int(time.time())}"
        self.dry_run = dry_run
        self.verbose = verbose
        self.resume = resume          # 断点续跑：已 verified 的 (stage×partition) 不再执行
        self.force = force            # 强制全量重跑
        self.result = RunResult()
        self.reconcile_ok = True
        # 代码目录与工作目录分离：workdir 放产物，app_dir 放代码
        # （容器里两者通常都是 /app；本地演练时 workdir 常是临时目录）
        spec.params.setdefault("app_dir", str(Path(__file__).resolve().parent))

    # ------------------------------------------------------------------
    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    def _artifact_path(self, stage, partition: Partition, name: str) -> Path:
        """产物路径：优先 stage.artifacts 声明，否则约定 <workdir>/out/<stage>/<partition>。"""
        ctx = build_context(self.spec, partition, self.spec.params)
        if name in stage.artifacts:
            return self.workdir / render(stage.artifacts[name], ctx)
        return self.workdir / "out" / stage.name / f"{partition.dataset}_{partition.key}_{partition.variant}"

    # ------------------------------------------------------------------
    def _execute(self, stage, partition: Partition) -> tuple[bool, str]:
        """执行一个 stage（带超时与重试）。返回 (成功, 输出)。"""
        ctx = build_context(self.spec, partition, self.spec.params)
        command = render(stage.command, ctx)
        timeout = parse_duration(stage.timeout)
        policy = stage.retry

        last_out = ""
        for attempt in range(1, policy.limit + 2):        # limit=0 也表示至少跑一次
            try:
                proc = subprocess.run(
                    command, shell=True, cwd=str(self.workdir),
                    capture_output=True, text=True, timeout=timeout or None,
                )
                last_out = (proc.stdout or "") + (proc.stderr or "")
                if proc.returncode == 0:
                    return True, last_out
                reason = f"exit={proc.returncode}"
            except subprocess.TimeoutExpired:
                reason = f"timeout({stage.timeout})"
            except Exception as exc:
                reason = f"exc={exc!r}"

            if attempt <= policy.limit:
                delay = min(parse_duration(policy.duration) * (policy.factor ** (attempt - 1)),
                            parse_duration(policy.max_duration))
                self._log(f"      [retry {attempt}/{policy.limit}] {reason}，{delay:.0f}s 后重试")
                time.sleep(delay)
            else:
                return False, f"{reason}\n{last_out}"
        return False, last_out

    # ------------------------------------------------------------------
    def _checks(self, stage, partition: Partition) -> list[tuple[str, CheckResult]]:
        ctx = build_context(self.spec, partition, self.spec.params)
        results = []
        for name in stage.checks:
            cfg = {}
            if isinstance(getattr(stage, "check_config", None), dict):
                cfg = stage.check_config.get(name, {})
            artifact = self._artifact_path(stage, partition, stage.outputs[0] if stage.outputs else "")
            results.append((name, run_check(name, artifact, cfg, ctx)))
        return results

    # ------------------------------------------------------------------
    def run(self) -> RunResult:
        spec = self.spec
        if self.state is not None:
            open_run(self.state, self.run_id, note=f"{spec.name}: {len(spec.partitions)} partitions")

        for stage in spec.stages:
            self._log(f"\n=== stage: {stage.name} ({len(spec.partitions)} partitions) ===")
            for partition in spec.partitions:
                tag = f"{partition.dataset}/{partition.key}/{partition.variant or '-'}"
                self._log(f"  [{stage.name}] {tag}")

                # 依赖语义（对应 Argo 的 dependencies）：上游失败则本 stage 不跑。
                # 不实现的后果：下游成功会把上游失败掩盖掉，对账看起来"全绿"。
                blocked_by = self._blocking_dependency(stage, partition)
                if blocked_by:
                    self._log(f"      [skip] 上游 {blocked_by} 未成功，本 stage 不执行")
                    self.result.runs.append(
                        StageRun(stage.name, tag, "skipped", detail=f"blocked by {blocked_by}"))
                    continue

                # 断点续跑：上一轮已 verified 的 (stage × partition) 直接跳过。
                # 判据放在框架层而不是任务层——否则每条管线都要自己实现一遍。
                if self.resume and not self.force and self._already_verified(stage, partition):
                    self._log("      [resume] 上轮已过检，跳过")
                    self.result.runs.append(
                        StageRun(stage.name, tag, "skipped", detail="already verified"))
                    continue

                if self.dry_run:
                    ctx = build_context(spec, partition, spec.params)
                    self._log(f"      (dry-run) {render(stage.command, ctx)}")
                    self.result.runs.append(StageRun(stage.name, tag, "skipped"))
                    continue

                METRICS.inflight.inc(1, stage=stage.name)
                with METRICS.stage_duration.time(stage=stage.name):
                    ok, out = self._execute(stage, partition)
                METRICS.inflight.inc(-1, stage=stage.name)
                attempts = 0
                if not ok:
                    self._log(f"      执行失败: {out.strip()[:200]}")
                    self._register(stage, partition, FAILED, attempts)
                    METRICS.stage_runs.inc(1, stage=stage.name, status=FAILED)
                    self.result.runs.append(StageRun(stage.name, tag, FAILED, attempts, detail=out))
                    continue

                # 质量闸门：不过检 -> failed（阻断交付，而不是"绿着但不对"）
                check_results = self._checks(stage, partition)
                failed_checks = [(n, r) for n, r in check_results if not r.passed]
                for name, r in check_results:
                    mark = "✓" if r.passed else "✗"
                    self._log(f"      [{mark}] {name}: {r.metric} {r.detail[:120]}")

                if failed_checks:
                    detail = "; ".join(f"{n}:{r.detail[:80]}" for n, r in failed_checks)
                    self._register(stage, partition, FAILED, attempts)
                    self.result.runs.append(
                        StageRun(stage.name, tag, FAILED, attempts, check_results, detail))
                    continue

                status = VERIFIED if check_results else GENERATED
                self._register(stage, partition, status, attempts)
                METRICS.stage_runs.inc(1, stage=stage.name, status=status)
                for name, r in check_results:
                    METRICS.check_results.inc(1, check=name,
                                              result="pass" if r.passed else "fail")
                self.result.runs.append(
                    StageRun(stage.name, tag, status, attempts, check_results))

        self._finalize()
        return self.result

    # ------------------------------------------------------------------
    def _already_verified(self, stage, partition: Partition) -> bool:
        if self.state is None:
            return False
        row = get_item(self.state, partition.dataset, partition.key,
                       partition.variant, stage=stage.name)
        return row is not None and row["status"] == VERIFIED

    def _blocking_dependency(self, stage, partition: Partition) -> str | None:
        """返回阻断本 stage 的上游 stage 名；无阻断返回 None。

        先看本次运行的结果，再查状态表（断点续跑时上游可能是上一轮跑的）。
        """
        tag = f"{partition.dataset}/{partition.key}/{partition.variant or '-'}"
        for dep in stage.dependencies:
            prior = [r for r in self.result.runs if r.stage == dep and r.partition == tag]
            if prior:
                # 只有"失败"才阻断；resume 跳过（already verified）不等于失败
                if any(p.status == FAILED for p in prior):
                    return dep
                continue
            if self.state is not None:
                row = get_item(self.state, partition.dataset, partition.key,
                               partition.variant, stage=dep)
                if row is not None and row["status"] == FAILED:
                    return dep
        return None

    def _register(self, stage, partition: Partition, status: str, attempts: int) -> None:
        if self.state is None:
            return
        upsert_generic_item(
            self.state, dataset=partition.dataset, partition=partition.key,
            variant=partition.variant, status=status, run_id=self.run_id,
            attempt=attempts, stage=stage.name,
        )

    def _finalize(self) -> None:
        spec = self.spec
        # 对账：expected = spec 声明的 partition 数，actual = 状态表
        report_lines = [f"# {spec.name} 运行报告 ({self.run_id})", ""]
        report_lines.append("| stage | partition | status | attempts | 检查 |")
        report_lines.append("|---|---|---|---|---|")
        for r in self.result.runs:
            checks = ",".join(f"{'✓' if c.passed else '✗'}{n}" for n, c in r.checks) or "-"
            report_lines.append(f"| {r.stage} | {r.partition} | {r.status} | {r.attempts} | {checks} |")
        report = "\n".join(report_lines) + "\n"
        self._log("\n" + report)

        counts = self.result.summary()
        self._log(f"汇总: {counts}")

        if self.state is not None:
            final_stage = spec.stages[-1].name if spec.stages else None
            # ⚠️ 必须按 dataset 分组对账：item 登记的 dataset 是 partition.dataset
            # （如 dwd_order），不是管线名。用管线名会导致 expected=0、假绿——
            # 这个 bug 在 ETL E2E 里真实踩到过。
            datasets = []
            for part in spec.partitions:
                if part.dataset not in datasets:
                    datasets.append(part.dataset)
            recons = [
                reconcile_generic(self.state, ds,
                                  [(p.dataset, p.key, p.variant) for p in spec.partitions],
                                  final_stage=final_stage)
                for ds in datasets
            ]
            for recon in recons:
                self._log(f"对账: {recon}")
                METRICS.reconcile_missing.set(len(recon["missing"]), dataset=recon["dataset"])
                METRICS.reconcile_not_verified.set(len(recon["not_verified"]),
                                                   dataset=recon["dataset"])
                METRICS.backlog.set(len(recon["missing"]) + len(recon["not_verified"]),
                                    stage=final_stage or "final")
            overall_ok = all(r["ok"] for r in recons)
            self.reconcile_ok = self.reconcile_ok and overall_ok
            close_run(self.state, self.run_id,
                      "succeeded" if self.result.ok and overall_ok else "failed",
                      f"{counts} | " + " | ".join(
                          f"{r['dataset']}: missing={len(r['missing'])} "
                          f"not_verified={len(r['not_verified'])}" for r in recons))
            self.state.close()

        # 指标落盘（供 Prometheus textfile collector 采集；本地演练可直接看）
        metrics_file = self.workdir / "metrics.prom"
        try:
            METRICS.write_file(metrics_file)
            self._log(f"指标: {metrics_file}")
        except Exception as exc:
            self._log(f"指标写入失败: {exc}")

        # Argo 输出参数：让编排层拿到业务结果
        try:
            from argo_io import write_params
            write_params({
                "run-id": self.run_id,
                "status": "pass" if self.result.ok else "fail",
                **{f"count-{k}": v for k, v in counts.items()},
            })
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse
    import os

    from spec import load_spec

    parser = argparse.ArgumentParser(description="声明式批量任务执行器")
    parser.add_argument("spec", help="管线定义 YAML，如 pipelines/tts.yaml")
    parser.add_argument("--state-db", default=os.getenv("TTS_STATE_DB", ""))
    parser.add_argument("--workdir", default=".")
    parser.add_argument("--run-id", default=os.getenv("TTS_RUN_ID", ""))
    parser.add_argument("--dry-run", action="store_true", help="只打印将执行的命令")
    parser.add_argument("--no-resume", action="store_true",
                        help="禁用断点续跑（默认开启：已过检的 stage×partition 会跳过）")
    parser.add_argument("--force", action="store_true", help="强制全量重跑")
    parser.add_argument("-p", "--param", action="append", default=[],
                        help="覆盖参数 key=value")
    args = parser.parse_args()

    overrides = dict(kv.split("=", 1) for kv in args.param if "=" in kv)
    spec = load_spec(args.spec, overrides)
    runner = Runner(spec, state_db=args.state_db or None, workdir=args.workdir,
                    run_id=args.run_id or None, dry_run=args.dry_run,
                    resume=not args.no_resume, force=args.force)
    result = runner.run()
    # 对账不过（漏数/未过检）也必须非零退出：不能"绿着但少数据"
    return 0 if (result.ok and runner.reconcile_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
