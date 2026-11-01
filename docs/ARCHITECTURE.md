# 架构与数据流

本文描述项目的分层结构、模块职责与数据流转。

## 1. 总体分层

项目按三条工作流组织，代码层相互独立，只通过**文件系统契约**交互：

```
┌──────────────────────────────────────────────────────────────┐
│  数据层 (只读)                                                 │
│  书籍 JSON ── book_reader.py ──► Book/Chapter 数据模型          │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  生成层 (TTS)                                                  │
│  main.py (CLI) ─► pipeline.py (编排)                           │
│                    ├─ text_composer.py (文案拼装)              │
│                    └─ tts_client.py (OpenAI API 调用)          │
│  输出: output/<slug>/<slug>_N_<voice>.mp3 + .prompt.txt        │
│        + .input.txt + .input.sha256                            │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  校验层 (ASR)                                                  │
│  verify_audio.py (CLI) ─► faster-whisper (转录)                │
│                    ├─ tts_verify/normalizer.py (归一化)        │
│                    ├─ tts_verify/comparator.py (对比定位)      │
│                    └─ tts_verify/reporter.py (报告)            │
│  输出: verify_report/report.json + report.md                   │
└──────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  对账层                                                        │
│  summary.py ─► state.py (expected vs actual)                   │
│  输出: 对账报告 + Argo 输出参数 (ok / missing / duplicates)     │
└──────────────────────────────────────────────────────────────┘
```

**关键原则**：生成层不校验，校验层不生成（`--fix` 例外：校验层调用 `tts_client` 重生成，
这是唯一跨层调用点，且带备份回滚）。

## 2. 模块职责

### 根目录脚本

| 模块 | 职责 | 维护注意 |
|---|---|---|
| `main.py` | 生成端 CLI：解析参数、装配 Config、分发多种模式 | 只做分发，不放业务逻辑 |
| `pipeline.py` | 生成编排：遍历书籍/章节、调 tts_client、控制 API 限速、断点续跑判定、状态登记 | 新增"每音频多块"逻辑改这里 |
| `tts_client.py` | TTS 封装：调用、重试、落盘影子文件与输入指纹 | 更换引擎时实现同接口 `generate(text, voice, output_path)` |
| `text_composer.py` | 把 Book/Chapter 拼成 TTS 输入文案 | 模板改动只影响文案 |
| `book_reader.py` | 读取书籍 JSON → 数据模型 | 输入格式变更改这里 |
| `models.py` | 纯数据类（Book/BookInfo/Chapter） | 保持无逻辑 |
| `config.py` | 加载 .env → Config | 新配置项在这里声明默认值 |
| `verify_audio.py` | 校验 CLI：扫描 → 转录 → 三态判定 → 修复 → 报告 → 对账登记 | 退出码 0/1 供 CI 消费 |
| `summary.py` | 对账 CLI：expected vs actual，漏数/未过检/重复 | 非零退出码让工作流失败 |
| `notify.py` | 批次收口：run 级汇总 + webhook 告警 | 未配置 webhook 时只打日志 |

### 可靠性模块

| 模块 | 职责 |
|---|---|
| `resume.py` | 输入指纹（sha256）与跳过判定——断点续跑与幂等的落地 |
| `state.py` | item 级状态表（run/item 两表）+ 对账 + 重复检测 |
| `argo_io.py` | Argo 输出参数/artifact 写入，把业务结果暴露给工作流下游 |

### tts_verify/ 包（校验层）

| 模块 | 职责 | 接口 |
|---|---|---|
| `normalizer.py` | 归一化文本（小写、去标点、数字词→数字、数字占位、连字符双形态） | `normalize(text) -> str` |
| `comparator.py` | 词级 SequenceMatcher 对比，提取漏读/错读 | `compare(src, tgt, min_gap_words) -> Comparison` |
| `sentence_matcher.py` | 句子级模糊匹配与三态判定 | `find_sentence_results(...)` |
| `reporter.py` | 结果数据类 + JSON/MD 报告 | `FileResult`, `Report` |

## 3. 关键数据流

### 3.1 生成流

```
find_book_json_files(books_dir)             # 递归找 *.json
  └─► 每本书: read_book() → Book
        └─► generate_book():
              ├─ opening: compose_opening(book)      → <slug>_0_<voice>.mp3
              ├─ 每章:    compose_chapter(chapter)    → <slug>_N_<voice>.mp3
              └─ closing: compose_closing(book)      → <slug>_<N+1>_<voice>.mp3
  每文件之间 sleep(api_delay_seconds)        # 限速
  每个文件生成前: should_skip() 判定 → 命中则跳过（断点续跑）
  每个文件成功后: 落盘 .input.sha256 + tts_item 状态登记
```

- 章节号：优先 `chapter.chapter_id`，缺失时用序号
- **每章节 = 一次 TTS 调用**，这是漏读的主要诱因（见 VERIFICATION.md）
- `tts_client.generate()` 每次调用先落盘 `.prompt.txt`（日志）与 `.input.txt`（原文基准），
  成功后落盘 `.input.sha256`（输入指纹）

### 3.2 校验流

```
verify_audio.py <音频目录>
  └─► 扫描音频 → 转录(带缓存) → 提取原文(.input.txt 优先)
        └─► 句子级三态判定 → 词级覆盖率/准确率
              └─► (--fix) 失败重生成 + 复检 + 回滚
                    └─► report.json / report.md + tts_item 登记 + Argo 输出参数
```

## 4. 与 Argo Workflows 的对应关系

| 本项目概念 | Argo 概念 |
|---|---|
| `generate_book()` 的四段 plan（opening/各章/closing） | DAG task 依赖 |
| `tts_client` 的指数退避重试 | `retryStrategy`（limit/backoff/retryPolicy） |
| `should_skip()` 输入指纹判定 | 任务幂等（Argo 只保证"再执行一次"，安全性由任务自己保证） |
| `tts_item` 状态表 | 外部状态存储（Pod 之间不能靠 emptyDir 共享） |
| `summary.py` 对账 | DAG 末节点 / `onExit` 汇总 |
| `verify_audio.py` 退出码 + Argo 输出参数 | node `outputs.parameters` 供 `when` 分支 |
| `pipeline.py --book X` 单书入口 | `withItems` 扇出，一个 Pod 一本书 |
