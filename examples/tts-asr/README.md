# examples/tts-asr — TTS 生成 + ASR 回检（领域插件示例）

这是框架的**领域插件示例**，不是框架主线。它把一条真实来源的批量音频管线整体归档在这里，
用来演示"领域管线如何接入框架"，以及"哪些东西属于领域、哪些属于框架"。

- **领域侧**：书籍 JSON 解析、文本编排、TTS 生成（OpenAI / edge-tts）、
  ASR 转写与句子级三态判定、对账 CLI、领域状态与告警。
- **框架侧只提供**：spec 语义、本地 runner、质量检查注册表、状态表、对象存储契约。
- **唯一接口**：示例管线 `pipelines/tts.yaml` 通过顶层 `plugins:` 让框架加载
  `checks_tts.py`（注册 `asr_three_state` 检查）；框架层**不 import** 这里的任何模块。

本示例的**生成与校验链路**依赖外部 API 或模型下载，不在离线 CI 套件内
（CI 只跑本目录的离线状态测试 `tests/`，不装 `openai` / `faster-whisper` 等重依赖）。

## 组成

| 文件 / 目录 | 作用 |
|---|---|
| `pipeline.py` | 生成编排 + 单书 CLI（逐文件断点续跑、限速、状态登记） |
| `tts_client.py` / `edge_tts_client.py` | 两个 TTS 引擎客户端（OpenAI / 微软 edge-tts） |
| `verify_audio.py` | 校验 CLI：扫描 → ASR 转录 → 三态判定 → 修复 → 报告 → 对账登记 |
| `tts_verify/` | 校验核心：`normalizer` / `sentence_matcher` / `comparator` / `reporter` |
| `checks_tts.py` | 把三态判定注册成框架检查插件 `asr_three_state`（`@register`） |
| `book_reader.py` / `models.py` / `text_composer.py` | 书籍 JSON 数据模型与文本编排 |
| `summary.py` | 领域对账 CLI（expected vs actual，非零退出码） |
| `notify_tts.py` | 批次收口告警（webhook 可插拔） |
| `config.py` | 领域配置（音色 / 格式 / 语速 / 路径；读 `.env`） |
| `domain_state.py` | 领域状态与对账逻辑（书籍/章节/音色口径 ↔ 框架通用 `dataset`/`partition`/`variant`） |
| `_bootstrap.py` | 把仓库 `app/` 加入 `sys.path`，让示例脚本可直接 `python3 xxx.py` 运行 |
| `data/book_example.json` | 示例书籍数据（**不含真实业务内容**） |
| `pipelines/tts.yaml` | 示例管线定义（声明 `plugins: [checks_tts]`） |
| `k8s/workflow.yaml` / `k8s/cronworkflow.yaml` | 早期手写的 Argo 编排（文档级示例，见下） |
| `requirements.txt` / `.env.example` / `Dockerfile` | 依赖、环境变量样例、领域镜像 |
| `docs/VERIFICATION.md` | 句子级漏读三态判定的方法、阈值标定与盲区 |
| `tests/test_tts_state.py` | 领域状态测试（离线可跑；不属于根 CI 套件） |

## 依赖与数据

```bash
cd examples/tts-asr
python3 -m pip install -r requirements.txt    # openai / faster-whisper / edge-tts
cp .env.example .env                          # 填 OPENAI_API_KEY（走 edge-tts 引擎时可不填）
```

- **书籍 JSON 结构**（`data/book_example.json`）：
  `book_info {title, author, hook_intro}` + `chapters [{chapter_id, chapter_title, chapter_intro, breakdown}]` + `outro`。
  章节文本由 `text_composer.py` 拼接，输出文件名用 JSON 文件名 stem（如 `book_example`）。
- **首次校验**会下载 `faster-whisper` 模型（几百 MB 量级），之后走本地缓存。
- 产物默认写到 `examples/tts-asr/output/book_example/`；`output/`、`*.db` 已被 `.gitignore` 忽略。

## 两条运行路径

### A. 直接跑领域 CLI（不用框架 runner）

```bash
cd examples/tts-asr
cp .env.example .env            # 填 OPENAI_API_KEY；也可以改用 edge-tts 引擎

# 生成（默认 OpenAI TTS；--engine edge-tts 可免 API key）
python3 pipeline.py --book data/book_example.json --voice cedar
# 断点续跑：输入指纹未变的章节默认跳过；加 --no-skip 强制全量重生成

# 校验漏读（三态判定 + report.json/report.md + 退出码，可直接接 CI）
python3 verify_audio.py output/book_example --min-accuracy 0.90

# 对账：expected vs actual，漏数 / 未过检 → 非零退出码
python3 summary.py --state-db state.db --books data/book_example.json --voice cedar
```

### B. 走框架 runner（验证 spec 语义 + 插件加载）

```bash
# 在仓库根执行；--workdir 会自动创建
cd <repo-root>
python3 app/runner.py examples/tts-asr/pipelines/tts.yaml \
  --state-db /tmp/tts-run/state.db --workdir "$PWD/examples/tts-asr" \
  -p example_dir="$PWD/examples/tts-asr"
```

说明：

- `pipelines/tts.yaml` 顶层的 `plugins: ["checks_tts"]` 会被框架识别：`app/plugins.py`
  把 spec 所在目录及其父目录加入 `sys.path`，再 `importlib` 加载 `checks_tts.py`，
  由它向框架的 checks 注册表注册 `asr_three_state`。这就是"领域 → 框架"的唯一接入方式。
- `-p example_dir=...` 覆盖示例管线的代码目录（YAML 里 `example_dir` 是空占位，由 CLI 注入），
  让 stage 命令调用本目录下的脚本；书籍 JSON 目录由 `params.books_dir: data` 指定。
- `--workdir` 指向示例目录：`pipeline.py` 默认把音频写到示例目录的 `output/`，
  而 `verify` stage 用相对路径 `output/<book>` 取产物，二者要落在同一棵树。
- 这条路径会真实调用 TTS API（生成 stage），没有 API key 时改用 `--engine edge-tts`
  或改跑路径 A。

### C. 编译成 Argo（示例，可选）

```bash
# 在仓库根执行：把示例管线编译成 Workflow CRD（需要自备镜像 / PVC / Secret 才能真正提交）
python3 app/spec2argo.py examples/tts-asr/pipelines/tts.yaml -o /tmp/compiled/tts.yaml
argo lint --offline /tmp/compiled/tts.yaml
```

`k8s/workflow.yaml` / `k8s/cronworkflow.yaml` 是早期手写的 Argo 版本（含 `generate → verify → fix → reconcile`、
`onExit` 收口、semaphore 互斥等），**文档级示例**：引用 `registry.example.com` 镜像与
`tts-*-pvc` / `tts-secrets`，需要自备镜像与存储才能跑；CI 不部署它们，也不提交这两个文件。

## 与框架的边界（插件加载方式）

| 方向 | 规则 |
|---|---|
| `examples → framework` | 允许：本示例 import 框架导出的接口（`checks.register`、spec 语义、环境契约） |
| `framework → examples` | **禁止**：`app/**` 不得 import 本目录的任何模块（CI grep 守卫），领域检查只能由 `plugins:` 显式加载 |
| 环境变量 | 框架契约是 `PIPELINE_*`（如 `PIPELINE_RUN_ID` / `PIPELINE_STATE_DB`）；本示例自己的 `TTS_*`（音色 / 格式等）只属于 `config.py`。框架对旧的 `TTS_RUN_ID` / `TTS_STATE_DB` / `TTS_ARGO_*` / `TTS_INPUT_DIR` 保留一个版本的兼容读取（`app/envcompat.py`） |
| 状态与对账 | 框架 `state.py` 提供通用 `pipeline_run` / `pipeline_item` 状态表；本示例的领域对账（书籍/章节口径）在 `summary.py` + `domain_state.py` 里，不进入框架 |
| Argo 编排 | 框架提供 `app/spec2argo.py` 编译器与根 `k8s/` 的 CI 基础设施；本目录 `k8s/` 是领域示例，未进 CI |

## 校验机制

句子级漏读三态判定（FOUND / UNSURE / MISSING）、阈值标定方法与已知盲区，见
[`docs/VERIFICATION.md`](docs/VERIFICATION.md)。

## 测试

```bash
# 在仓库根执行（脚本自己把 app/ 与示例目录加入 sys.path，不依赖 cwd）
python3 examples/tts-asr/tests/test_tts_state.py
```

该测试只依赖标准库（不 import `openai` / `faster-whisper`），覆盖书籍口径 → 通用状态表映射、
对账与重复检测。CI 的 `unit-tests` 会单独跑它（不装重依赖），但它不属于根 `tests/` 的四个套件；
修改领域状态逻辑时也请在本地手动跑一遍。
