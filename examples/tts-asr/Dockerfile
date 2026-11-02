# examples/tts-asr 领域示例镜像
#
# 构建（在仓库根目录，需要同时打包框架包与示例代码）：
#   docker build -f examples/tts-asr/Dockerfile -t specargo-tts-asr:latest .
#
# 说明：faster-whisper 依赖 av（内置 ffmpeg 动态库），无需系统 ffmpeg；
# 如需直接调 ffmpeg CLI，取消下面注释。
FROM python:3.12-slim

# RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 示例依赖（openai / faster-whisper / edge-tts）
COPY examples/tts-asr/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 框架层：安装 specargo 包（编译产物与示例都用 `python3 -m specargo...` 调用）
COPY pyproject.toml /app/pyproject.toml
COPY requirements.txt /app/framework-requirements.txt
COPY src/ /app/src/
RUN pip install --no-cache-dir -r /app/framework-requirements.txt /app

# 示例层（领域代码）
COPY examples/tts-asr/ /app/examples/tts-asr/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PIPELINE_OUTPUT_DIR=/output \
    PIPELINE_BOOKS_DIR=/books

CMD ["python", "/app/examples/tts-asr/pipeline.py", "--help"]
