FROM python:3.12-slim

# faster-whisper 依赖 av（内置 ffmpeg 动态库），无需系统 ffmpeg；
# 若后续要直接调 ffmpeg CLI（视频抽帧等），取消下一段注释。
# RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先装依赖（层缓存）：生成端 openai + 校验端 faster-whisper
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 默认入口由 Argo template 的 command 覆盖；本地可直接 python pipeline.py --help
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TTS_OUTPUT_DIR=/output \
    TTS_BOOKS_DIR=/books

CMD ["python", "pipeline.py", "--help"]
