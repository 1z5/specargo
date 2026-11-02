# 通用镜像：框架层 app/（纯标准库 + PyYAML）+ S3 兼容对象存储服务端。
#
# 为什么把对象存储做进镜像：集群冒烟测试需要一个 S3 兼容端点来验证
# Argo 的 artifact 传递链路。依赖外部镜像（Docker Hub / Quay）会引入
# 拉取失败的不确定性，所以用 pip 装的 moto server 自建端点——
# 验证的是 Argo 侧的 artifact 机制，与生产用 MinIO/OSS/S3 走同一套 S3 API。
#
# 构建：docker build -f Dockerfile -t specargo:ci .
# 领域示例镜像见 examples/tts-asr/Dockerfile。
FROM python:3.12-slim

WORKDIR /app

# 框架依赖（PyYAML）+ moto[server]（S3 兼容 HTTP 服务端，默认端口 5000）
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt "moto[server]"

COPY app/ /app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python3", "-c", "print('specargo ci image ready')"]
