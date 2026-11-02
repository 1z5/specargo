# 通用镜像：安装 specargo 包（框架层，纯标准库 + PyYAML）+ S3 兼容对象存储服务端。
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
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt "moto[server]"

# 框架本体：包化安装后编译产物用 `python3 -m specargo.<module>` 调用，
# 不再依赖镜像里存在 /app/*.py 源文件。
COPY pyproject.toml /app/pyproject.toml
COPY src/ /app/src/
RUN pip install --no-cache-dir /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 冒烟验证：包可导入 + -m 调用可用
RUN python3 -c "import specargo; print('specargo', specargo.__version__, 'ready')"

CMD ["python3", "-c", "print('specargo ci image ready')"]
