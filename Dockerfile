FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    MENDARR_DATA_DIR=/data \
    MENDARR_HOST=0.0.0.0 \
    MENDARR_PORT=8095

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tini gosu \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 mendarr \
    && useradd --uid 1000 --gid mendarr --create-home --home-dir /home/mendarr mendarr \
    && mkdir -p /app /data \
    && chown -R mendarr:mendarr /app /data /home/mendarr

WORKDIR /app
COPY requirements-runtime.txt ./
RUN pip install -r requirements-runtime.txt

COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
COPY docker-entrypoint.sh ./
RUN chmod +x /app/docker-entrypoint.sh \
    && chown -R mendarr:mendarr /app /data

EXPOSE 8095

ENTRYPOINT ["tini", "--", "/app/docker-entrypoint.sh"]
