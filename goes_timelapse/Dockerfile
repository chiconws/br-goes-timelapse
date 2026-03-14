ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm
FROM ${BUILD_FROM}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
        libexpat1 \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/goes_timelapse/src
COPY rootfs /

ENV PYTHONPATH=/opt/goes_timelapse/src

RUN chmod +x /etc/services.d/goes-timelapse/run
