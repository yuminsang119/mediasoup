# ============================================================
# mediasoup RTMP/RTSP → WebRTC Streaming Server
# Docker Production Image
#
# 서버 사양:
#   CPU: Intel Xeon Gold 64코어 × 2 (128코어)
#   RAM: 128GB
#   GPU: NVIDIA A2 × 3 (NVENC 하드웨어 인코딩)
#
# 빌드: docker compose build
# 실행: docker compose up -d
# ============================================================

# ── Stage 1: Build mediasoup worker ──────────────────────────
FROM node:22-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    python3-pip \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/mediasoup

# 의존성 캐싱을 위해 package.json 먼저 복사
COPY package.json package-lock.json* ./
COPY worker/Makefile worker/tasks.py worker/meson.build worker/meson_options.txt ./worker/
COPY worker/subprojects ./worker/subprojects/
COPY worker/include ./worker/include/
COPY worker/src ./worker/src/
COPY worker/fbs ./worker/fbs/
COPY worker/test ./worker/test/
COPY worker/scripts ./worker/scripts/
COPY worker/deps ./worker/deps/
COPY node ./node/
COPY npm-scripts.mjs ./

# mediasoup 빌드 (C++ Worker + TypeScript)
ENV MEDIASOUP_LOCAL_DEV="true"
RUN npm install --ignore-scripts && npm run postinstall

# Worker 바이너리 확인
RUN ls -la worker/out/Release/mediasoup-worker

# ── Stage 2: Production Runtime ──────────────────────────────
FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04

LABEL maintainer="mediasoup-streaming"
LABEL description="RTMP/RTSP to WebRTC Streaming Server with NVIDIA GPU"

# 환경변수
ENV DEBIAN_FRONTEND=noninteractive
ENV NODE_ENV=production
ENV NODE_OPTIONS="--max-old-space-size=8192"
ENV UV_THREADPOOL_SIZE=128

# 시스템 패키지 + FFmpeg(NVENC 지원)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    # FFmpeg with NVENC support
    ffmpeg \
    # nvidia-smi (모니터링용)
    nvidia-utils-535 \
    # Nginx RTMP (드론 RTMP 수신)
    nginx \
    libnginx-mod-rtmp \
    # 네트워크 도구
    iproute2 \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Node.js 22
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# 앱 디렉토리
WORKDIR /app

# Builder에서 빌드된 mediasoup 복사
COPY --from=builder /build/mediasoup/node/lib ./node_modules/mediasoup/node/lib/
COPY --from=builder /build/mediasoup/worker/out/Release/mediasoup-worker ./node_modules/mediasoup/worker/out/Release/
COPY --from=builder /build/mediasoup/package.json ./node_modules/mediasoup/

# 스트리밍 서버 코드 복사
COPY node/examples/rtmp-rtsp-to-webrtc/server.js ./server.js
COPY node/examples/rtmp-rtsp-to-webrtc/package.json* ./

# ws 의존성 설치
RUN cd /app && npm init -y && npm install ws mediasoup

# Nginx RTMP 설정 복사
COPY node/examples/rtmp-rtsp-to-webrtc/docker/nginx-rtmp.conf /etc/nginx/nginx.conf

# 헬스체크 스크립트
COPY node/examples/rtmp-rtsp-to-webrtc/docker/healthcheck.sh /healthcheck.sh
RUN chmod +x /healthcheck.sh

# 시작 스크립트
COPY node/examples/rtmp-rtsp-to-webrtc/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 포트 노출
# 3000: HTTP + WebSocket (뷰어 + 모니터링)
# 1935: RTMP 수신 (드론)
# 40000-49999: WebRTC UDP 미디어 포트
EXPOSE 3000
EXPOSE 1935
EXPOSE 40000-49999/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD /healthcheck.sh

ENTRYPOINT ["/entrypoint.sh"]
