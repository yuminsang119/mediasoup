#!/bin/bash
set -e

echo "═══════════════════════════════════════════════════════════"
echo "  mediasoup RTMP/RTSP → WebRTC Streaming Server"
echo "  Docker Container Starting..."
echo "═══════════════════════════════════════════════════════════"

# ── GPU 확인 ──
echo ""
echo "── GPU Status ──"
if command -v nvidia-smi &> /dev/null; then
    nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2>/dev/null || echo "  GPU not available"
else
    echo "  nvidia-smi not found (CPU-only mode)"
fi

# ── FFmpeg NVENC 확인 ──
echo ""
echo "── FFmpeg Encoders ──"
echo "  H264 NVENC: $(ffmpeg -encoders 2>/dev/null | grep -c h264_nvenc) available"
echo "  AV1  NVENC: $(ffmpeg -encoders 2>/dev/null | grep -c av1_nvenc) available"
echo "  VP8  (CPU): $(ffmpeg -encoders 2>/dev/null | grep -c libvpx) available"

# ── 환경변수 → server.js 설정 반영 ──
echo ""
echo "── Configuration ──"
echo "  LISTEN_IP:     ${MEDIASOUP_LISTEN_IP:-0.0.0.0}"
echo "  ANNOUNCED_IP:  ${MEDIASOUP_ANNOUNCED_IP:-auto}"
echo "  NUM_WORKERS:   ${MEDIASOUP_NUM_WORKERS:-64}"
echo "  RTC_PORT_RANGE: ${MEDIASOUP_RTC_MIN_PORT:-40000}-${MEDIASOUP_RTC_MAX_PORT:-49999}"

# ── 스트림 설정 파일 확인 ──
if [ -f "${STREAMS_CONFIG:-/app/config/streams.json}" ]; then
    STREAM_COUNT=$(python3 -c "import json; print(len(json.load(open('${STREAMS_CONFIG:-/app/config/streams.json}'))))" 2>/dev/null || echo "?")
    echo "  STREAMS:       ${STREAM_COUNT} configured"
else
    echo "  STREAMS:       Using default config in server.js"
fi

# ── Nginx RTMP 시작 (드론 RTMP 수신용) ──
echo ""
echo "── Starting Nginx RTMP Server ──"
if [ -f /etc/nginx/nginx.conf ]; then
    nginx -t 2>/dev/null && nginx && echo "  Nginx RTMP started on :1935" || echo "  Nginx RTMP failed to start"
else
    echo "  Nginx config not found, skipping"
fi

# ── 커널 네트워크 최적화 (가능한 경우) ──
if [ -w /proc/sys/net/core/rmem_max ]; then
    echo 8388608 > /proc/sys/net/core/rmem_max 2>/dev/null || true
    echo 8388608 > /proc/sys/net/core/wmem_max 2>/dev/null || true
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Starting mediasoup server..."
echo "  Viewer:  http://localhost:3000"
echo "  Monitor: http://localhost:3000/monitor"
echo "  Stats:   http://localhost:3000/api/stats"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── Node.js 서버 실행 ──
exec node /app/server.js
