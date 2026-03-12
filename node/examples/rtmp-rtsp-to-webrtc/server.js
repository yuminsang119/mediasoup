/**
 * RTMP/RTSP → WebRTC Streaming Server (Production)
 *
 * 서버 사양:
 *   CPU:  Intel Xeon Gold × 2 (64코어 × 2 = 128코어 / 256스레드)
 *   RAM:  128GB DDR4 ECC
 *   GPU:  NVIDIA A2 × 3 (하드웨어 인코딩 NVENC)
 *
 * Architecture:
 *   Drone (RTMP) ─┐                                    ┌─→ WebRTC Client
 *   CCTV  (RTSP) ─┤→ FFmpeg(NVENC/GPU) → RTP ─┐       │
 *   CCTV  (RTSP) ─┘                            ├→ mediasoup Workers (128코어) ──→ WebRTC Clients
 *   ...           ─→ FFmpeg(NVENC/GPU) → RTP ──┘       │
 *                                                       └─→ WebRTC Client
 *
 * Features:
 *   - 128코어 활용: mediasoup Worker 풀 (코어당 1 Worker)
 *   - NVIDIA A2 × 3: FFmpeg NVENC 하드웨어 인코딩 (GPU별 라운드로빈)
 *   - 실시간 모니터링 대시보드: CPU, RAM, GPU, 스트림, Worker 상태
 *   - 자동 Worker 로드밸런싱
 *   - FFmpeg 프로세스 자동 재시작
 */

const mediasoup = require('mediasoup');
const http = require('http');
const WebSocket = require('ws');
const { spawn, execSync } = require('child_process');
const os = require('os');
const fs = require('fs');
const path = require('path');

// ─── Server Hardware Configuration ───────────────────────────────────────────

const HARDWARE = {
  // Intel Xeon Gold × 2, 64코어 × 2
  totalCores: 128,
  totalThreads: 256,
  // 128GB RAM
  totalRamGB: 128,
  // NVIDIA A2 GPU × 3
  gpus: [
    { id: 0, name: 'NVIDIA A2', nvencSlots: 16 },
    { id: 1, name: 'NVIDIA A2', nvencSlots: 16 },
    { id: 2, name: 'NVIDIA A2', nvencSlots: 16 },
  ],
};

// ─── Configuration ───────────────────────────────────────────────────────────

const config = {
  listenIp: '0.0.0.0',
  announcedIp: process.env.MEDIASOUP_ANNOUNCED_IP || null,
  httpPort: 3000,
  monitorPort: 3001,

  // TURN/STUN 서버 설정 (coturn)
  turn: {
    enabled: true,
    host: process.env.TURN_SERVER || process.env.PUBLIC_IP || '127.0.0.1',
    port: parseInt(process.env.TURN_PORT) || 3478,
    username: process.env.TURN_USERNAME || 'mediasoup',
    password: process.env.TURN_PASSWORD || 'mediasoup123',
  },

  // 녹화 설정
  recording: {
    enabled: process.env.RECORDING_ENABLED === 'true',
    dir: process.env.RECORDING_DIR || '/recordings',
    // HLS 세그먼트 길이 (초)
    hlsSegmentDuration: 6,
    // HLS playlist 유지 세그먼트 수 (0 = 전체 유지)
    hlsListSize: 0,
    // MP4 분할 시간 (초, 3600 = 1시간)
    mp4SegmentDuration: 3600,
    // 보관 기간 (시간, 0 = 무제한)
    retentionHours: parseInt(process.env.RECORDING_RETENTION_HOURS) || 168,
  },

  // mediasoup Worker 풀 설정
  // 128코어 중 절반을 mediasoup에, 나머지를 FFmpeg/OS에 할당
  numWorkers: 64,
  worker: {
    logLevel: 'warn',
    rtcMinPort: 40000,
    rtcMaxPort: 59999,     // 20000개 포트 (worker당 ~312포트)
  },

  // WebRTC Transport 설정 (대용량 서버용)
  webRtcTransport: {
    initialAvailableOutgoingBitrate: 10000000,  // 10 Mbps 초기값
    maxIncomingBitrate: 50000000,               // 50 Mbps 최대
  },

  mediaCodecs: [
    {
      kind: 'audio',
      mimeType: 'audio/opus',
      clockRate: 48000,
      channels: 2,
    },
    {
      kind: 'video',
      mimeType: 'video/H264',
      clockRate: 90000,
      parameters: {
        'packetization-mode': 1,
        'profile-level-id': '42e01f',
        'level-asymmetry-allowed': 1,
      },
    },
    {
      kind: 'video',
      mimeType: 'video/VP8',
      clockRate: 90000,
    },
    {
      kind: 'video',
      mimeType: 'video/AV1',
      clockRate: 90000,
    },
  ],

  streams: [
    // ── 드론 스트림 (RTMP) ──
    {
      id: 'drone-1',
      label: '드론 카메라 1',
      type: 'rtmp',
      url: 'rtmp://localhost:1935/live/drone1',
      videoCodec: 'H264',
      hwAccel: true,       // GPU 하드웨어 인코딩 사용
      resolution: '1920x1080',
      fps: 30,
      bitrate: '4M',
    },
    {
      id: 'drone-2',
      label: '드론 카메라 2',
      type: 'rtmp',
      url: 'rtmp://localhost:1935/live/drone2',
      videoCodec: 'H264',
      hwAccel: true,
      resolution: '1280x720',
      fps: 30,
      bitrate: '2M',
    },
    // ── CCTV 스트림 (RTSP) ──
    {
      id: 'cctv-1',
      label: 'CCTV 정문',
      type: 'rtsp',
      url: 'rtsp://admin:pass@192.168.1.100:554/stream1',
      videoCodec: 'H264',
      hwAccel: true,
      resolution: '1920x1080',
      fps: 25,
      bitrate: '3M',
    },
    {
      id: 'cctv-2',
      label: 'CCTV 주차장',
      type: 'rtsp',
      url: 'rtsp://admin:pass@192.168.1.101:554/stream1',
      videoCodec: 'H264',
      hwAccel: true,
      resolution: '1920x1080',
      fps: 25,
      bitrate: '3M',
    },
    {
      id: 'cctv-3',
      label: 'CCTV 로비',
      type: 'rtsp',
      url: 'rtsp://admin:pass@192.168.1.102:554/stream1',
      videoCodec: 'H264',
      hwAccel: true,
      resolution: '1280x720',
      fps: 15,
      bitrate: '1.5M',
    },
  ],
};

// ─── Global State ────────────────────────────────────────────────────────────

const workers = [];              // mediasoup Worker 풀
const routers = [];              // Worker당 Router
const streams = new Map();       // streamId → stream state
const consumers = new Map();     // consumerId → consumer
const clientSessions = new Map();// ws → session state
let nextWorkerIdx = 0;
let nextGpuIdx = 0;
let ssrcCounter = 10000000;

// 이벤트 로그 (119 관제 대시보드용)
const eventLog = [];
const MAX_EVENTS = 100;
let wssRef = null; // WebSocket.Server reference for broadcasting

function addEvent(type, message, data = {}) {
  const event = {
    id: Date.now() + '-' + Math.random().toString(36).substr(2, 6),
    time: new Date().toISOString(),
    type, // 'stream' | 'recording' | 'client' | 'system'
    message,
    ...data,
  };
  eventLog.unshift(event);
  if (eventLog.length > MAX_EVENTS) eventLog.pop();

  // Broadcast to all connected clients
  if (wssRef) {
    const payload = JSON.stringify({ action: 'streamEvent', event });
    wssRef.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  }
  return event;
}

// 모니터링 통계
const stats = {
  startTime: Date.now(),
  totalConnections: 0,
  activeConnections: 0,
  totalStreams: 0,
  activeStreams: 0,
  totalConsumers: 0,
  workerStats: [],
  gpuStats: [],
  ffmpegProcesses: [],
};

// ─── Worker Pool ─────────────────────────────────────────────────────────────

async function createWorkerPool() {
  const numWorkers = Math.min(config.numWorkers, os.cpus().length);
  console.log(`Creating ${numWorkers} mediasoup Workers...`);

  for (let i = 0; i < numWorkers; i++) {
    const worker = await mediasoup.createWorker({
      logLevel: config.worker.logLevel,
      rtcMinPort: config.worker.rtcMinPort,
      rtcMaxPort: config.worker.rtcMaxPort,
    });

    worker.on('died', () => {
      console.error(`Worker ${i} died, recreating...`);
      recreateWorker(i);
    });

    const router = await worker.createRouter({ mediaCodecs: config.mediaCodecs });

    workers.push(worker);
    routers.push(router);

    stats.workerStats.push({
      id: i,
      pid: worker.pid,
      routerCount: 1,
      transportCount: 0,
      producerCount: 0,
      consumerCount: 0,
    });
  }

  console.log(`${numWorkers} Workers created (PIDs: ${workers.map(w => w.pid).join(', ').substring(0, 80)}...)`);
}

async function recreateWorker(index) {
  try {
    const worker = await mediasoup.createWorker({
      logLevel: config.worker.logLevel,
      rtcMinPort: config.worker.rtcMinPort,
      rtcMaxPort: config.worker.rtcMaxPort,
    });
    worker.on('died', () => recreateWorker(index));

    const router = await worker.createRouter({ mediaCodecs: config.mediaCodecs });
    workers[index] = worker;
    routers[index] = router;
    stats.workerStats[index].pid = worker.pid;
    console.log(`Worker ${index} recreated (PID: ${worker.pid})`);
  } catch (err) {
    console.error(`Failed to recreate worker ${index}:`, err.message);
    setTimeout(() => recreateWorker(index), 5000);
  }
}

// 라운드로빈으로 Worker 선택 (최소 부하 Worker 우선)
function getNextWorkerIndex() {
  let minLoad = Infinity;
  let bestIdx = 0;

  for (let i = 0; i < workers.length; i++) {
    const load = stats.workerStats[i].consumerCount + stats.workerStats[i].producerCount;
    if (load < minLoad) {
      minLoad = load;
      bestIdx = i;
    }
  }

  return bestIdx;
}

// GPU 라운드로빈 할당
function getNextGpuId() {
  const gpuId = nextGpuIdx % HARDWARE.gpus.length;
  nextGpuIdx++;
  return gpuId;
}

function getNextSsrc() {
  return ssrcCounter++;
}

// ─── FFmpeg Stream Ingestion (GPU Accelerated) ───────────────────────────────

async function ingestStream(streamConfig) {
  const { id, url: streamUrl, type, videoCodec, label, hwAccel,
          resolution, fps, bitrate } = streamConfig;

  const workerIdx = getNextWorkerIndex();
  const router = routers[workerIdx];
  const gpuId = hwAccel ? getNextGpuId() : null;

  console.log(`[${id}] Ingesting → Worker #${workerIdx}, GPU #${gpuId ?? 'CPU'}: ${label}`);

  // PlainTransport for audio
  const audioTransport = await router.createPlainTransport({
    listenInfo: { protocol: 'udp', ip: config.listenIp },
    rtcpMux: false,
    comedia: true,
  });

  // PlainTransport for video
  const videoTransport = await router.createPlainTransport({
    listenInfo: { protocol: 'udp', ip: config.listenIp },
    rtcpMux: false,
    comedia: true,
  });

  const audioRtpPort = audioTransport.tuple.localPort;
  const audioRtcpPort = audioTransport.rtcpTuple.localPort;
  const videoRtpPort = videoTransport.tuple.localPort;
  const videoRtcpPort = videoTransport.rtcpTuple.localPort;

  // Codec mapping
  let ffmpegVideoEncoder, payloadType, mimeType;
  if (hwAccel && videoCodec === 'H264') {
    ffmpegVideoEncoder = 'h264_nvenc';
    payloadType = 101;
    mimeType = 'video/H264';
  } else if (hwAccel && videoCodec === 'AV1') {
    ffmpegVideoEncoder = 'av1_nvenc';
    payloadType = 102;
    mimeType = 'video/AV1';
  } else if (videoCodec === 'H264') {
    ffmpegVideoEncoder = 'libx264';
    payloadType = 101;
    mimeType = 'video/H264';
  } else if (videoCodec === 'AV1') {
    ffmpegVideoEncoder = 'libsvtav1';
    payloadType = 102;
    mimeType = 'video/AV1';
  } else {
    ffmpegVideoEncoder = 'libvpx';
    payloadType = 100;
    mimeType = 'video/VP8';
  }

  const audioSsrc = getNextSsrc();
  const videoSsrc = getNextSsrc();

  // Create producers
  const audioProducer = await audioTransport.produce({
    kind: 'audio',
    rtpParameters: {
      codecs: [{
        mimeType: 'audio/opus',
        payloadType: 97,
        clockRate: 48000,
        channels: 2,
        parameters: { minptime: 10, useinbandfec: 1 },
      }],
      encodings: [{ ssrc: audioSsrc }],
    },
  });

  const videoProducer = await videoTransport.produce({
    kind: 'video',
    rtpParameters: {
      codecs: [{
        mimeType,
        payloadType,
        clockRate: 90000,
        parameters: videoCodec === 'H264'
          ? { 'packetization-mode': 1, 'profile-level-id': '42e01f' }
          : {},
      }],
      encodings: [{ ssrc: videoSsrc }],
    },
  });

  // Update worker stats
  stats.workerStats[workerIdx].producerCount += 2;
  stats.workerStats[workerIdx].transportCount += 2;

  // Build FFmpeg args
  const ffmpegArgs = buildFfmpegArgs({
    streamUrl, type, ffmpegVideoEncoder, gpuId,
    audioRtpPort, audioRtcpPort, videoRtpPort, videoRtcpPort,
    audioSsrc, videoSsrc, payloadType,
    listenIp: '127.0.0.1',
    resolution: resolution || '1280x720',
    fps: fps || 30,
    bitrate: bitrate || '2M',
    hwAccel: hwAccel || false,
  });

  console.log(`[${id}] FFmpeg: ${ffmpegVideoEncoder} @ ${resolution} ${fps}fps ${bitrate}`);

  // Spawn FFmpeg
  const ffmpegProc = spawn('ffmpeg', ffmpegArgs);
  let lastFfmpegStats = '';

  ffmpegProc.stderr.on('data', (data) => {
    const msg = data.toString();
    // 프레임/비트레이트 정보 파싱
    const frameMatch = msg.match(/frame=\s*(\d+)/);
    const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
    const bitrateMatch = msg.match(/bitrate=\s*([\d.]+\s*\w+)/);
    if (frameMatch) {
      lastFfmpegStats = `frame=${frameMatch[1]} fps=${fpsMatch?.[1] || '?'} bitrate=${bitrateMatch?.[1] || '?'}`;
    }
    if (msg.includes('Error') || msg.includes('error')) {
      console.error(`[${id}] FFmpeg: ${msg.trim()}`);
    }
  });

  ffmpegProc.on('close', (code) => {
    console.log(`[${id}] FFmpeg exited (code ${code})`);
    if (code !== 0 && streams.has(id)) {
      console.log(`[${id}] Auto-restart in 5s...`);
      setTimeout(() => {
        if (streams.has(id)) {
          // Close old transports, re-ingest
          try {
            audioTransport.close();
            videoTransport.close();
          } catch {}
          stats.workerStats[workerIdx].producerCount -= 2;
          stats.workerStats[workerIdx].transportCount -= 2;
          ingestStream(streamConfig);
        }
      }, 5000);
    }
  });

  ffmpegProc.on('error', (err) => {
    console.error(`[${id}] FFmpeg spawn error:`, err.message);
  });

  // ── 녹화 FFmpeg 프로세스 (별도) ──
  let recordingProc = null;
  let recordingDir = null;
  if (config.recording.enabled) {
    recordingDir = path.join(config.recording.dir, id, new Date().toISOString().replace(/[:.]/g, '-'));
    fs.mkdirSync(recordingDir, { recursive: true });

    const recArgs = buildRecordingArgs({
      streamUrl, type, recordingDir,
      hlsSegmentDuration: config.recording.hlsSegmentDuration,
      hlsListSize: config.recording.hlsListSize,
      mp4SegmentDuration: config.recording.mp4SegmentDuration,
    });

    recordingProc = spawn('ffmpeg', recArgs);
    recordingProc.stderr.on('data', () => {}); // suppress
    recordingProc.on('close', (code) => {
      console.log(`[${id}] Recording FFmpeg exited (code ${code})`);
    });
    console.log(`[${id}] Recording to ${recordingDir}`);
  }

  const streamState = {
    config: streamConfig,
    workerIdx,
    gpuId,
    audioTransport,
    videoTransport,
    audioProducer,
    videoProducer,
    ffmpeg: ffmpegProc,
    recordingProc,
    recordingDir,
    startTime: Date.now(),
    getStats: () => lastFfmpegStats,
  };

  streams.set(id, streamState);
  stats.activeStreams = streams.size;
  stats.totalStreams++;

  return streamState;
}

function buildFfmpegArgs({
  streamUrl, type, ffmpegVideoEncoder, gpuId,
  audioRtpPort, audioRtcpPort, videoRtpPort, videoRtcpPort,
  audioSsrc, videoSsrc, payloadType, listenIp,
  resolution, fps, bitrate, hwAccel,
}) {
  const args = [];

  // GPU 하드웨어 디코더 + 인코더
  if (hwAccel && gpuId !== null) {
    args.push(
      '-hwaccel', 'cuda',
      '-hwaccel_device', String(gpuId),
      '-hwaccel_output_format', 'cuda',
    );
  }

  // Input options
  args.push('-fflags', '+genpts+discardcorrupt');

  if (type === 'rtsp') {
    args.push(
      '-rtsp_transport', 'tcp',
      '-stimeout', '5000000',      // 5초 타임아웃
      '-i', streamUrl,
    );
  } else {
    // RTMP
    args.push(
      '-listen', '1',
      '-timeout', '30',
      '-i', streamUrl,
    );
  }

  // Audio → Opus RTP
  args.push(
    '-map', '0:a:0?',               // ? = optional (오디오 없는 경우 무시)
    '-c:a', 'libopus',
    '-b:a', '128k',
    '-ac', '2',
    '-ar', '48000',
    '-ssrc', String(audioSsrc),
    '-payload_type', '97',
    '-f', 'rtp',
    `rtp://${listenIp}:${audioRtpPort}?rtcpport=${audioRtcpPort}`,
  );

  // Video → selected encoder RTP
  args.push('-map', '0:v:0');

  // NVIDIA NVENC 인코딩 설정
  if (hwAccel && gpuId !== null) {
    args.push(
      '-c:v', ffmpegVideoEncoder,
      '-gpu', String(gpuId),
      '-preset', 'p4',              // NVENC preset: p1(fastest) ~ p7(slowest)
      '-tune', 'll',                // Low Latency 튜닝
      '-rc', 'cbr',                 // Constant Bitrate (안정적 스트리밍)
      '-b:v', bitrate,
      '-maxrate', bitrate,
      '-bufsize', `${parseInt(bitrate) * 2}M`,
    );
    // 해상도 변환 (GPU 스케일링)
    args.push('-vf', `scale_cuda=${resolution.replace('x', ':')}`);
  } else {
    // CPU 소프트웨어 인코딩
    args.push(
      '-c:v', ffmpegVideoEncoder,
      '-b:v', bitrate,
      '-maxrate', `${parseInt(bitrate) * 1.25}M`,
      '-bufsize', `${parseInt(bitrate) * 2}M`,
      '-s', resolution,
    );
  }

  args.push(
    '-g', String(fps * 2),           // 2초 간격 키프레임
    '-r', String(fps),
    '-ssrc', String(videoSsrc),
    '-payload_type', String(payloadType),
    '-f', 'rtp',
    `rtp://${listenIp}:${videoRtpPort}?rtcpport=${videoRtcpPort}`,
  );

  return args;
}

function buildRecordingArgs({ streamUrl, type, recordingDir,
  hlsSegmentDuration, hlsListSize, mp4SegmentDuration }) {
  const inputArgs = type === 'rtsp'
    ? ['-rtsp_transport', 'tcp', '-stimeout', '5000000', '-i', streamUrl]
    : ['-listen', '1', '-timeout', '30', '-i', streamUrl];

  return [
    '-fflags', '+genpts',
    ...inputArgs,
    // HLS 출력 (라이브 + VOD 재생용)
    '-c:v', 'copy',       // 원본 코덱 그대로 (트랜스코딩 없음)
    '-c:a', 'aac',
    '-b:a', '128k',
    '-f', 'hls',
    '-hls_time', String(hlsSegmentDuration),
    '-hls_list_size', String(hlsListSize),
    '-hls_flags', 'delete_segments+append_list',
    '-hls_segment_filename', path.join(recordingDir, 'seg_%05d.ts'),
    path.join(recordingDir, 'index.m3u8'),
    // MP4 분할 녹화 (보관용)
    '-c:v', 'copy',
    '-c:a', 'aac',
    '-b:a', '128k',
    '-f', 'segment',
    '-segment_time', String(mp4SegmentDuration),
    '-segment_format', 'mp4',
    '-reset_timestamps', '1',
    '-strftime', '1',
    path.join(recordingDir, 'rec_%Y%m%d_%H%M%S.mp4'),
  ];
}

// 녹화 파일 보관 기간 관리
function cleanupOldRecordings() {
  if (!config.recording.enabled || config.recording.retentionHours <= 0) return;

  const maxAge = config.recording.retentionHours * 3600 * 1000;
  const baseDir = config.recording.dir;

  try {
    if (!fs.existsSync(baseDir)) return;
    const streamDirs = fs.readdirSync(baseDir);
    for (const streamId of streamDirs) {
      const streamPath = path.join(baseDir, streamId);
      if (!fs.statSync(streamPath).isDirectory()) continue;

      const sessions = fs.readdirSync(streamPath);
      for (const session of sessions) {
        const sessionPath = path.join(streamPath, session);
        if (!fs.statSync(sessionPath).isDirectory()) continue;

        const stat = fs.statSync(sessionPath);
        if (Date.now() - stat.mtimeMs > maxAge) {
          fs.rmSync(sessionPath, { recursive: true, force: true });
          console.log(`[Recording] Cleaned up old recording: ${sessionPath}`);
        }
      }
    }
  } catch (err) {
    console.error('[Recording] Cleanup error:', err.message);
  }
}

// ─── WebRTC Consumer Management ──────────────────────────────────────────────

async function createWebRtcTransport(workerIdx) {
  const router = routers[workerIdx];

  const transport = await router.createWebRtcTransport({
    listenInfos: [{
      protocol: 'udp',
      ip: config.listenIp,
      announcedAddress: config.announcedIp,
    }],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
    initialAvailableOutgoingBitrate: config.webRtcTransport.initialAvailableOutgoingBitrate,
  });

  stats.workerStats[workerIdx].transportCount++;

  return transport;
}

async function consumeStream(transport, workerIdx, streamId, rtpCapabilities) {
  const stream = streams.get(streamId);
  if (!stream) throw new Error(`Stream ${streamId} not found`);

  const router = routers[workerIdx];

  // 스트림이 다른 Worker에 있으면 pipeToRouter로 연결
  if (stream.workerIdx !== workerIdx) {
    const srcRouter = routers[stream.workerIdx];
    await srcRouter.pipeToRouter({
      producerId: stream.audioProducer.id,
      router,
    });
    await srcRouter.pipeToRouter({
      producerId: stream.videoProducer.id,
      router,
    });
  }

  const result = [];

  // Audio consumer
  if (router.canConsume({ producerId: stream.audioProducer.id, rtpCapabilities })) {
    const audioConsumer = await transport.consume({
      producerId: stream.audioProducer.id,
      rtpCapabilities,
      paused: true,
    });
    consumers.set(audioConsumer.id, audioConsumer);
    stats.workerStats[workerIdx].consumerCount++;
    stats.totalConsumers++;
    result.push({
      id: audioConsumer.id,
      producerId: stream.audioProducer.id,
      kind: 'audio',
      rtpParameters: audioConsumer.rtpParameters,
    });
  }

  // Video consumer
  if (router.canConsume({ producerId: stream.videoProducer.id, rtpCapabilities })) {
    const videoConsumer = await transport.consume({
      producerId: stream.videoProducer.id,
      rtpCapabilities,
      paused: true,
    });
    consumers.set(videoConsumer.id, videoConsumer);
    stats.workerStats[workerIdx].consumerCount++;
    stats.totalConsumers++;
    result.push({
      id: videoConsumer.id,
      producerId: stream.videoProducer.id,
      kind: 'video',
      rtpParameters: videoConsumer.rtpParameters,
    });
  }

  return result;
}

// ─── WebSocket Signaling ─────────────────────────────────────────────────────

function handleWebSocket(ws) {
  const workerIdx = getNextWorkerIndex();
  let consumerTransport = null;
  const clientConsumerIds = new Set();

  stats.totalConnections++;
  stats.activeConnections++;
  addEvent('client', '클라이언트 접속', { connections: stats.activeConnections });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
      switch (msg.action) {
        case 'getRouterRtpCapabilities': {
          // TURN/STUN 서버 정보도 함께 전달
          const iceServers = [];
          if (config.turn.enabled) {
            iceServers.push(
              { urls: `stun:${config.turn.host}:${config.turn.port}` },
              {
                urls: `turn:${config.turn.host}:${config.turn.port}`,
                username: config.turn.username,
                credential: config.turn.password,
              },
              {
                urls: `turn:${config.turn.host}:${config.turn.port}?transport=tcp`,
                username: config.turn.username,
                credential: config.turn.password,
              }
            );
          }
          ws.send(JSON.stringify({
            action: 'routerRtpCapabilities',
            rtpCapabilities: routers[workerIdx].rtpCapabilities,
            iceServers,
          }));
          break;
        }

        case 'getStreams': {
          const streamList = [];
          for (const [id, stream] of streams) {
            streamList.push({
              id,
              label: stream.config.label,
              type: stream.config.type,
              videoCodec: stream.config.videoCodec,
              resolution: stream.config.resolution,
              fps: stream.config.fps,
              gpuId: stream.gpuId,
              uptime: Math.floor((Date.now() - stream.startTime) / 1000),
            });
          }
          ws.send(JSON.stringify({ action: 'streamList', streams: streamList }));
          break;
        }

        case 'createConsumerTransport': {
          consumerTransport = await createWebRtcTransport(workerIdx);
          ws.send(JSON.stringify({
            action: 'consumerTransportCreated',
            id: consumerTransport.id,
            iceParameters: consumerTransport.iceParameters,
            iceCandidates: consumerTransport.iceCandidates,
            dtlsParameters: consumerTransport.dtlsParameters,
          }));
          break;
        }

        case 'connectConsumerTransport': {
          if (!consumerTransport) break;
          await consumerTransport.connect({ dtlsParameters: msg.dtlsParameters });
          ws.send(JSON.stringify({ action: 'consumerTransportConnected' }));
          break;
        }

        case 'consume': {
          if (!consumerTransport) break;
          const consumerList = await consumeStream(
            consumerTransport, workerIdx, msg.streamId, msg.rtpCapabilities
          );
          for (const c of consumerList) clientConsumerIds.add(c.id);
          ws.send(JSON.stringify({
            action: 'consumed',
            streamId: msg.streamId,
            consumers: consumerList,
          }));
          break;
        }

        case 'resumeConsumer': {
          const consumer = consumers.get(msg.consumerId);
          if (consumer) {
            await consumer.resume();
            ws.send(JSON.stringify({ action: 'consumerResumed', consumerId: msg.consumerId }));
          }
          break;
        }

        case 'getServerStats': {
          ws.send(JSON.stringify({ action: 'serverStats', stats: getMonitoringStats() }));
          break;
        }
      }
    } catch (error) {
      console.error('WS error:', error.message);
      ws.send(JSON.stringify({ action: 'error', message: error.message }));
    }
  });

  ws.on('close', () => {
    stats.activeConnections--;
    addEvent('client', '클라이언트 연결 해제', { connections: stats.activeConnections });
    for (const id of clientConsumerIds) {
      const consumer = consumers.get(id);
      if (consumer) {
        consumer.close();
        consumers.delete(id);
        stats.workerStats[workerIdx].consumerCount--;
      }
    }
    if (consumerTransport) {
      consumerTransport.close();
      stats.workerStats[workerIdx].transportCount--;
    }
  });
}

// ─── System Monitoring ───────────────────────────────────────────────────────

function getMonitoringStats() {
  const cpus = os.cpus();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000);

  // CPU 사용률 계산
  const cpuUsages = cpus.map((cpu) => {
    const total = Object.values(cpu.times).reduce((a, b) => a + b, 0);
    const idle = cpu.times.idle;
    return ((total - idle) / total * 100).toFixed(1);
  });
  const avgCpuUsage = (cpuUsages.reduce((a, b) => a + parseFloat(b), 0) / cpuUsages.length).toFixed(1);

  // GPU 정보 (nvidia-smi)
  let gpuInfo = [];
  try {
    const nvidiaSmi = execSync(
      'nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,encoder.stats.sessionCount --format=csv,noheader,nounits',
      { timeout: 3000 }
    ).toString().trim();

    gpuInfo = nvidiaSmi.split('\n').map((line) => {
      const [index, name, utilGpu, memUsed, memTotal, temp, power, encSessions] =
        line.split(',').map(s => s.trim());
      return {
        id: parseInt(index),
        name,
        utilizationPercent: parseFloat(utilGpu),
        memoryUsedMB: parseInt(memUsed),
        memoryTotalMB: parseInt(memTotal),
        temperatureC: parseInt(temp),
        powerW: parseFloat(power),
        encoderSessions: parseInt(encSessions) || 0,
      };
    });
  } catch {
    gpuInfo = HARDWARE.gpus.map((g) => ({
      id: g.id, name: g.name,
      utilizationPercent: 0, memoryUsedMB: 0, memoryTotalMB: 0,
      temperatureC: 0, powerW: 0, encoderSessions: 0,
      error: 'nvidia-smi not available',
    }));
  }

  // FFmpeg 프로세스 상태
  const ffmpegStats = [];
  for (const [id, stream] of streams) {
    ffmpegStats.push({
      id,
      label: stream.config.label,
      type: stream.config.type,
      videoCodec: stream.config.videoCodec,
      resolution: stream.config.resolution,
      gpuId: stream.gpuId,
      workerIdx: stream.workerIdx,
      pid: stream.ffmpeg?.pid,
      running: stream.ffmpeg && !stream.ffmpeg.killed,
      uptime: Math.floor((Date.now() - stream.startTime) / 1000),
      ffmpegStats: stream.getStats(),
    });
  }

  // Worker 상태
  const workerInfo = stats.workerStats.map((w, i) => ({
    ...w,
    alive: !!workers[i] && !workers[i].closed,
  }));

  return {
    server: {
      uptime,
      uptimeStr: formatUptime(uptime),
      platform: os.platform(),
      hostname: os.hostname(),
      nodeVersion: process.version,
    },
    cpu: {
      model: cpus[0]?.model || 'Unknown',
      cores: cpus.length,
      averageUsagePercent: parseFloat(avgCpuUsage),
    },
    memory: {
      totalGB: (totalMem / 1073741824).toFixed(1),
      usedGB: (usedMem / 1073741824).toFixed(1),
      freeGB: (freeMem / 1073741824).toFixed(1),
      usagePercent: ((usedMem / totalMem) * 100).toFixed(1),
    },
    gpu: gpuInfo,
    mediasoup: {
      workers: workerInfo,
      totalWorkers: workers.length,
      aliveWorkers: workers.filter(w => w && !w.closed).length,
    },
    streams: ffmpegStats,
    connections: {
      total: stats.totalConnections,
      active: stats.activeConnections,
      totalConsumers: stats.totalConsumers,
      activeConsumers: consumers.size,
    },
  };
}

function formatUptime(seconds) {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  return `${m}m ${s}s`;
}

// ─── HTTP Servers ────────────────────────────────────────────────────────────

function createMainServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(getViewerHtml());
    } else if (req.url === '/monitor' || req.url === '/monitor/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(getMonitorHtml());
    } else if (req.url === '/api/stats') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getMonitoringStats()));
    } else if (req.url === '/api/streams') {
      const list = [];
      for (const [id, s] of streams) {
        list.push({ id, label: s.config.label, type: s.config.type, videoCodec: s.config.videoCodec });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(list));

    // ── 녹화 API ──
    } else if (req.url === '/api/recordings') {
      // 전체 녹화 목록
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getRecordingsList()));

    } else if (req.url.startsWith('/api/recordings/')) {
      // 특정 스트림 녹화 목록
      const streamId = req.url.split('/')[3];
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getRecordingsList(streamId)));

    } else if (req.url === '/api/stream-events') {
      // 이벤트 로그 목록
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(eventLog));

    } else if (req.url.startsWith('/recordings/')) {
      // 녹화 파일 직접 서빙 (HLS .m3u8, .ts, .mp4)
      serveRecordingFile(req, res);

    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });

  const wss = new WebSocket.Server({ server });
  wssRef = wss;
  wss.on('connection', handleWebSocket);
  return server;
}

// ─── Recording Helpers ───────────────────────────────────────────────────────

function getRecordingsList(filterStreamId) {
  const baseDir = config.recording.dir;
  const result = [];

  try {
    if (!fs.existsSync(baseDir)) return result;
    const streamDirs = fs.readdirSync(baseDir);

    for (const streamId of streamDirs) {
      if (filterStreamId && streamId !== filterStreamId) continue;
      const streamPath = path.join(baseDir, streamId);
      if (!fs.statSync(streamPath).isDirectory()) continue;

      const sessions = fs.readdirSync(streamPath).sort().reverse();
      for (const session of sessions) {
        const sessionPath = path.join(streamPath, session);
        if (!fs.statSync(sessionPath).isDirectory()) continue;

        const files = fs.readdirSync(sessionPath);
        const hlsReady = files.includes('index.m3u8');
        const mp4Files = files.filter(f => f.endsWith('.mp4'));
        const stat = fs.statSync(sessionPath);

        result.push({
          streamId,
          session,
          startTime: stat.birthtimeMs || stat.ctimeMs,
          hlsUrl: hlsReady ? `/recordings/${streamId}/${session}/index.m3u8` : null,
          mp4Files: mp4Files.map(f => `/recordings/${streamId}/${session}/${f}`),
          totalFiles: files.length,
        });
      }
    }
  } catch (err) {
    console.error('[Recording] List error:', err.message);
  }

  return result;
}

function serveRecordingFile(req, res) {
  // /recordings/streamId/session/filename → 파일 서빙
  const urlPath = decodeURIComponent(req.url);
  const relPath = urlPath.replace(/^\/recordings\//, '');
  const filePath = path.join(config.recording.dir, relPath);

  // 경로 탈출 방지
  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(path.resolve(config.recording.dir))) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  if (!fs.existsSync(resolved)) {
    res.writeHead(404);
    res.end('Not Found');
    return;
  }

  const ext = path.extname(resolved).toLowerCase();
  const mimeTypes = {
    '.m3u8': 'application/vnd.apple.mpegurl',
    '.ts': 'video/mp2t',
    '.mp4': 'video/mp4',
  };
  const contentType = mimeTypes[ext] || 'application/octet-stream';

  const stat = fs.statSync(resolved);
  res.writeHead(200, {
    'Content-Type': contentType,
    'Content-Length': stat.size,
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': ext === '.m3u8' ? 'no-cache' : 'max-age=3600',
  });
  fs.createReadStream(resolved).pipe(res);
}

// ─── Monitor Dashboard HTML ──────────────────────────────────────────────────

function getMonitorHtml() {
  return `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Server Monitor - Drone/CCTV Streaming</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Segoe UI',system-ui,sans-serif; background:#0a0e17; color:#c8d6e5; font-size:14px; }
  .top-bar { background:#111827; padding:12px 24px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #1f2937; }
  .top-bar h1 { font-size:18px; color:#f0f0f0; }
  .top-bar .uptime { color:#6b7280; font-size:13px; }
  .top-bar .live { color:#10b981; font-weight:600; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); gap:16px; padding:20px; }
  .card { background:#111827; border-radius:12px; border:1px solid #1f2937; overflow:hidden; }
  .card-header { padding:12px 16px; background:#1f2937; font-weight:600; font-size:13px; color:#9ca3af; text-transform:uppercase; letter-spacing:0.05em; display:flex; justify-content:space-between; }
  .card-body { padding:16px; }
  .metric { display:flex; justify-content:space-between; align-items:center; padding:6px 0; border-bottom:1px solid #1f293744; }
  .metric:last-child { border:none; }
  .metric-label { color:#6b7280; font-size:12px; }
  .metric-value { font-weight:600; font-size:15px; }
  .bar-bg { background:#1f2937; border-radius:4px; height:8px; margin-top:4px; overflow:hidden; }
  .bar-fill { height:100%; border-radius:4px; transition:width 0.5s; }
  .bar-green { background:linear-gradient(90deg,#10b981,#34d399); }
  .bar-yellow { background:linear-gradient(90deg,#f59e0b,#fbbf24); }
  .bar-red { background:linear-gradient(90deg,#ef4444,#f87171); }
  .gpu-card { display:flex; gap:12px; padding:8px 0; border-bottom:1px solid #1f293744; }
  .gpu-card:last-child { border:none; }
  .gpu-id { background:#374151; color:#d1d5db; border-radius:6px; padding:4px 10px; font-weight:700; font-size:13px; min-width:40px; text-align:center; height:fit-content; }
  .gpu-info { flex:1; }
  .gpu-name { font-size:12px; color:#9ca3af; margin-bottom:4px; }
  .stream-row { display:flex; align-items:center; gap:8px; padding:8px 0; border-bottom:1px solid #1f293744; }
  .stream-row:last-child { border:none; }
  .stream-dot { width:8px; height:8px; border-radius:50%; }
  .stream-dot.running { background:#10b981; box-shadow:0 0 6px #10b98188; }
  .stream-dot.stopped { background:#ef4444; }
  .stream-name { flex:1; font-size:13px; }
  .stream-meta { font-size:11px; color:#6b7280; }
  .badge-sm { padding:1px 6px; border-radius:3px; font-size:10px; font-weight:700; }
  .badge-gpu { background:#7c3aed33; color:#a78bfa; }
  .badge-rtmp { background:#ef444433; color:#f87171; }
  .badge-rtsp { background:#3b82f633; color:#60a5fa; }
  .worker-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(36px,1fr)); gap:4px; }
  .worker-cell { aspect-ratio:1; border-radius:4px; display:flex; align-items:center; justify-content:center; font-size:9px; font-weight:700; cursor:pointer; transition:transform 0.1s; }
  .worker-cell:hover { transform:scale(1.3); z-index:1; }
  .worker-idle { background:#1f2937; color:#4b5563; }
  .worker-low { background:#064e3b; color:#10b981; }
  .worker-mid { background:#78350f; color:#f59e0b; }
  .worker-high { background:#7f1d1d; color:#ef4444; }
  .full-width { grid-column: 1 / -1; }
</style>
</head>
<body>
<div class="top-bar">
  <h1>Server Monitoring Dashboard</h1>
  <div>
    <span class="live" id="live-dot">LIVE</span>
    <span class="uptime" id="uptime">--</span>
  </div>
</div>
<div class="grid" id="dashboard"></div>

<script>
let statsData = null;

async function fetchStats() {
  try {
    const res = await fetch('/api/stats');
    statsData = await res.json();
    render(statsData);
  } catch(e) { console.error(e); }
}

function barColor(pct) {
  if (pct < 60) return 'bar-green';
  if (pct < 85) return 'bar-yellow';
  return 'bar-red';
}

function render(s) {
  document.getElementById('uptime').textContent = s.server.uptimeStr;

  const dashboard = document.getElementById('dashboard');
  dashboard.innerHTML = '';

  // ── CPU Card
  dashboard.innerHTML += '<div class="card"><div class="card-header">CPU<span>' + s.cpu.cores + ' cores</span></div><div class="card-body">'
    + '<div class="metric"><span class="metric-label">' + s.cpu.model.substring(0,40) + '</span></div>'
    + '<div class="metric"><span class="metric-label">Average Usage</span><span class="metric-value">' + s.cpu.averageUsagePercent + '%</span></div>'
    + '<div class="bar-bg"><div class="bar-fill ' + barColor(s.cpu.averageUsagePercent) + '" style="width:' + s.cpu.averageUsagePercent + '%"></div></div>'
    + '</div></div>';

  // ── Memory Card
  dashboard.innerHTML += '<div class="card"><div class="card-header">Memory<span>' + s.memory.totalGB + ' GB</span></div><div class="card-body">'
    + '<div class="metric"><span class="metric-label">Used</span><span class="metric-value">' + s.memory.usedGB + ' GB (' + s.memory.usagePercent + '%)</span></div>'
    + '<div class="bar-bg"><div class="bar-fill ' + barColor(parseFloat(s.memory.usagePercent)) + '" style="width:' + s.memory.usagePercent + '%"></div></div>'
    + '<div class="metric"><span class="metric-label">Free</span><span class="metric-value">' + s.memory.freeGB + ' GB</span></div>'
    + '</div></div>';

  // ── Connections Card
  dashboard.innerHTML += '<div class="card"><div class="card-header">Connections</div><div class="card-body">'
    + '<div class="metric"><span class="metric-label">Active Clients</span><span class="metric-value">' + s.connections.active + '</span></div>'
    + '<div class="metric"><span class="metric-label">Total Connections</span><span class="metric-value">' + s.connections.total + '</span></div>'
    + '<div class="metric"><span class="metric-label">Active Consumers</span><span class="metric-value">' + s.connections.activeConsumers + '</span></div>'
    + '</div></div>';

  // ── GPU Cards
  let gpuHtml = '<div class="card"><div class="card-header">GPU (NVIDIA A2 x' + s.gpu.length + ')</div><div class="card-body">';
  for (const g of s.gpu) {
    const memPct = g.memoryTotalMB > 0 ? ((g.memoryUsedMB / g.memoryTotalMB) * 100).toFixed(0) : 0;
    gpuHtml += '<div class="gpu-card"><div class="gpu-id">#' + g.id + '</div><div class="gpu-info">'
      + '<div class="gpu-name">' + g.name + '</div>'
      + '<div class="metric"><span class="metric-label">GPU Util</span><span class="metric-value">' + g.utilizationPercent + '%</span></div>'
      + '<div class="bar-bg"><div class="bar-fill ' + barColor(g.utilizationPercent) + '" style="width:' + g.utilizationPercent + '%"></div></div>'
      + '<div class="metric"><span class="metric-label">VRAM</span><span class="metric-value">' + g.memoryUsedMB + '/' + g.memoryTotalMB + ' MB</span></div>'
      + '<div class="metric"><span class="metric-label">Temp / Power</span><span class="metric-value">' + g.temperatureC + '°C / ' + g.powerW + 'W</span></div>'
      + '<div class="metric"><span class="metric-label">Encoder Sessions</span><span class="metric-value">' + g.encoderSessions + '</span></div>'
      + '</div></div>';
  }
  gpuHtml += '</div></div>';
  dashboard.innerHTML += gpuHtml;

  // ── Streams Card
  let streamHtml = '<div class="card"><div class="card-header">Streams<span>' + s.streams.length + ' active</span></div><div class="card-body">';
  for (const st of s.streams) {
    const badge = st.type === 'rtmp' ? 'badge-rtmp' : 'badge-rtsp';
    streamHtml += '<div class="stream-row">'
      + '<div class="stream-dot ' + (st.running ? 'running' : 'stopped') + '"></div>'
      + '<div class="stream-name">' + st.label + '</div>'
      + '<span class="badge-sm ' + badge + '">' + st.type.toUpperCase() + '</span> '
      + '<span class="badge-sm badge-gpu">GPU#' + (st.gpuId ?? 'CPU') + '</span>'
      + '<div class="stream-meta">' + st.resolution + ' ' + st.videoCodec + ' W#' + st.workerIdx + '</div>'
      + '</div>';
  }
  streamHtml += '</div></div>';
  dashboard.innerHTML += streamHtml;

  // ── Workers Heatmap
  let workerHtml = '<div class="card full-width"><div class="card-header">mediasoup Workers<span>' + s.mediasoup.aliveWorkers + '/' + s.mediasoup.totalWorkers + ' alive</span></div><div class="card-body"><div class="worker-grid">';
  for (const w of s.mediasoup.workers) {
    const load = w.consumerCount + w.producerCount;
    let cls = 'worker-idle';
    if (load > 20) cls = 'worker-high';
    else if (load > 5) cls = 'worker-mid';
    else if (load > 0) cls = 'worker-low';
    workerHtml += '<div class="worker-cell ' + cls + '" title="Worker ' + w.id + ': ' + load + ' streams, PID ' + w.pid + '">' + w.id + '</div>';
  }
  workerHtml += '</div></div></div>';
  dashboard.innerHTML += workerHtml;
}

fetchStats();
setInterval(fetchStats, 2000);
</script>
</body>
</html>`;
}

// ─── Viewer HTML ─────────────────────────────────────────────────────────────

function getViewerHtml() {
  return `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>119 통합영상관제 시스템</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0a0f;--panel:#111827;--panel2:#1a1a2e;--border:#1e293b;--red:#dc2626;--red-light:#ef4444;--amber:#f59e0b;--green:#10b981;--blue:#3b82f6;--purple:#7c3aed;--text:#e5e7eb;--text-dim:#6b7280;--text-muted:#4b5563}
html,body{height:100%;overflow:hidden}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);font-size:13px;display:flex;flex-direction:column}

/* ── Header ── */
.header{background:linear-gradient(180deg,#1a0000 0%,#0d0d1a 100%);border-bottom:2px solid var(--red);padding:0 16px;height:48px;display:flex;align-items:center;gap:16px;flex-shrink:0}
.header-logo{display:flex;align-items:center;gap:10px}
.logo-icon{width:32px;height:32px;background:var(--red);border-radius:6px;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:14px;color:#fff;text-shadow:0 1px 2px rgba(0,0,0,0.5)}
.header-title{font-size:16px;font-weight:700;color:#fff;letter-spacing:0.5px}
.header-sub{font-size:11px;color:var(--red-light);font-weight:600}
.header-right{margin-left:auto;display:flex;align-items:center;gap:16px}
.header-clock{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums;color:#fff;letter-spacing:1px}
.header-date{font-size:11px;color:var(--text-dim)}
.header-stat{display:flex;align-items:center;gap:6px;padding:4px 10px;background:rgba(255,255,255,0.05);border-radius:6px;font-size:11px}
.header-stat .val{font-weight:700;color:#fff}
.header-btn{background:rgba(255,255,255,0.08);border:1px solid rgba(255,255,255,0.1);color:var(--text);padding:5px 12px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;transition:all 0.2s}
.header-btn:hover{background:rgba(255,255,255,0.15)}
.header-btn.active{background:var(--red);border-color:var(--red);color:#fff}

/* ── Main Layout ── */
.main{flex:1;display:grid;grid-template-columns:220px 1fr 260px;overflow:hidden}
.main.left-collapsed{grid-template-columns:0px 1fr 260px}
.main.right-collapsed{grid-template-columns:220px 1fr 0px}
.main.both-collapsed{grid-template-columns:0px 1fr 0px}

/* ── Left Panel: Stream List ── */
.left-panel{background:var(--panel);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.panel-header{padding:10px 12px;background:rgba(255,255,255,0.03);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;color:var(--text-dim);flex-shrink:0}
.panel-header .count{background:var(--red);color:#fff;padding:1px 7px;border-radius:10px;font-size:10px}
.stream-list{flex:1;overflow-y:auto;padding:6px}
.stream-list::-webkit-scrollbar{width:4px}
.stream-list::-webkit-scrollbar-thumb{background:var(--text-muted);border-radius:2px}
.stream-item{padding:8px 10px;border-radius:8px;cursor:pointer;display:flex;align-items:center;gap:8px;transition:all 0.15s;border:1px solid transparent;margin-bottom:2px}
.stream-item:hover{background:rgba(255,255,255,0.05);border-color:var(--border)}
.stream-item.watching{background:rgba(16,185,129,0.1);border-color:var(--green)}
.stream-item .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.stream-item .dot.live{background:var(--green);box-shadow:0 0 6px var(--green)}
.stream-item .dot.offline{background:var(--red)}
.stream-item .info{flex:1;min-width:0}
.stream-item .name{font-size:12px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.stream-item .meta{font-size:10px;color:var(--text-dim);margin-top:1px}
.stream-item .badges{display:flex;gap:3px;flex-shrink:0}
.sbadge{padding:1px 5px;border-radius:3px;font-size:9px;font-weight:700}
.sbadge-rtmp{background:rgba(239,68,68,0.2);color:var(--red-light)}
.sbadge-rtsp{background:rgba(59,130,246,0.2);color:var(--blue)}
.sbadge-gpu{background:rgba(124,58,237,0.2);color:#a78bfa}

/* ── Center: Video Grid ── */
.center-panel{display:flex;flex-direction:column;overflow:hidden;background:#050508}
.grid-toolbar{padding:6px 12px;background:var(--panel);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;flex-shrink:0}
.grid-toolbar .label{font-size:11px;color:var(--text-dim);margin-right:4px}
.layout-btn{width:28px;height:28px;background:rgba(255,255,255,0.06);border:1px solid var(--border);border-radius:4px;cursor:pointer;display:flex;align-items:center;justify-content:center;color:var(--text-dim);font-size:10px;font-weight:700;transition:all 0.15s}
.layout-btn:hover{border-color:var(--text-dim)}
.layout-btn.active{background:var(--red);border-color:var(--red);color:#fff}
.grid-toolbar .sep{width:1px;height:20px;background:var(--border);margin:0 4px}
.toggle-panel-btn{background:none;border:1px solid var(--border);color:var(--text-dim);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:10px;transition:all 0.15s}
.toggle-panel-btn:hover{border-color:var(--text-dim);color:var(--text)}

.video-grid{flex:1;display:grid;gap:2px;padding:2px;overflow:hidden}
.video-grid.g1x1{grid-template-columns:1fr;grid-template-rows:1fr}
.video-grid.g2x2{grid-template-columns:1fr 1fr;grid-template-rows:1fr 1fr}
.video-grid.g3x3{grid-template-columns:1fr 1fr 1fr;grid-template-rows:1fr 1fr 1fr}
.video-grid.g4x4{grid-template-columns:1fr 1fr 1fr 1fr;grid-template-rows:1fr 1fr 1fr 1fr}
.video-grid.g1p5{grid-template-columns:2fr 1fr;grid-template-rows:1fr 1fr 1fr}
.video-grid.g1p5 .vcell:first-child{grid-row:1/4}

.vcell{background:#0a0a10;border:1px solid #1a1a2a;border-radius:4px;position:relative;overflow:hidden;display:flex;align-items:center;justify-content:center;min-height:0}
.vcell.active{border-color:var(--green)}
.vcell.focused{border-color:var(--red);border-width:2px}
.vcell.drag-over{border-color:var(--blue);border-width:2px;background:rgba(59,130,246,0.08)}
.vcell.dragging{opacity:0.4}
.vcell video{width:100%;height:100%;object-fit:contain;display:block}
.vcell .overlay{position:absolute;top:0;left:0;right:0;padding:6px 8px;background:linear-gradient(180deg,rgba(0,0,0,0.7) 0%,transparent 100%);display:flex;align-items:center;gap:6px;pointer-events:none;opacity:0;transition:opacity 0.2s}
.vcell:hover .overlay{opacity:1}
.vcell.active .overlay{opacity:1}
.vcell .overlay .live-badge{display:flex;align-items:center;gap:4px;font-size:10px;font-weight:700;color:var(--red-light)}
.vcell .overlay .live-dot{width:6px;height:6px;border-radius:50%;background:var(--red);animation:pulse 1.5s infinite}
.vcell .overlay .vname{font-size:11px;font-weight:600;color:#fff}
.vcell .overlay .vinfo{margin-left:auto;font-size:9px;color:rgba(255,255,255,0.6)}
.vcell .cell-controls{position:absolute;bottom:0;left:0;right:0;padding:6px 8px;background:linear-gradient(0deg,rgba(0,0,0,0.7) 0%,transparent 100%);display:flex;align-items:center;gap:4px;opacity:0;transition:opacity 0.2s}
.vcell:hover .cell-controls{opacity:1}
.vcell .cell-btn{background:rgba(255,255,255,0.15);border:none;color:#fff;width:24px;height:24px;border-radius:4px;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:11px;transition:background 0.15s}
.vcell .cell-btn:hover{background:rgba(255,255,255,0.3)}
.vcell .rec-indicator{position:absolute;top:8px;right:8px;display:flex;align-items:center;gap:4px;font-size:9px;font-weight:700;color:var(--red-light);opacity:0.9}
.vcell .rec-dot{width:6px;height:6px;border-radius:50%;background:var(--red);animation:pulse 1s infinite}
.vcell .empty-label{color:var(--text-muted);font-size:12px;text-align:center;pointer-events:none;user-select:none}
.vcell .empty-label .num{font-size:24px;font-weight:200;color:var(--text-muted);opacity:0.3}

@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}

/* ── Right Panel: Events ── */
.right-panel{background:var(--panel);border-left:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.event-list{flex:1;overflow-y:auto;padding:6px}
.event-list::-webkit-scrollbar{width:4px}
.event-list::-webkit-scrollbar-thumb{background:var(--text-muted);border-radius:2px}
.event-item{padding:6px 8px;border-radius:6px;margin-bottom:2px;display:flex;gap:8px;align-items:flex-start;font-size:11px;transition:background 0.15s}
.event-item:hover{background:rgba(255,255,255,0.03)}
.event-item.new{animation:eventFlash 1s ease-out}
@keyframes eventFlash{0%{background:rgba(220,38,38,0.2)}100%{background:transparent}}
.event-icon{width:18px;height:18px;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:10px;flex-shrink:0;margin-top:1px}
.event-icon.stream{background:rgba(239,68,68,0.15);color:var(--red-light)}
.event-icon.recording{background:rgba(245,158,11,0.15);color:var(--amber)}
.event-icon.client{background:rgba(16,185,129,0.15);color:var(--green)}
.event-icon.system{background:rgba(107,114,128,0.15);color:var(--text-dim)}
.event-body{flex:1;min-width:0}
.event-msg{color:var(--text);line-height:1.4}
.event-time{font-size:9px;color:var(--text-muted);margin-top:1px;font-variant-numeric:tabular-nums}

/* ── Bottom Status Bar ── */
.status-bar{background:linear-gradient(0deg,#0d0d1a 0%,var(--panel) 100%);border-top:1px solid var(--border);padding:0 16px;height:32px;display:flex;align-items:center;gap:16px;flex-shrink:0;font-size:11px}
.status-item{display:flex;align-items:center;gap:6px;color:var(--text-dim)}
.status-item .label{font-size:10px}
.status-item .value{font-weight:700;color:var(--text);font-variant-numeric:tabular-nums}
.mini-bar{width:50px;height:4px;background:rgba(255,255,255,0.1);border-radius:2px;overflow:hidden}
.mini-bar-fill{height:100%;border-radius:2px;transition:width 0.5s}
.mini-bar-fill.green{background:var(--green)}
.mini-bar-fill.yellow{background:var(--amber)}
.mini-bar-fill.red{background:var(--red)}
.status-sep{width:1px;height:16px;background:var(--border)}
.ws-status{display:flex;align-items:center;gap:4px}
.ws-dot{width:6px;height:6px;border-radius:50%;background:var(--green)}
.ws-dot.disconnected{background:var(--red)}

/* ── Responsive ── */
@media(max-width:1200px){
  .main{grid-template-columns:180px 1fr 220px}
}
@media(max-width:900px){
  .main{grid-template-columns:1fr !important}
  .left-panel,.right-panel{display:none}
}
</style>
</head>
<body>

<!-- ═══ HEADER ═══ -->
<div class="header">
  <div class="header-logo">
    <div class="logo-icon">119</div>
    <div>
      <div class="header-title">통합영상관제 시스템</div>
      <div class="header-sub">Integrated Video Control System</div>
    </div>
  </div>
  <div class="header-right">
    <div class="header-stat">
      <span class="label">스트림</span>
      <span class="val" id="h-streams">0</span>
    </div>
    <div class="header-stat">
      <span class="label">접속자</span>
      <span class="val" id="h-clients">0</span>
    </div>
    <div>
      <div class="header-clock" id="h-clock">--:--:--</div>
      <div class="header-date" id="h-date">----.--.--</div>
    </div>
    <a href="/monitor" class="header-btn">시스템 모니터</a>
    <button class="header-btn" onclick="toggleFullscreen()" title="전체화면">&#x26F6;</button>
  </div>
</div>

<!-- ═══ MAIN ═══ -->
<div class="main" id="main-layout">

  <!-- ── Left: Stream List ── -->
  <div class="left-panel" id="left-panel">
    <div class="panel-header">
      <span>영상소스</span>
      <span class="count" id="stream-count">0</span>
    </div>
    <div class="stream-list" id="stream-list"></div>
  </div>

  <!-- ── Center: Video Grid ── -->
  <div class="center-panel">
    <div class="grid-toolbar">
      <span class="label">레이아웃</span>
      <button class="layout-btn" data-layout="g1x1" title="1x1">1</button>
      <button class="layout-btn active" data-layout="g2x2" title="2x2">4</button>
      <button class="layout-btn" data-layout="g3x3" title="3x3">9</button>
      <button class="layout-btn" data-layout="g4x4" title="4x4">16</button>
      <button class="layout-btn" data-layout="g1p5" title="1+5">1+5</button>
      <div class="sep"></div>
      <button class="toggle-panel-btn" onclick="togglePanel('left')">◀ 목록</button>
      <button class="toggle-panel-btn" onclick="togglePanel('right')">이벤트 ▶</button>
      <div style="flex:1"></div>
      <button class="header-btn" onclick="watchAll()" title="전체 시청">전체 연결</button>
    </div>
    <div class="video-grid g2x2" id="video-grid"></div>
  </div>

  <!-- ── Right: Event Log ── -->
  <div class="right-panel" id="right-panel">
    <div class="panel-header">
      <span>이벤트 로그</span>
      <span class="count" id="event-count">0</span>
    </div>
    <div class="event-list" id="event-list"></div>
  </div>
</div>

<!-- ═══ STATUS BAR ═══ -->
<div class="status-bar">
  <div class="ws-status">
    <div class="ws-dot" id="ws-dot"></div>
    <span id="ws-label" style="color:var(--text-dim)">연결 중...</span>
  </div>
  <div class="status-sep"></div>
  <div class="status-item">
    <span class="label">CPU</span>
    <span class="value" id="sb-cpu">--%</span>
    <div class="mini-bar"><div class="mini-bar-fill green" id="sb-cpu-bar" style="width:0%"></div></div>
  </div>
  <div class="status-item">
    <span class="label">RAM</span>
    <span class="value" id="sb-ram">--%</span>
    <div class="mini-bar"><div class="mini-bar-fill green" id="sb-ram-bar" style="width:0%"></div></div>
  </div>
  <div class="status-item">
    <span class="label">GPU</span>
    <span class="value" id="sb-gpu">--</span>
  </div>
  <div class="status-sep"></div>
  <div class="status-item">
    <span class="label">Workers</span>
    <span class="value" id="sb-workers">--</span>
  </div>
  <div class="status-item">
    <span class="label">녹화</span>
    <span class="value" id="sb-rec">--</span>
  </div>
  <div style="flex:1"></div>
  <div class="status-item">
    <span class="label">Uptime</span>
    <span class="value" id="sb-uptime">--</span>
  </div>
</div>

<script>
// ═══ State ═══
const ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://') + location.host);
let device = null;
let consumerTransport = null;
const streamList = [];
const gridSlots = {};     // slotIdx → { streamId, videoEl, consumers }
const watchingStreams = new Set();
let currentLayout = 'g2x2';
let maxSlots = 4;
let focusedSlot = -1;
let dragSrcSlot = -1;
const events = [];

// ═══ Clock ═══
function updateClock() {
  const now = new Date();
  const pad = n => String(n).padStart(2,'0');
  document.getElementById('h-clock').textContent = pad(now.getHours())+':'+pad(now.getMinutes())+':'+pad(now.getSeconds());
  const days = ['일','월','화','수','목','금','토'];
  document.getElementById('h-date').textContent = now.getFullYear()+'.'+pad(now.getMonth()+1)+'.'+pad(now.getDate())+' ('+days[now.getDay()]+')';
}
updateClock();
setInterval(updateClock, 1000);

// ═══ Layout ═══
const layoutMap = { g1x1:1, g2x2:4, g3x3:9, g4x4:16, g1p5:6 };
document.querySelectorAll('.layout-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.layout-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    setLayout(btn.dataset.layout);
  });
});

function setLayout(layout) {
  currentLayout = layout;
  maxSlots = layoutMap[layout];
  const grid = document.getElementById('video-grid');
  grid.className = 'video-grid ' + layout;
  rebuildGrid();
}

function rebuildGrid() {
  const grid = document.getElementById('video-grid');
  const existing = { ...gridSlots };
  grid.innerHTML = '';
  // Clear slots beyond maxSlots
  for (const idx of Object.keys(gridSlots)) {
    if (parseInt(idx) >= maxSlots) {
      const slot = gridSlots[idx];
      if (slot.streamId) watchingStreams.delete(slot.streamId);
      delete gridSlots[idx];
    }
  }
  for (let i = 0; i < maxSlots; i++) {
    const cell = document.createElement('div');
    cell.className = 'vcell';
    cell.dataset.slot = i;
    cell.addEventListener('click', () => setFocus(i));
    // Drag-and-drop support
    cell.draggable = true;
    cell.addEventListener('dragstart', (e) => { dragSrcSlot = i; cell.classList.add('dragging'); e.dataTransfer.effectAllowed = 'move'; });
    cell.addEventListener('dragend', () => { cell.classList.remove('dragging'); document.querySelectorAll('.vcell').forEach(c=>c.classList.remove('drag-over')); });
    cell.addEventListener('dragover', (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; cell.classList.add('drag-over'); });
    cell.addEventListener('dragleave', () => { cell.classList.remove('drag-over'); });
    cell.addEventListener('drop', (e) => {
      e.preventDefault(); cell.classList.remove('drag-over');
      const sid = e.dataTransfer.getData('streamId');
      if (sid) { dropStreamToSlot(sid, i); } else { swapSlots(dragSrcSlot, i); }
    });
    if (gridSlots[i] && gridSlots[i].streamId) {
      const slot = gridSlots[i];
      cell.classList.add('active');
      const video = document.createElement('video');
      video.muted = true;
      video.playsInline = true;
      video.autoplay = true;
      if (slot.mediaStream) { video.srcObject = slot.mediaStream; video.play().catch(()=>{}); }
      cell.appendChild(video);
      slot.videoEl = video;
      // overlay
      const s = streamList.find(x=>x.id===slot.streamId);
      cell.innerHTML += '<div class="overlay"><span class="live-badge"><span class="live-dot"></span>LIVE</span><span class="vname">'+(s?s.label:slot.streamId)+'</span><span class="vinfo">'+(s?s.resolution+' '+s.videoCodec:'')+'</span></div>';
      cell.innerHTML += '<div class="cell-controls"><button class="cell-btn" onclick="event.stopPropagation();captureSnapshot('+i+')" title="스냅샷">&#x1F4F7;</button><button class="cell-btn" onclick="event.stopPropagation();fullscreenCell('+i+')" title="전체화면">&#x26F6;</button><button class="cell-btn" onclick="event.stopPropagation();removeFromGrid('+i+')" title="제거">&#x2715;</button></div>';
    } else {
      gridSlots[i] = { streamId: null, videoEl: null, mediaStream: null };
      cell.innerHTML = '<div class="empty-label"><div class="num">' + (i+1) + '</div>영상 소스를 선택하세요</div>';
    }
    if (i === focusedSlot) cell.classList.add('focused');
    grid.appendChild(cell);
  }
  updateStreamListUI();
}

function setFocus(idx) {
  focusedSlot = idx;
  document.querySelectorAll('.vcell').forEach((c,i) => {
    c.classList.toggle('focused', i===idx);
  });
}

// ═══ Panel Toggle ═══
function togglePanel(side) {
  const main = document.getElementById('main-layout');
  if (side === 'left') {
    document.getElementById('left-panel').style.display =
      document.getElementById('left-panel').style.display === 'none' ? '' : 'none';
  } else {
    document.getElementById('right-panel').style.display =
      document.getElementById('right-panel').style.display === 'none' ? '' : 'none';
  }
}

function toggleFullscreen() {
  if (!document.fullscreenElement) document.documentElement.requestFullscreen();
  else document.exitFullscreen();
}

// ═══ Stream List UI ═══
function updateStreamListUI() {
  const container = document.getElementById('stream-list');
  container.innerHTML = '';
  document.getElementById('stream-count').textContent = streamList.length;
  document.getElementById('h-streams').textContent = streamList.length;
  for (const s of streamList) {
    const item = document.createElement('div');
    item.className = 'stream-item' + (watchingStreams.has(s.id) ? ' watching' : '');
    const typeClass = s.type === 'rtmp' ? 'sbadge-rtmp' : 'sbadge-rtsp';
    item.innerHTML = '<div class="dot live"></div>'
      + '<div class="info"><div class="name">' + s.label + '</div><div class="meta">' + s.resolution + ' ' + s.fps + 'fps</div></div>'
      + '<div class="badges"><span class="sbadge ' + typeClass + '">' + s.type.toUpperCase() + '</span>'
      + (s.gpuId != null ? '<span class="sbadge sbadge-gpu">G'+s.gpuId+'</span>' : '')
      + '</div>';
    item.draggable = true;
    item.addEventListener('dragstart', (e) => { e.dataTransfer.setData('streamId', s.id); e.dataTransfer.effectAllowed = 'copy'; });
    item.addEventListener('click', () => addToGrid(s.id));
    container.appendChild(item);
  }
}

// ═══ Grid Management ═══
function addToGrid(streamId) {
  if (watchingStreams.has(streamId)) return;
  // Find empty slot
  let slotIdx = -1;
  for (let i = 0; i < maxSlots; i++) {
    if (!gridSlots[i] || !gridSlots[i].streamId) { slotIdx = i; break; }
  }
  if (slotIdx === -1) {
    // Replace focused or last slot
    slotIdx = focusedSlot >= 0 ? focusedSlot : maxSlots - 1;
    if (gridSlots[slotIdx]?.streamId) {
      watchingStreams.delete(gridSlots[slotIdx].streamId);
    }
  }
  gridSlots[slotIdx] = { streamId, videoEl: null, mediaStream: null };
  watchingStreams.add(streamId);
  rebuildGrid();
  watchStream(streamId, slotIdx);
}

function removeFromGrid(slotIdx) {
  const slot = gridSlots[slotIdx];
  if (slot?.streamId) watchingStreams.delete(slot.streamId);
  gridSlots[slotIdx] = { streamId: null, videoEl: null, mediaStream: null };
  rebuildGrid();
}

function dropStreamToSlot(streamId, slotIdx) {
  if (watchingStreams.has(streamId)) return;
  if (gridSlots[slotIdx]?.streamId) watchingStreams.delete(gridSlots[slotIdx].streamId);
  gridSlots[slotIdx] = { streamId, videoEl: null, mediaStream: null };
  watchingStreams.add(streamId);
  rebuildGrid();
  watchStream(streamId, slotIdx);
}

function swapSlots(fromIdx, toIdx) {
  if (fromIdx === toIdx || fromIdx < 0) return;
  const tmp = gridSlots[fromIdx];
  gridSlots[fromIdx] = gridSlots[toIdx];
  gridSlots[toIdx] = tmp;
  rebuildGrid();
  addLocalEvent('system', '셀 ' + (fromIdx+1) + ' ↔ ' + (toIdx+1) + ' 위치 교환');
}

function watchAll() {
  const available = streamList.filter(s => !watchingStreams.has(s.id));
  for (const s of available) {
    let slotIdx = -1;
    for (let i = 0; i < maxSlots; i++) {
      if (!gridSlots[i] || !gridSlots[i].streamId) { slotIdx = i; break; }
    }
    if (slotIdx === -1) break;
    gridSlots[slotIdx] = { streamId: s.id, videoEl: null, mediaStream: null };
    watchingStreams.add(s.id);
  }
  rebuildGrid();
  for (let i = 0; i < maxSlots; i++) {
    if (gridSlots[i]?.streamId && !gridSlots[i]?.mediaStream) {
      watchStream(gridSlots[i].streamId, i);
    }
  }
}

// ═══ Snapshot ═══
function captureSnapshot(slotIdx) {
  const slot = gridSlots[slotIdx];
  if (!slot?.videoEl) return;
  const canvas = document.createElement('canvas');
  canvas.width = slot.videoEl.videoWidth;
  canvas.height = slot.videoEl.videoHeight;
  canvas.getContext('2d').drawImage(slot.videoEl, 0, 0);
  const a = document.createElement('a');
  a.href = canvas.toDataURL('image/png');
  a.download = (slot.streamId||'capture') + '_' + new Date().toISOString().replace(/[:.]/g,'-') + '.png';
  a.click();
  addLocalEvent('system', '스냅샷 저장: ' + (slot.streamId||''));
}

function fullscreenCell(slotIdx) {
  const cells = document.querySelectorAll('.vcell');
  if (cells[slotIdx]) {
    cells[slotIdx].requestFullscreen().catch(()=>{});
  }
}

// ═══ Events ═══
function addLocalEvent(type, message) {
  const event = { type, message, time: new Date().toISOString() };
  events.unshift(event);
  if (events.length > 50) events.pop();
  renderEvents();
}

function renderEvents() {
  const container = document.getElementById('event-list');
  const isNew = container.children.length < events.length;
  container.innerHTML = '';
  document.getElementById('event-count').textContent = events.length;
  for (let i = 0; i < events.length; i++) {
    const e = events[i];
    const div = document.createElement('div');
    div.className = 'event-item' + (i === 0 && isNew ? ' new' : '');
    const iconMap = { stream:'S', recording:'R', client:'C', system:'I' };
    const t = new Date(e.time);
    const timeStr = String(t.getHours()).padStart(2,'0')+':'+String(t.getMinutes()).padStart(2,'0')+':'+String(t.getSeconds()).padStart(2,'0');
    div.innerHTML = '<div class="event-icon '+(e.type||'system')+'">'+(iconMap[e.type]||'I')+'</div>'
      + '<div class="event-body"><div class="event-msg">'+e.message+'</div><div class="event-time">'+timeStr+'</div></div>';
    container.appendChild(div);
  }
}

// Load initial events
fetch('/api/stream-events').then(r=>r.json()).then(list => {
  for (const e of list) events.push(e);
  renderEvents();
}).catch(()=>{});

// ═══ WebSocket ═══
ws.onopen = () => {
  document.getElementById('ws-dot').classList.remove('disconnected');
  document.getElementById('ws-label').textContent = '연결됨';
  ws.send(JSON.stringify({ action: 'getRouterRtpCapabilities' }));
  addLocalEvent('system', '서버 연결 성공');
};

ws.onclose = () => {
  document.getElementById('ws-dot').classList.add('disconnected');
  document.getElementById('ws-label').textContent = '연결 끊김';
  addLocalEvent('system', '서버 연결 끊김');
};

ws.onmessage = async (event) => {
  const msg = JSON.parse(event.data);
  switch (msg.action) {
    case 'routerRtpCapabilities': {
      const { Device } = await import('https://esm.sh/mediasoup-client@3');
      device = new Device();
      await device.load({ routerRtpCapabilities: msg.rtpCapabilities });
      ws.send(JSON.stringify({ action: 'getStreams' }));
      break;
    }
    case 'streamList': {
      streamList.length = 0;
      streamList.push(...msg.streams);
      updateStreamListUI();
      rebuildGrid();
      break;
    }
    case 'consumerTransportCreated': {
      consumerTransport = device.createRecvTransport({
        id: msg.id,
        iceParameters: msg.iceParameters,
        iceCandidates: msg.iceCandidates,
        dtlsParameters: msg.dtlsParameters,
      });
      consumerTransport.on('connect', ({ dtlsParameters }, cb) => {
        ws.send(JSON.stringify({ action: 'connectConsumerTransport', dtlsParameters }));
        const h = (e) => {
          const r = JSON.parse(e.data);
          if (r.action === 'consumerTransportConnected') { cb(); ws.removeEventListener('message', h); }
        };
        ws.addEventListener('message', h);
      });
      break;
    }
    case 'consumed': {
      const ms = new MediaStream();
      for (const ci of msg.consumers) {
        const c = await consumerTransport.consume({ id: ci.id, producerId: ci.producerId, kind: ci.kind, rtpParameters: ci.rtpParameters });
        ms.addTrack(c.track);
        ws.send(JSON.stringify({ action: 'resumeConsumer', consumerId: ci.id }));
      }
      // Find slot for this stream
      for (let i = 0; i < maxSlots; i++) {
        if (gridSlots[i]?.streamId === msg.streamId) {
          gridSlots[i].mediaStream = ms;
          rebuildGrid();
          break;
        }
      }
      addLocalEvent('stream', '영상 연결: ' + msg.streamId);
      break;
    }
    case 'streamEvent': {
      addLocalEvent(msg.event.type, msg.event.message);
      break;
    }
  }
};

async function watchStream(streamId, slotIdx) {
  if (!device) return;
  if (!consumerTransport) {
    ws.send(JSON.stringify({ action: 'createConsumerTransport' }));
    await new Promise(r => {
      const h = (e) => {
        const m = JSON.parse(e.data);
        if (m.action === 'consumerTransportCreated') {
          ws.removeEventListener('message', h);
          ws.dispatchEvent(new MessageEvent('message',{data:e.data}));
          setTimeout(r, 100);
        }
      };
      ws.addEventListener('message', h);
    });
  }
  ws.send(JSON.stringify({ action: 'consume', streamId, rtpCapabilities: device.rtpCapabilities }));
}

// ═══ Status Bar Polling ═══
async function updateStatusBar() {
  try {
    const res = await fetch('/api/stats');
    const s = await res.json();
    const cpuPct = s.cpu.averageUsagePercent;
    const ramPct = parseFloat(s.memory.usagePercent);
    document.getElementById('sb-cpu').textContent = cpuPct + '%';
    document.getElementById('sb-ram').textContent = ramPct.toFixed(0) + '%';
    setMiniBar('sb-cpu-bar', cpuPct);
    setMiniBar('sb-ram-bar', ramPct);
    const gpuText = s.gpu.map(g => g.utilizationPercent + '%/' + g.temperatureC + '\\u00B0').join(' ');
    document.getElementById('sb-gpu').textContent = gpuText || '--';
    document.getElementById('sb-workers').textContent = s.mediasoup.aliveWorkers + '/' + s.mediasoup.totalWorkers;
    document.getElementById('sb-uptime').textContent = s.server.uptimeStr;
    document.getElementById('h-clients').textContent = s.connections.active;
    const recCount = s.streams.filter(st => st.running).length;
    document.getElementById('sb-rec').textContent = recCount + ' 활성';
  } catch {}
}

function setMiniBar(id, pct) {
  const el = document.getElementById(id);
  el.style.width = Math.min(pct, 100) + '%';
  el.className = 'mini-bar-fill ' + (pct < 60 ? 'green' : pct < 85 ? 'yellow' : 'red');
}

updateStatusBar();
setInterval(updateStatusBar, 3000);

// ═══ Keyboard Shortcuts ═══
document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  const num = parseInt(e.key);
  if (num >= 1 && num <= 9 && num <= streamList.length) {
    e.preventDefault();
    addToGrid(streamList[num-1].id);
  }
  if (e.key === 'f' || e.key === 'F') {
    e.preventDefault();
    if (focusedSlot >= 0) fullscreenCell(focusedSlot);
  }
  if (e.key === 'g' || e.key === 'G') {
    e.preventDefault();
    const layouts = ['g1x1','g2x2','g3x3','g4x4','g1p5'];
    const next = layouts[(layouts.indexOf(currentLayout)+1) % layouts.length];
    document.querySelectorAll('.layout-btn').forEach(b => b.classList.toggle('active', b.dataset.layout === next));
    setLayout(next);
  }
});

// Init grid
rebuildGrid();
</script>
</body>
</html>`;
}

// ─── Stop Stream ─────────────────────────────────────────────────────────────

function stopStream(streamId) {
  const stream = streams.get(streamId);
  if (!stream) return;

  if (stream.ffmpeg && !stream.ffmpeg.killed) stream.ffmpeg.kill('SIGTERM');
  if (stream.recordingProc && !stream.recordingProc.killed) stream.recordingProc.kill('SIGTERM');
  stream.audioProducer.close();
  stream.videoProducer.close();
  stream.audioTransport.close();
  stream.videoTransport.close();

  stats.workerStats[stream.workerIdx].producerCount -= 2;
  stats.workerStats[stream.workerIdx].transportCount -= 2;

  streams.delete(streamId);
  stats.activeStreams = streams.size;
  console.log(`[${streamId}] Stream stopped`);
  addEvent('stream', `스트림 중지: ${stream.config.label}`);
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  RTMP/RTSP → WebRTC Streaming Server (Production)');
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`  CPU:  ${os.cpus()[0]?.model || 'Unknown'} × ${os.cpus().length} cores`);
  console.log(`  RAM:  ${(os.totalmem() / 1073741824).toFixed(0)} GB`);
  console.log(`  GPU:  NVIDIA A2 × ${HARDWARE.gpus.length}`);
  console.log(`  Workers: ${config.numWorkers} mediasoup workers`);
  console.log('═══════════════════════════════════════════════════════════');

  // Worker 풀 생성
  await createWorkerPool();

  // HTTP + WebSocket 서버
  const server = createMainServer();
  server.listen(config.httpPort, () => {
    console.log(`Viewer:  http://localhost:${config.httpPort}`);
    console.log(`Monitor: http://localhost:${config.httpPort}/monitor`);
  });

  // 스트림 수집 시작
  for (const streamConfig of config.streams) {
    try {
      await ingestStream(streamConfig);
      console.log(`[${streamConfig.id}] Ingestion started`);
      addEvent('stream', `스트림 시작: ${streamConfig.label} (${streamConfig.type.toUpperCase()})`);
    } catch (error) {
      console.error(`[${streamConfig.id}] Failed:`, error.message);
      addEvent('stream', `스트림 실패: ${streamConfig.label} - ${error.message}`);
    }
  }

  console.log('───────────────────────────────────────────────────────────');
  console.log(`Active streams: ${streams.size}`);
  console.log('Ready to serve WebRTC clients');

  // 녹화 디렉토리 생성
  if (config.recording.enabled) {
    fs.mkdirSync(config.recording.dir, { recursive: true });
    console.log(`Recording enabled: ${config.recording.dir}`);
    // 1시간마다 오래된 녹화 정리
    setInterval(cleanupOldRecordings, 3600000);
  }

  // TURN 서버 정보 출력
  if (config.turn.enabled) {
    console.log(`TURN/STUN: ${config.turn.host}:${config.turn.port}`);
  }

  // 주기적 상태 로그 (60초마다)
  setInterval(() => {
    const s = getMonitoringStats();
    console.log(
      `[MONITOR] CPU:${s.cpu.averageUsagePercent}% | RAM:${s.memory.usedGB}/${s.memory.totalGB}GB | ` +
      `Clients:${s.connections.active} | Consumers:${s.connections.activeConsumers} | ` +
      `Streams:${s.streams.length} | Workers:${s.mediasoup.aliveWorkers}/${s.mediasoup.totalWorkers}`
    );
  }, 60000);

  // Graceful shutdown
  process.on('SIGINT', () => {
    console.log('\nGraceful shutdown...');
    for (const [id] of streams) stopStream(id);
    for (const w of workers) { try { w.close(); } catch {} }
    process.exit(0);
  });
}

main().catch(console.error);
