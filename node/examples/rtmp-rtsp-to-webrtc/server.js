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
  announcedIp: null,       // 운영 시 공인 IP 설정
  httpPort: 3000,
  monitorPort: 3001,       // 모니터링 대시보드 포트

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

  const streamState = {
    config: streamConfig,
    workerIdx,
    gpuId,
    audioTransport,
    videoTransport,
    audioProducer,
    videoProducer,
    ffmpeg: ffmpegProc,
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

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
      switch (msg.action) {
        case 'getRouterRtpCapabilities': {
          ws.send(JSON.stringify({
            action: 'routerRtpCapabilities',
            rtpCapabilities: routers[workerIdx].rtpCapabilities,
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
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });

  const wss = new WebSocket.Server({ server });
  wss.on('connection', handleWebSocket);
  return server;
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
<title>Drone / CCTV WebRTC Viewer</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Segoe UI',system-ui,sans-serif; background:#0a0e17; color:#eee; }
  .top-bar { background:#111827; padding:10px 24px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #1f2937; }
  .top-bar h1 { font-size:16px; color:#f0f0f0; }
  .top-bar a { color:#60a5fa; font-size:13px; text-decoration:none; }
  .streams { display:grid; grid-template-columns:repeat(auto-fit,minmax(480px,1fr)); gap:12px; padding:16px; }
  .stream-card { background:#111827; border-radius:10px; overflow:hidden; border:2px solid #1f2937; transition:border-color 0.3s; }
  .stream-card.active { border-color:#10b981; }
  .stream-card header { padding:10px 14px; background:#1f2937; display:flex; justify-content:space-between; align-items:center; }
  .stream-card header h3 { font-size:13px; font-weight:600; }
  .badge { padding:2px 7px; border-radius:4px; font-size:10px; font-weight:700; }
  .badge.rtmp { background:#ef4444; color:#fff; }
  .badge.rtsp { background:#3b82f6; color:#fff; }
  .badge.codec { background:#10b981; color:#000; }
  .badge.gpu { background:#7c3aed; color:#fff; }
  video { width:100%; aspect-ratio:16/9; background:#000; display:block; }
  .controls { padding:8px 14px; display:flex; gap:8px; align-items:center; }
  button { padding:5px 14px; border:none; border-radius:6px; cursor:pointer; font-size:12px; font-weight:600; }
  .btn-watch { background:#10b981; color:#000; }
  .btn-watch:hover { background:#059669; }
  .status { font-size:11px; color:#6b7280; margin-left:auto; }
  #loading { text-align:center; padding:60px; color:#6b7280; }
</style>
</head>
<body>
<div class="top-bar">
  <h1>Drone / CCTV - WebRTC Live Viewer</h1>
  <a href="/monitor">System Monitor</a>
</div>
<div id="loading">Connecting...</div>
<div class="streams" id="streams"></div>

<script>
const ws = new WebSocket('ws://' + location.host);
let device = null;
let consumerTransport = null;
const videoElements = {};

ws.onopen = () => {
  ws.send(JSON.stringify({ action: 'getRouterRtpCapabilities' }));
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
      document.getElementById('loading').style.display = 'none';
      renderStreams(msg.streams);
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
      const v = videoElements[msg.streamId];
      if (v) { v.srcObject = ms; v.play(); v.closest('.stream-card').classList.add('active');
        document.getElementById('st-' + msg.streamId).textContent = 'LIVE'; }
      break;
    }
  }
};

function renderStreams(list) {
  const c = document.getElementById('streams');
  c.innerHTML = '';
  for (const s of list) {
    const d = document.createElement('div');
    d.className = 'stream-card';
    d.innerHTML = '<header><h3>' + s.label + '</h3><div>'
      + '<span class="badge ' + s.type + '">' + s.type.toUpperCase() + '</span> '
      + '<span class="badge codec">' + s.videoCodec + '</span> '
      + (s.gpuId !== null ? '<span class="badge gpu">GPU#' + s.gpuId + '</span>' : '')
      + '</div></header>'
      + '<video id="v-' + s.id + '" muted playsinline></video>'
      + '<div class="controls"><button class="btn-watch" onclick="watch(\\'' + s.id + '\\')">Watch</button>'
      + '<span class="status">' + s.resolution + ' ' + s.fps + 'fps</span>'
      + '<span class="status" id="st-' + s.id + '">Ready</span></div>';
    c.appendChild(d);
    videoElements[s.id] = d.querySelector('video');
  }
}

window.watch = async function(sid) {
  if (!device) return;
  document.getElementById('st-' + sid).textContent = 'Connecting...';
  if (!consumerTransport) {
    ws.send(JSON.stringify({ action: 'createConsumerTransport' }));
    await new Promise(r => {
      const h = (e) => { const m = JSON.parse(e.data);
        if (m.action === 'consumerTransportCreated') { ws.removeEventListener('message', h); ws.dispatchEvent(new MessageEvent('message',{data:e.data})); setTimeout(r,100); }
      }; ws.addEventListener('message', h);
    });
  }
  ws.send(JSON.stringify({ action: 'consume', streamId: sid, rtpCapabilities: device.rtpCapabilities }));
};
</script>
</body>
</html>`;
}

// ─── Stop Stream ─────────────────────────────────────────────────────────────

function stopStream(streamId) {
  const stream = streams.get(streamId);
  if (!stream) return;

  if (stream.ffmpeg && !stream.ffmpeg.killed) stream.ffmpeg.kill('SIGTERM');
  stream.audioProducer.close();
  stream.videoProducer.close();
  stream.audioTransport.close();
  stream.videoTransport.close();

  stats.workerStats[stream.workerIdx].producerCount -= 2;
  stats.workerStats[stream.workerIdx].transportCount -= 2;

  streams.delete(streamId);
  stats.activeStreams = streams.size;
  console.log(`[${streamId}] Stream stopped`);
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
    } catch (error) {
      console.error(`[${streamConfig.id}] Failed:`, error.message);
    }
  }

  console.log('───────────────────────────────────────────────────────────');
  console.log(`Active streams: ${streams.size}`);
  console.log('Ready to serve WebRTC clients');

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
