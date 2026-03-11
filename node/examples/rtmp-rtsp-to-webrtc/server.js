/**
 * RTMP/RTSP to WebRTC Streaming Server using mediasoup
 *
 * Architecture:
 *   Drone (RTMP) ──→ FFmpeg ──→ RTP ──→ PlainTransport ──→ mediasoup Router ──→ WebRTC Clients
 *   CCTV  (RTSP) ──→ FFmpeg ──→ RTP ──→ PlainTransport ──→ mediasoup Router ──→ WebRTC Clients
 *
 * This server:
 * 1. Creates a mediasoup worker and router with AV1/VP8/H264 + Opus codecs
 * 2. Spawns FFmpeg processes to ingest RTMP/RTSP streams and output RTP
 * 3. Creates PlainTransport to receive RTP from FFmpeg
 * 4. Serves WebRTC consumers via WebSocket signaling
 */

const mediasoup = require('mediasoup');
const http = require('http');
const WebSocket = require('ws');
const { spawn } = require('child_process');
const url = require('url');

// ─── Configuration ───────────────────────────────────────────────────────────

const config = {
  // mediasoup listen IP
  listenIp: '127.0.0.1',
  // Announced IP (use public IP for production)
  announcedIp: null,
  // HTTP/WebSocket server port
  httpPort: 3000,
  // mediasoup worker settings
  worker: {
    logLevel: 'warn',
    rtcMinPort: 40000,
    rtcMaxPort: 49999,
  },
  // Media codecs supported
  mediaCodecs: [
    {
      kind: 'audio',
      mimeType: 'audio/opus',
      clockRate: 48000,
      channels: 2,
    },
    {
      kind: 'video',
      mimeType: 'video/VP8',
      clockRate: 90000,
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
      mimeType: 'video/AV1',
      clockRate: 90000,
    },
  ],
  // Stream sources - configure your drone/CCTV sources here
  streams: [
    {
      id: 'drone-1',
      label: 'Drone Camera 1',
      type: 'rtmp',
      url: 'rtmp://localhost:1935/live/drone1',
      videoCodec: 'VP8',    // Output codec: VP8, H264, or AV1
    },
    {
      id: 'cctv-1',
      label: 'CCTV Camera 1',
      type: 'rtsp',
      url: 'rtsp://admin:password@192.168.1.100:554/stream1',
      videoCodec: 'H264',
    },
    {
      id: 'cctv-2',
      label: 'CCTV Camera 2',
      type: 'rtsp',
      url: 'rtsp://admin:password@192.168.1.101:554/stream1',
      videoCodec: 'VP8',
    },
  ],
};

// ─── Global State ────────────────────────────────────────────────────────────

let worker;
let router;
const streams = new Map();      // streamId -> { producers, ffmpeg, plainTransport }
const consumers = new Map();    // consumerId -> consumer

// ─── mediasoup Setup ─────────────────────────────────────────────────────────

async function createWorkerAndRouter() {
  worker = await mediasoup.createWorker({
    logLevel: config.worker.logLevel,
    rtcMinPort: config.worker.rtcMinPort,
    rtcMaxPort: config.worker.rtcMaxPort,
  });

  worker.on('died', () => {
    console.error('mediasoup worker died, exiting...');
    process.exit(1);
  });

  router = await worker.createRouter({ mediaCodecs: config.mediaCodecs });
  console.log('mediasoup router created');

  return { worker, router };
}

// ─── FFmpeg Stream Ingestion ─────────────────────────────────────────────────

/**
 * Ingest an RTMP or RTSP stream using FFmpeg, outputting RTP to a
 * mediasoup PlainTransport.
 */
async function ingestStream(streamConfig) {
  const { id, url: streamUrl, type, videoCodec, label } = streamConfig;

  console.log(`[${id}] Ingesting ${type.toUpperCase()} stream: ${label}`);

  // Create PlainTransport for audio RTP
  const audioTransport = await router.createPlainTransport({
    listenInfo: {
      protocol: 'udp',
      ip: config.listenIp,
    },
    rtcpMux: false,
    comedia: true,  // Auto-detect source from first RTP packet
  });

  // Create PlainTransport for video RTP
  const videoTransport = await router.createPlainTransport({
    listenInfo: {
      protocol: 'udp',
      ip: config.listenIp,
    },
    rtcpMux: false,
    comedia: true,
  });

  const audioRtpPort = audioTransport.tuple.localPort;
  const audioRtcpPort = audioTransport.rtcpTuple.localPort;
  const videoRtpPort = videoTransport.tuple.localPort;
  const videoRtcpPort = videoTransport.rtcpTuple.localPort;

  console.log(`[${id}] Audio RTP port: ${audioRtpPort}, RTCP: ${audioRtcpPort}`);
  console.log(`[${id}] Video RTP port: ${videoRtpPort}, RTCP: ${videoRtcpPort}`);

  // Determine FFmpeg video encoder and RTP payload type
  let ffmpegVideoCodec, payloadType, mimeType;
  switch (videoCodec) {
    case 'H264':
      ffmpegVideoCodec = 'libx264';
      payloadType = 101;
      mimeType = 'video/H264';
      break;
    case 'AV1':
      ffmpegVideoCodec = 'libsvtav1';
      payloadType = 102;
      mimeType = 'video/AV1';
      break;
    case 'VP8':
    default:
      ffmpegVideoCodec = 'libvpx';
      payloadType = 100;
      mimeType = 'video/VP8';
      break;
  }

  // Audio: Opus, PT 97, SSRC 11111111
  // Video: selected codec, dynamic PT, SSRC 22222222
  const audioSsrc = 11111111 + streams.size * 2;
  const videoSsrc = 22222222 + streams.size * 2;

  // Build FFmpeg command
  const ffmpegArgs = buildFfmpegArgs({
    streamUrl,
    type,
    ffmpegVideoCodec,
    audioRtpPort,
    audioRtcpPort,
    videoRtpPort,
    videoRtcpPort,
    audioSsrc,
    videoSsrc,
    payloadType,
    listenIp: config.listenIp,
  });

  console.log(`[${id}] FFmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

  // Create producers on PlainTransport
  const audioProducer = await audioTransport.produce({
    kind: 'audio',
    rtpParameters: {
      codecs: [
        {
          mimeType: 'audio/opus',
          payloadType: 97,
          clockRate: 48000,
          channels: 2,
          parameters: {
            minptime: 10,
            useinbandfec: 1,
          },
        },
      ],
      encodings: [{ ssrc: audioSsrc }],
    },
  });

  const videoRtpParameters = {
    codecs: [
      {
        mimeType,
        payloadType,
        clockRate: 90000,
        parameters: videoCodec === 'H264'
          ? { 'packetization-mode': 1, 'profile-level-id': '42e01f' }
          : {},
      },
    ],
    encodings: [{ ssrc: videoSsrc }],
  };

  const videoProducer = await videoTransport.produce({
    kind: 'video',
    rtpParameters: videoRtpParameters,
  });

  console.log(`[${id}] Audio producer: ${audioProducer.id}`);
  console.log(`[${id}] Video producer: ${videoProducer.id}`);

  // Spawn FFmpeg process
  const ffmpeg = spawn('ffmpeg', ffmpegArgs);

  ffmpeg.stderr.on('data', (data) => {
    // FFmpeg outputs progress to stderr
    const msg = data.toString();
    if (msg.includes('Error') || msg.includes('error')) {
      console.error(`[${id}] FFmpeg error: ${msg.trim()}`);
    }
  });

  ffmpeg.on('close', (code) => {
    console.log(`[${id}] FFmpeg exited with code ${code}`);
    // Auto-restart after 5 seconds if unexpected exit
    if (code !== 0 && streams.has(id)) {
      console.log(`[${id}] Restarting FFmpeg in 5 seconds...`);
      setTimeout(() => {
        if (streams.has(id)) {
          ingestStream(streamConfig);
        }
      }, 5000);
    }
  });

  ffmpeg.on('error', (err) => {
    console.error(`[${id}] FFmpeg spawn error:`, err.message);
  });

  // Store stream state
  streams.set(id, {
    config: streamConfig,
    audioTransport,
    videoTransport,
    audioProducer,
    videoProducer,
    ffmpeg,
  });

  return { audioProducer, videoProducer };
}

function buildFfmpegArgs({
  streamUrl, type, ffmpegVideoCodec,
  audioRtpPort, audioRtcpPort, videoRtpPort, videoRtcpPort,
  audioSsrc, videoSsrc, payloadType, listenIp,
}) {
  const inputArgs = type === 'rtsp'
    ? ['-rtsp_transport', 'tcp', '-i', streamUrl]
    : ['-listen', '1', '-i', streamUrl];

  // SDP-based RTP output for audio and video on separate ports
  const audioSdp = [
    'v=0',
    'o=- 0 0 IN IP4 127.0.0.1',
    's=FFmpeg',
    't=0 0',
    `c=IN IP4 ${listenIp}`,
    `m=audio ${audioRtpPort} RTP/AVP 97`,
    'a=rtpmap:97 opus/48000/2',
    `a=ssrc:${audioSsrc}`,
  ].join('\\n');

  const videoSdp = [
    'v=0',
    'o=- 0 0 IN IP4 127.0.0.1',
    's=FFmpeg',
    't=0 0',
    `c=IN IP4 ${listenIp}`,
    `m=video ${videoRtpPort} RTP/AVP ${payloadType}`,
    `a=rtpmap:${payloadType} ${ffmpegVideoCodec === 'libvpx' ? 'VP8' : ffmpegVideoCodec === 'libx264' ? 'H264' : 'AV1'}/90000`,
    `a=ssrc:${videoSsrc}`,
  ].join('\\n');

  return [
    // Global options
    '-re',                        // Read at native framerate
    '-fflags', '+genpts',         // Generate presentation timestamps
    // Input
    ...inputArgs,
    // Audio output → Opus RTP
    '-map', '0:a:0',
    '-c:a', 'libopus',
    '-b:a', '128k',
    '-ac', '2',
    '-ar', '48000',
    '-ssrc', String(audioSsrc),
    '-payload_type', '97',
    '-f', 'rtp',
    `rtp://${listenIp}:${audioRtpPort}?rtcpport=${audioRtcpPort}`,
    // Video output → selected codec RTP
    '-map', '0:v:0',
    '-c:v', ffmpegVideoCodec,
    '-b:v', '2M',
    '-maxrate', '2.5M',
    '-bufsize', '5M',
    '-g', '60',                   // Keyframe interval
    '-r', '30',                   // 30 FPS
    '-s', '1280x720',             // 720p output
    '-ssrc', String(videoSsrc),
    '-payload_type', String(payloadType),
    '-f', 'rtp',
    `rtp://${listenIp}:${videoRtpPort}?rtcpport=${videoRtcpPort}`,
  ];
}

// ─── WebRTC Consumer Management ──────────────────────────────────────────────

async function createWebRtcTransport() {
  const transport = await router.createWebRtcTransport({
    listenInfos: [
      {
        protocol: 'udp',
        ip: config.listenIp,
        announcedAddress: config.announcedIp,
      },
    ],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
  });

  return transport;
}

async function consumeStream(transport, streamId, rtpCapabilities) {
  const stream = streams.get(streamId);
  if (!stream) {
    throw new Error(`Stream ${streamId} not found`);
  }

  const result = [];

  // Consume audio
  if (router.canConsume({
    producerId: stream.audioProducer.id,
    rtpCapabilities,
  })) {
    const audioConsumer = await transport.consume({
      producerId: stream.audioProducer.id,
      rtpCapabilities,
      paused: true,
    });
    consumers.set(audioConsumer.id, audioConsumer);
    result.push({
      id: audioConsumer.id,
      producerId: stream.audioProducer.id,
      kind: 'audio',
      rtpParameters: audioConsumer.rtpParameters,
    });
  }

  // Consume video
  if (router.canConsume({
    producerId: stream.videoProducer.id,
    rtpCapabilities,
  })) {
    const videoConsumer = await transport.consume({
      producerId: stream.videoProducer.id,
      rtpCapabilities,
      paused: true,
    });
    consumers.set(videoConsumer.id, videoConsumer);
    result.push({
      id: videoConsumer.id,
      producerId: stream.videoProducer.id,
      kind: 'video',
      rtpParameters: videoConsumer.rtpParameters,
    });
  }

  return result;
}

// ─── WebSocket Signaling Server ──────────────────────────────────────────────

function handleWebSocket(ws) {
  let consumerTransport = null;
  const clientConsumers = new Map();

  ws.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    try {
      switch (msg.action) {
        // Client requests router RTP capabilities
        case 'getRouterRtpCapabilities': {
          ws.send(JSON.stringify({
            action: 'routerRtpCapabilities',
            rtpCapabilities: router.rtpCapabilities,
          }));
          break;
        }

        // Client requests available streams list
        case 'getStreams': {
          const streamList = [];
          for (const [id, stream] of streams) {
            streamList.push({
              id,
              label: stream.config.label,
              type: stream.config.type,
              videoCodec: stream.config.videoCodec,
            });
          }
          ws.send(JSON.stringify({
            action: 'streamList',
            streams: streamList,
          }));
          break;
        }

        // Client requests a consumer transport
        case 'createConsumerTransport': {
          consumerTransport = await createWebRtcTransport();
          ws.send(JSON.stringify({
            action: 'consumerTransportCreated',
            id: consumerTransport.id,
            iceParameters: consumerTransport.iceParameters,
            iceCandidates: consumerTransport.iceCandidates,
            dtlsParameters: consumerTransport.dtlsParameters,
          }));
          break;
        }

        // Client connects consumer transport
        case 'connectConsumerTransport': {
          if (!consumerTransport) break;
          await consumerTransport.connect({
            dtlsParameters: msg.dtlsParameters,
          });
          ws.send(JSON.stringify({ action: 'consumerTransportConnected' }));
          break;
        }

        // Client wants to consume a stream
        case 'consume': {
          if (!consumerTransport) break;
          const consumers = await consumeStream(
            consumerTransport,
            msg.streamId,
            msg.rtpCapabilities
          );
          for (const c of consumers) {
            clientConsumers.set(c.id, c);
          }
          ws.send(JSON.stringify({
            action: 'consumed',
            streamId: msg.streamId,
            consumers,
          }));
          break;
        }

        // Client resumes a consumer
        case 'resumeConsumer': {
          const consumer = consumers.get(msg.consumerId);
          if (consumer) {
            await consumer.resume();
            ws.send(JSON.stringify({
              action: 'consumerResumed',
              consumerId: msg.consumerId,
            }));
          }
          break;
        }
      }
    } catch (error) {
      console.error('WebSocket handler error:', error.message);
      ws.send(JSON.stringify({ action: 'error', message: error.message }));
    }
  });

  ws.on('close', () => {
    // Cleanup consumer transport and consumers
    for (const [id] of clientConsumers) {
      const consumer = consumers.get(id);
      if (consumer) {
        consumer.close();
        consumers.delete(id);
      }
    }
    if (consumerTransport) {
      consumerTransport.close();
    }
  });
}

// ─── HTTP Server ─────────────────────────────────────────────────────────────

function createHttpServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(getClientHtml());
    } else if (req.url === '/api/streams') {
      const streamList = [];
      for (const [id, stream] of streams) {
        streamList.push({
          id,
          label: stream.config.label,
          type: stream.config.type,
          videoCodec: stream.config.videoCodec,
        });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(streamList));
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });

  const wss = new WebSocket.Server({ server });
  wss.on('connection', handleWebSocket);

  return server;
}

// ─── Client HTML ─────────────────────────────────────────────────────────────

function getClientHtml() {
  return `<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Drone / CCTV WebRTC Viewer</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
    h1 { text-align: center; margin-bottom: 20px; color: #0f3460; background: #e0e0e0; padding: 12px; border-radius: 8px; }
    .streams { display: grid; grid-template-columns: repeat(auto-fit, minmax(480px, 1fr)); gap: 16px; }
    .stream-card {
      background: #16213e; border-radius: 12px; overflow: hidden;
      border: 2px solid #0f3460; transition: border-color 0.3s;
    }
    .stream-card.active { border-color: #4ecca3; }
    .stream-card header { padding: 12px 16px; background: #0f3460; display: flex; justify-content: space-between; align-items: center; }
    .stream-card header h3 { font-size: 14px; }
    .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
    .badge.rtmp { background: #e74c3c; }
    .badge.rtsp { background: #3498db; }
    .badge.codec { background: #2ecc71; color: #000; }
    video { width: 100%; aspect-ratio: 16/9; background: #000; display: block; }
    .controls { padding: 10px 16px; display: flex; gap: 8px; align-items: center; }
    button {
      padding: 6px 16px; border: none; border-radius: 6px; cursor: pointer;
      font-size: 13px; font-weight: 600; transition: background 0.2s;
    }
    .btn-watch { background: #4ecca3; color: #000; }
    .btn-watch:hover { background: #3ba88a; }
    .btn-stop { background: #e74c3c; color: #fff; }
    .btn-stop:hover { background: #c0392b; }
    .status { font-size: 12px; color: #888; margin-left: auto; }
    #stream-list { text-align: center; padding: 40px; color: #888; }
  </style>
</head>
<body>
  <h1>Drone / CCTV - WebRTC Live Viewer</h1>
  <div id="stream-list">Connecting to server...</div>
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
          // Load mediasoup-client Device
          const { Device } = await import('https://esm.sh/mediasoup-client@3');
          device = new Device();
          await device.load({ routerRtpCapabilities: msg.rtpCapabilities });
          // Request stream list
          ws.send(JSON.stringify({ action: 'getStreams' }));
          break;
        }

        case 'streamList': {
          document.getElementById('stream-list').style.display = 'none';
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
          consumerTransport.on('connect', ({ dtlsParameters }, callback) => {
            ws.send(JSON.stringify({
              action: 'connectConsumerTransport',
              dtlsParameters,
            }));
            // Wait for confirmation
            const handler = (event) => {
              const resp = JSON.parse(event.data);
              if (resp.action === 'consumerTransportConnected') {
                callback();
                ws.removeEventListener('message', handler);
              }
            };
            ws.addEventListener('message', handler);
          });
          break;
        }

        case 'consumed': {
          const streamId = msg.streamId;
          const mediaStream = new MediaStream();

          for (const consumerInfo of msg.consumers) {
            const consumer = await consumerTransport.consume({
              id: consumerInfo.id,
              producerId: consumerInfo.producerId,
              kind: consumerInfo.kind,
              rtpParameters: consumerInfo.rtpParameters,
            });
            mediaStream.addTrack(consumer.track);

            // Resume consumer
            ws.send(JSON.stringify({
              action: 'resumeConsumer',
              consumerId: consumerInfo.id,
            }));
          }

          const video = videoElements[streamId];
          if (video) {
            video.srcObject = mediaStream;
            video.play();
            video.closest('.stream-card').classList.add('active');
            document.getElementById('status-' + streamId).textContent = 'Live';
          }
          break;
        }
      }
    };

    function renderStreams(streamList) {
      const container = document.getElementById('streams');
      container.innerHTML = '';

      for (const stream of streamList) {
        const card = document.createElement('div');
        card.className = 'stream-card';
        card.innerHTML =
          '<header>' +
            '<h3>' + stream.label + '</h3>' +
            '<div>' +
              '<span class="badge ' + stream.type + '">' + stream.type.toUpperCase() + '</span> ' +
              '<span class="badge codec">' + stream.videoCodec + '</span>' +
            '</div>' +
          '</header>' +
          '<video id="video-' + stream.id + '" muted playsinline></video>' +
          '<div class="controls">' +
            '<button class="btn-watch" onclick="watchStream(\\'' + stream.id + '\\')">Watch</button>' +
            '<span class="status" id="status-' + stream.id + '">Ready</span>' +
          '</div>';
        container.appendChild(card);
        videoElements[stream.id] = card.querySelector('video');
      }
    }

    window.watchStream = async function(streamId) {
      if (!device) return;

      document.getElementById('status-' + streamId).textContent = 'Connecting...';

      // Create consumer transport if not exists
      if (!consumerTransport) {
        ws.send(JSON.stringify({ action: 'createConsumerTransport' }));
        // Wait for transport creation
        await new Promise((resolve) => {
          const handler = (event) => {
            const resp = JSON.parse(event.data);
            if (resp.action === 'consumerTransportCreated') {
              ws.removeEventListener('message', handler);
              // Trigger the main handler
              ws.dispatchEvent(new MessageEvent('message', { data: event.data }));
              setTimeout(resolve, 100);
            }
          };
          ws.addEventListener('message', handler);
        });
      }

      // Request to consume stream
      ws.send(JSON.stringify({
        action: 'consume',
        streamId,
        rtpCapabilities: device.rtpCapabilities,
      }));
    };
  </script>
</body>
</html>`;
}

// ─── Stop Stream ─────────────────────────────────────────────────────────────

function stopStream(streamId) {
  const stream = streams.get(streamId);
  if (!stream) return;

  // Kill FFmpeg process
  if (stream.ffmpeg && !stream.ffmpeg.killed) {
    stream.ffmpeg.kill('SIGTERM');
  }

  // Close producers and transports
  stream.audioProducer.close();
  stream.videoProducer.close();
  stream.audioTransport.close();
  stream.videoTransport.close();

  streams.delete(streamId);
  console.log(`[${streamId}] Stream stopped`);
}

// ─── Main Entry ──────────────────────────────────────────────────────────────

async function main() {
  console.log('===========================================');
  console.log('  RTMP/RTSP → WebRTC Streaming Server');
  console.log('  Powered by mediasoup + FFmpeg');
  console.log('===========================================');

  // Create mediasoup worker and router
  await createWorkerAndRouter();

  // Start HTTP/WebSocket server
  const server = createHttpServer();
  server.listen(config.httpPort, () => {
    console.log(`HTTP server listening on port ${config.httpPort}`);
    console.log(`Open http://localhost:${config.httpPort} in your browser`);
  });

  // Ingest configured streams
  for (const streamConfig of config.streams) {
    try {
      await ingestStream(streamConfig);
      console.log(`[${streamConfig.id}] Stream ingestion started`);
    } catch (error) {
      console.error(`[${streamConfig.id}] Failed to start ingestion:`, error.message);
    }
  }

  console.log('-------------------------------------------');
  console.log(`Total streams: ${streams.size}`);
  console.log('Ready to serve WebRTC clients');

  // Graceful shutdown
  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    for (const [id] of streams) {
      stopStream(id);
    }
    worker.close();
    process.exit(0);
  });
}

main().catch(console.error);
