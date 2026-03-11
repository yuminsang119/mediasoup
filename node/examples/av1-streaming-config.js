/**
 * mediasoup AV1 Codec Streaming Configuration Example
 *
 * This file demonstrates how to configure mediasoup for AV1 video codec streaming
 * with SVC (Scalable Video Coding) support using the Dependency Descriptor
 * RTP header extension.
 *
 * AV1 advantages over VP8/VP9/H264:
 * - ~30% better compression efficiency than VP9
 * - Native SVC support via Dependency Descriptor
 * - Royalty-free codec
 * - Adaptive spatial/temporal layer selection per consumer
 *
 * Usage:
 *   This is a configuration reference. Import the mediaCodecs into your
 *   mediasoup router creation.
 */

const mediasoup = require('mediasoup');

// AV1 media codec configuration for Router creation
const mediaCodecs = [
  // Audio: Opus codec (standard)
  {
    kind: 'audio',
    mimeType: 'audio/opus',
    clockRate: 48000,
    channels: 2,
    parameters: {
      useinbandfec: 1
    },
    rtcpFeedback: [
      { type: 'transport-cc' }
    ]
  },
  // Video: AV1 codec with full RTCP feedback support
  {
    kind: 'video',
    mimeType: 'video/AV1',
    clockRate: 90000,
    parameters: {},
    rtcpFeedback: [
      { type: 'nack' },
      { type: 'nack', parameter: 'pli' },
      { type: 'ccm', parameter: 'fir' },
      { type: 'goog-remb' },
      { type: 'transport-cc' }
    ]
  }
];

/**
 * AV1 SVC Scalability Modes Reference:
 *
 * L1T1 - 1 spatial, 1 temporal (no SVC, baseline)
 * L1T2 - 1 spatial, 2 temporal layers
 * L1T3 - 1 spatial, 3 temporal layers
 * L2T1 - 2 spatial, 1 temporal layers
 * L2T2 - 2 spatial, 2 temporal layers
 * L2T3 - 2 spatial, 3 temporal layers
 * L3T1 - 3 spatial, 1 temporal layers
 * L3T2 - 3 spatial, 2 temporal layers
 * L3T3 - 3 spatial, 3 temporal layers (maximum quality/flexibility)
 *
 * Client-side producer encoding example for AV1 SVC:
 *
 *   const producer = await sendTransport.produce({
 *     track: videoTrack,
 *     encodings: [
 *       { scalabilityMode: 'L3T3', maxBitrate: 5000000 }
 *     ],
 *     codec: device.rtpCapabilities.codecs.find(
 *       c => c.mimeType.toLowerCase() === 'video/av1'
 *     )
 *   });
 *
 * Server-side consumer layer selection:
 *
 *   await consumer.setPreferredLayers({
 *     spatialLayer: 2,   // 0=quarter, 1=half, 2=full resolution
 *     temporalLayer: 2   // 0=low fps, 1=mid fps, 2=full fps
 *   });
 */

async function createAv1Router(worker) {
  const router = await worker.createRouter({ mediaCodecs });

  console.log('AV1 Router created');
  console.log('Router RTP capabilities:', JSON.stringify(router.rtpCapabilities, null, 2));

  return router;
}

async function createAv1Consumer(transport, producerId, rtpCapabilities) {
  const consumer = await transport.consume({
    producerId,
    rtpCapabilities,
    paused: true // Always create paused, resume after client is ready
  });

  // For AV1 SVC, set preferred layers to maximum quality initially
  if (consumer.kind === 'video' && consumer.type === 'svc') {
    await consumer.setPreferredLayers({
      spatialLayer: 2,
      temporalLayer: 2
    });
    console.log('AV1 SVC consumer created with max layers');
  }

  return consumer;
}

// Adaptive layer selection based on consumer bandwidth estimation
async function adaptAv1Layers(consumer, availableBitrate) {
  if (consumer.kind !== 'video' || consumer.type !== 'svc') {
    return;
  }

  let spatialLayer, temporalLayer;

  if (availableBitrate >= 2500000) {
    // High bandwidth: full resolution, full framerate
    spatialLayer = 2;
    temporalLayer = 2;
  } else if (availableBitrate >= 1000000) {
    // Medium bandwidth: half resolution, full framerate
    spatialLayer = 1;
    temporalLayer = 2;
  } else if (availableBitrate >= 500000) {
    // Low bandwidth: quarter resolution, full framerate
    spatialLayer = 0;
    temporalLayer = 2;
  } else {
    // Very low bandwidth: quarter resolution, low framerate
    spatialLayer = 0;
    temporalLayer = 0;
  }

  await consumer.setPreferredLayers({ spatialLayer, temporalLayer });
  console.log(`AV1 SVC adapted: S${spatialLayer}T${temporalLayer} for ${availableBitrate}bps`);
}

module.exports = {
  mediaCodecs,
  createAv1Router,
  createAv1Consumer,
  adaptAv1Layers
};
