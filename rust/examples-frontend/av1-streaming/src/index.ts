/* eslint-disable no-console */
import { Device, parseScalabilityMode } from 'mediasoup-client';
import { MediaKind, RtpCapabilities, RtpParameters } from 'mediasoup-client/lib/RtpParameters';
import { DtlsParameters, TransportOptions, Transport } from 'mediasoup-client/lib/Transport';
import { Consumer } from 'mediasoup-client/lib/types';
import { ConsumerOptions } from 'mediasoup-client/lib/Consumer';

type Brand<K, T> = K & { __brand: T };

type ConsumerId = Brand<string, 'ConsumerId'>;
type ProducerId = Brand<string, 'ProducerId'>;

interface ServerInit {
	action: 'Init';
	consumerTransportOptions: TransportOptions;
	producerTransportOptions: TransportOptions;
	routerRtpCapabilities: RtpCapabilities;
}

interface ServerConnectedProducerTransport {
	action: 'ConnectedProducerTransport';
}

interface ServerProduced {
	action: 'Produced';
	id: ProducerId;
}

interface ServerConnectedConsumerTransport {
	action: 'ConnectedConsumerTransport';
}

interface ServerConsumed {
	action: 'Consumed';
	id: ConsumerId;
	kind: MediaKind;
	rtpParameters: RtpParameters;
}

type ServerMessage =
	ServerInit |
	ServerConnectedProducerTransport |
	ServerProduced |
	ServerConnectedConsumerTransport |
	ServerConsumed;

interface ClientInit {
	action: 'Init';
	rtpCapabilities: RtpCapabilities;
}

interface ClientConnectProducerTransport {
	action: 'ConnectProducerTransport';
	dtlsParameters: DtlsParameters;
}

interface ClientConnectConsumerTransport {
	action: 'ConnectConsumerTransport';
	dtlsParameters: DtlsParameters;
}

interface ClientProduce {
	action: 'Produce';
	kind: MediaKind;
	rtpParameters: RtpParameters;
}

interface ClientConsume {
	action: 'Consume';
	producerId: ProducerId;
}

interface ClientConsumerResume {
	action: 'ConsumerResume';
	id: ConsumerId;
}

interface ClientSetConsumerPreferredLayers {
	action: 'SetConsumerPreferredLayers';
	id: ConsumerId;
	preferredLayers: {
		spatialLayer: number
		temporalLayer: number
	}
}

type ClientMessage =
	ClientInit |
	ClientConnectProducerTransport |
	ClientProduce |
	ClientConnectConsumerTransport |
	ClientConsume |
	ClientConsumerResume |
	ClientSetConsumerPreferredLayers;

async function init()
{
	const sendPreview = document.querySelector('#preview-send') as HTMLVideoElement;
	const receivePreview = document.querySelector('#preview-receive') as HTMLVideoElement;
	const videoCodecNode = document.querySelector('#video-codec') as HTMLSpanElement;
	const scalabilityModeNode = document.querySelector('#scalability-mode') as HTMLSpanElement;

	sendPreview.onloadedmetadata = () =>
	{
		sendPreview.play();
	};
	receivePreview.onloadedmetadata = () =>
	{
		receivePreview.play();
	};

	const decreaseLayer = document.querySelector('#decreaseLayer') as HTMLButtonElement;
	const increaseLayer = document.querySelector('#increaseLayer') as HTMLButtonElement;
	const spatialLayerNode = document.querySelector('#spatial') as HTMLSpanElement;
	const temporalLayerNode = document.querySelector('#temporal') as HTMLSpanElement;
	temporalLayerNode.innerText = 'none';

	let videoConsumer: Consumer | null = null;
	let maxSpatialLayer = 0;
	let maxTemporalLayer = 0;
	let preferredSpatialLayer = 0;
	let preferredTemporalLayer = 0;

	decreaseLayer.addEventListener('click', () => {
		let newPreferredSpatialLayer: number;
		let newPreferredTemporalLayer: number;

		if (preferredTemporalLayer > 0) {
			newPreferredSpatialLayer = preferredSpatialLayer;
			newPreferredTemporalLayer = preferredTemporalLayer - 1;
		} else if (preferredSpatialLayer > 0) {
			newPreferredSpatialLayer = preferredSpatialLayer - 1;
			newPreferredTemporalLayer = maxTemporalLayer;
		} else {
			newPreferredSpatialLayer = maxSpatialLayer;
			newPreferredTemporalLayer = maxTemporalLayer;
		}

		setPreferredLayers(newPreferredSpatialLayer, newPreferredTemporalLayer);
	});
	increaseLayer.addEventListener('click', () => {
		let newPreferredSpatialLayer: number;
		let newPreferredTemporalLayer: number;

		if (preferredTemporalLayer < maxTemporalLayer) {
			newPreferredSpatialLayer = preferredSpatialLayer;
			newPreferredTemporalLayer = preferredTemporalLayer + 1;
		} else if (preferredSpatialLayer < maxSpatialLayer) {
			newPreferredSpatialLayer = preferredSpatialLayer + 1;
			newPreferredTemporalLayer = 0;
		} else {
			newPreferredSpatialLayer = 0;
			newPreferredTemporalLayer = 0;
		}

		setPreferredLayers(newPreferredSpatialLayer, newPreferredTemporalLayer);
	});

	const setPreferredLayers = (spatialLayer: number, temporalLayer: number = 0): void => {
		if (!videoConsumer) {
			throw new Error('Failed to update preferred layers: video consumer not found.');
		}

		preferredSpatialLayer = spatialLayer;
		preferredTemporalLayer = temporalLayer;

		spatialLayerNode.innerText = String(spatialLayer);
		temporalLayerNode.innerText = String(temporalLayer);

		send({
			action: 'SetConsumerPreferredLayers',
			id: videoConsumer.id as ConsumerId,
			preferredLayers: {spatialLayer, temporalLayer}
		});
	};

	const receiveMediaStream = new MediaStream();

	const ws = new WebSocket('ws://localhost:3000/ws');

	function send(message: ClientMessage)
	{
		ws.send(JSON.stringify(message));
	}

	const device = new Device();
	let producerTransport: Transport | undefined;
	let consumerTransport: Transport | undefined;

	{
		const waitingForResponse: Map<ServerMessage['action'], Function> = new Map();

		ws.onmessage = async (message) =>
		{
			const decodedMessage: ServerMessage = JSON.parse(message.data);

			switch (decodedMessage.action)
			{
				case 'Init': {
					await device.load({
						routerRtpCapabilities : decodedMessage.routerRtpCapabilities
					});

					send({
						action          : 'Init',
						rtpCapabilities : device.rtpCapabilities
					});

					producerTransport = device.createSendTransport(
						decodedMessage.producerTransportOptions
					);

					producerTransport
						.on('connect', ({ dtlsParameters }, success) =>
						{
							send({
								action : 'ConnectProducerTransport',
								dtlsParameters
							});
							waitingForResponse.set('ConnectedProducerTransport', () =>
							{
								success();
								console.log('Producer transport connected');
							});
						})
						.on('produce', ({ kind, rtpParameters }, success) =>
						{
							send({
								action : 'Produce',
								kind,
								rtpParameters
							});
							waitingForResponse.set('Produced', ({ id }: {id: string}) =>
							{
								success({ id });
							});
						});

					// Request camera and microphone access
					const mediaStream = await navigator.mediaDevices.getUserMedia({
						audio : true,
						video : {
							width : {
								ideal : 1280
							},
							height : {
								ideal : 720
							},
							frameRate : {
								ideal : 30
							}
						}
					});

					sendPreview.srcObject = mediaStream;

					const producers = [];

					for (const track of mediaStream.getTracks())
					{
						// For video tracks, use AV1 codec with SVC scalability mode
						const codec = track.kind === 'video'
							? device.rtpCapabilities.codecs?.find(
								(c) => c.mimeType.toLowerCase() === 'video/av1'
							)
							: undefined;

						let encodings;

						if (track.kind === 'video' && codec)
						{
							videoCodecNode.innerText = 'AV1';

							// AV1 SVC encoding with L3T3 scalability mode:
							// - L3: 3 spatial layers (quarter, half, full resolution)
							// - T3: 3 temporal layers (15fps, 30fps, 60fps at full res)
							// This allows the SFU to adaptively select layers based on
							// each consumer's available bandwidth.
							encodings = [
								{
									scalabilityMode: 'L3T3',
									maxBitrate: 5000000
								},
							];
							scalabilityModeNode.innerText = 'L3T3';
						}

						const producer = await producerTransport.produce({
							track,
							encodings,
							codec
						});

						producers.push(producer);
						console.log(`${track.kind} producer created:`, producer);
					}

					// Create consumer transport to receive tracks back
					consumerTransport = device.createRecvTransport(
						decodedMessage.consumerTransportOptions
					);

					consumerTransport
						.on('connect', ({ dtlsParameters }, success) =>
						{
							send({
								action : 'ConnectConsumerTransport',
								dtlsParameters
							});
							waitingForResponse.set('ConnectedConsumerTransport', () =>
							{
								success();
								console.log('Consumer transport connected');
							});
						});

					// Consume all produced tracks
					for (const producer of producers)
					{
						await new Promise((resolve) =>
						{
							send({
								action     : 'Consume',
								producerId : producer.id as ProducerId
							});
							waitingForResponse.set('Consumed', async (consumerOptions: ConsumerOptions) =>
							{
								const consumer = await (consumerTransport as Transport).consume(
									consumerOptions
								);

								console.log(`${consumer.kind} consumer created:`, consumer);

								send({
									action : 'ConsumerResume',
									id     : consumer.id as ConsumerId
								});

								receiveMediaStream.addTrack(consumer.track);
								receivePreview.srcObject = receiveMediaStream;

								if (consumer.kind === 'video')
								{
									videoConsumer = consumer;

									const encodings = videoConsumer.rtpParameters.encodings ?? [];

									if (encodings[0]) {
										const scalabilityMode = parseScalabilityMode(encodings[0].scalabilityMode);

										maxSpatialLayer = scalabilityMode.spatialLayers - 1;
										maxTemporalLayer = scalabilityMode.temporalLayers - 1;
										preferredSpatialLayer = maxSpatialLayer;
										preferredTemporalLayer = maxTemporalLayer;

										setPreferredLayers(preferredSpatialLayer, preferredTemporalLayer);
									}
								}

								resolve(undefined);
							});
						});
					}

					break;
				}
				default: {
					const callback = waitingForResponse.get(decodedMessage.action);

					if (callback)
					{
						waitingForResponse.delete(decodedMessage.action);
						callback(decodedMessage);
					}
					else
					{
						console.error('Received unexpected message', decodedMessage);
					}
				}
			}
		};
	}
	ws.onerror = console.error;
}

init();
