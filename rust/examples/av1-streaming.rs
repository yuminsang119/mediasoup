use actix::prelude::*;
use actix_web::web::{Data, Payload};
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use mediasoup::prelude::*;
use mediasoup::worker::{WorkerLogLevel, WorkerLogTag};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::num::{NonZeroU32, NonZeroU8};

/// List of codecs that SFU will accept from clients.
/// AV1 is configured as the primary video codec with SVC support via Dependency Descriptor.
fn media_codecs() -> Vec<RtpCodecCapability> {
    vec![
        RtpCodecCapability::Audio {
            mime_type: MimeTypeAudio::Opus,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(48000).unwrap(),
            channels: NonZeroU8::new(2).unwrap(),
            parameters: RtpCodecParametersParameters::from([("useinbandfec", 1_u32.into())]),
            rtcp_feedback: vec![RtcpFeedback::TransportCc],
        },
        // AV1 video codec - supports SVC with Dependency Descriptor RTP header extension.
        // AV1 provides superior compression efficiency compared to VP8/VP9/H264,
        // with native scalable video coding (SVC) support via the AV1 dependency descriptor.
        RtpCodecCapability::Video {
            mime_type: MimeTypeVideo::AV1,
            preferred_payload_type: None,
            clock_rate: NonZeroU32::new(90000).unwrap(),
            parameters: RtpCodecParametersParameters::default(),
            rtcp_feedback: vec![
                RtcpFeedback::Nack,
                RtcpFeedback::NackPli,
                RtcpFeedback::CcmFir,
                RtcpFeedback::GoogRemb,
                RtcpFeedback::TransportCc,
            ],
        },
    ]
}

/// Data structure containing all the necessary information about transport options required from
/// the server to establish transport connection on the client
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransportOptions {
    id: TransportId,
    dtls_parameters: DtlsParameters,
    ice_candidates: Vec<IceCandidate>,
    ice_parameters: IceParameters,
}

/// Server messages sent to the client
#[derive(Serialize, Message)]
#[serde(tag = "action")]
#[rtype(result = "()")]
#[allow(clippy::large_enum_variant)]
enum ServerMessage {
    /// Initialization message with consumer/producer transport options and Router's RTP
    /// capabilities necessary to establish WebRTC transport connection client-side
    #[serde(rename_all = "camelCase")]
    Init {
        consumer_transport_options: TransportOptions,
        producer_transport_options: TransportOptions,
        router_rtp_capabilities: RtpCapabilitiesFinalized,
    },
    ConnectedProducerTransport,
    #[serde(rename_all = "camelCase")]
    Produced { id: ProducerId },
    ConnectedConsumerTransport,
    #[serde(rename_all = "camelCase")]
    Consumed {
        id: ConsumerId,
        producer_id: ProducerId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
    },
}

/// Client messages sent to the server
#[derive(Deserialize, Message)]
#[serde(tag = "action")]
#[rtype(result = "()")]
enum ClientMessage {
    #[serde(rename_all = "camelCase")]
    Init { rtp_capabilities: RtpCapabilities },
    #[serde(rename_all = "camelCase")]
    ConnectProducerTransport { dtls_parameters: DtlsParameters },
    #[serde(rename_all = "camelCase")]
    Produce {
        kind: MediaKind,
        rtp_parameters: RtpParameters,
    },
    #[serde(rename_all = "camelCase")]
    ConnectConsumerTransport { dtls_parameters: DtlsParameters },
    #[serde(rename_all = "camelCase")]
    Consume { producer_id: ProducerId },
    #[serde(rename_all = "camelCase")]
    ConsumerResume { id: ConsumerId },
    /// Request to set preferred spatial and temporal layers for AV1 SVC
    #[serde(rename_all = "camelCase")]
    SetConsumerPreferredLayers {
        id: ConsumerId,
        preferred_layers: ConsumerLayers,
    },
}

/// Internal actor messages for convenience
#[derive(Message)]
#[rtype(result = "()")]
enum InternalMessage {
    SaveProducer(Producer),
    SaveConsumer(Consumer),
    Stop,
}

/// Consumer/producer transports pair for the client
struct Transports {
    consumer: WebRtcTransport,
    producer: WebRtcTransport,
}

/// Actor that will represent WebSocket connection from the client for AV1 streaming.
/// AV1 supports SVC (Scalable Video Coding) via the Dependency Descriptor RTP header extension,
/// allowing adaptive bitrate streaming with spatial and temporal layer selection.
struct Av1StreamingConnection {
    client_rtp_capabilities: Option<RtpCapabilities>,
    consumers: HashMap<ConsumerId, Consumer>,
    producers: Vec<Producer>,
    router: Router,
    transports: Transports,
}

impl Av1StreamingConnection {
    async fn new(worker_manager: &WorkerManager) -> Result<Self, String> {
        let worker = worker_manager
            .create_worker({
                let mut settings = WorkerSettings::default();
                settings.log_level = WorkerLogLevel::Debug;
                settings.log_tags = vec![
                    WorkerLogTag::Info,
                    WorkerLogTag::Ice,
                    WorkerLogTag::Dtls,
                    WorkerLogTag::Rtp,
                    WorkerLogTag::Srtp,
                    WorkerLogTag::Rtcp,
                    WorkerLogTag::Rtx,
                    WorkerLogTag::Bwe,
                    WorkerLogTag::Score,
                    WorkerLogTag::Simulcast,
                    WorkerLogTag::Svc,
                    WorkerLogTag::Sctp,
                    WorkerLogTag::Message,
                ];

                settings
            })
            .await
            .map_err(|error| format!("Failed to create worker: {error}"))?;
        let router = worker
            .create_router(RouterOptions::new(media_codecs()))
            .await
            .map_err(|error| format!("Failed to create router: {error}"))?;

        let transport_options =
            WebRtcTransportOptions::new(WebRtcTransportListenInfos::new(ListenInfo {
                protocol: Protocol::Udp,
                ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                announced_address: None,
                expose_internal_ip: false,
                port: None,
                port_range: None,
                flags: None,
                send_buffer_size: None,
                recv_buffer_size: None,
            }));
        let producer_transport = router
            .create_webrtc_transport(transport_options.clone())
            .await
            .map_err(|error| format!("Failed to create producer transport: {error}"))?;

        let consumer_transport = router
            .create_webrtc_transport(transport_options)
            .await
            .map_err(|error| format!("Failed to create consumer transport: {error}"))?;

        Ok(Self {
            client_rtp_capabilities: None,
            consumers: HashMap::new(),
            producers: vec![],
            router,
            transports: Transports {
                consumer: consumer_transport,
                producer: producer_transport,
            },
        })
    }
}

impl Actor for Av1StreamingConnection {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("AV1 streaming WebSocket connection created");

        let server_init_message = ServerMessage::Init {
            consumer_transport_options: TransportOptions {
                id: self.transports.consumer.id(),
                dtls_parameters: self.transports.consumer.dtls_parameters(),
                ice_candidates: self.transports.consumer.ice_candidates().clone(),
                ice_parameters: self.transports.consumer.ice_parameters().clone(),
            },
            producer_transport_options: TransportOptions {
                id: self.transports.producer.id(),
                dtls_parameters: self.transports.producer.dtls_parameters(),
                ice_candidates: self.transports.producer.ice_candidates().clone(),
                ice_parameters: self.transports.producer.ice_parameters().clone(),
            },
            router_rtp_capabilities: self.router.rtp_capabilities().clone(),
        };

        ctx.address().do_send(server_init_message);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        println!("AV1 streaming WebSocket connection closed");
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Av1StreamingConnection {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {}
            Ok(ws::Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(message) => {
                    ctx.address().do_send(message);
                }
                Err(error) => {
                    eprintln!("Failed to parse client message: {text}\n{error}");
                }
            },
            Ok(ws::Message::Binary(bin)) => {
                eprintln!("Unexpected binary message: {bin:?}");
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => ctx.stop(),
        }
    }
}

impl Handler<ClientMessage> for Av1StreamingConnection {
    type Result = ();

    fn handle(&mut self, message: ClientMessage, ctx: &mut Self::Context) {
        match message {
            ClientMessage::Init { rtp_capabilities } => {
                self.client_rtp_capabilities.replace(rtp_capabilities);
            }
            ClientMessage::SetConsumerPreferredLayers {
                id,
                preferred_layers,
            } => {
                // AV1 SVC layer selection - allows the consumer to select which spatial and
                // temporal layers to receive, enabling adaptive quality based on bandwidth
                if let Some(consumer) = self.consumers.get(&id).cloned() {
                    actix::spawn(async move {
                        match consumer.set_preferred_layers(preferred_layers).await {
                            Ok(_) => {
                                println!(
                                    "AV1 SVC: set preferred layers {:?} for consumer {}",
                                    preferred_layers,
                                    consumer.id(),
                                );
                            }
                            Err(error) => {
                                println!(
                                    "Failed to set AV1 SVC layers {:?} for consumer {}: {}",
                                    preferred_layers,
                                    consumer.id(),
                                    error,
                                );
                            }
                        }
                    });
                }
            }
            ClientMessage::ConnectProducerTransport { dtls_parameters } => {
                let address = ctx.address();
                let transport = self.transports.producer.clone();
                actix::spawn(async move {
                    match transport
                        .connect(WebRtcTransportRemoteParameters { dtls_parameters })
                        .await
                    {
                        Ok(_) => {
                            address.do_send(ServerMessage::ConnectedProducerTransport);
                            println!("Producer transport connected");
                        }
                        Err(error) => {
                            eprintln!("Failed to connect producer transport: {error}");
                            address.do_send(InternalMessage::Stop);
                        }
                    }
                });
            }
            ClientMessage::Produce {
                kind,
                rtp_parameters,
            } => {
                let address = ctx.address();
                let transport = self.transports.producer.clone();
                actix::spawn(async move {
                    match transport
                        .produce(ProducerOptions::new(kind, rtp_parameters))
                        .await
                    {
                        Ok(producer) => {
                            let id = producer.id();
                            address.do_send(ServerMessage::Produced { id });
                            address.do_send(InternalMessage::SaveProducer(producer));
                            println!("{kind:?} producer created (AV1): {id}");
                        }
                        Err(error) => {
                            eprintln!("Failed to create {kind:?} producer: {error}");
                            address.do_send(InternalMessage::Stop);
                        }
                    }
                });
            }
            ClientMessage::ConnectConsumerTransport { dtls_parameters } => {
                let address = ctx.address();
                let transport = self.transports.consumer.clone();
                actix::spawn(async move {
                    match transport
                        .connect(WebRtcTransportRemoteParameters { dtls_parameters })
                        .await
                    {
                        Ok(_) => {
                            address.do_send(ServerMessage::ConnectedConsumerTransport);
                            println!("Consumer transport connected");
                        }
                        Err(error) => {
                            eprintln!("Failed to connect consumer transport: {error}");
                            address.do_send(InternalMessage::Stop);
                        }
                    }
                });
            }
            ClientMessage::Consume { producer_id } => {
                let address = ctx.address();
                let transport = self.transports.consumer.clone();
                let rtp_capabilities = match self.client_rtp_capabilities.clone() {
                    Some(rtp_capabilities) => rtp_capabilities,
                    None => {
                        eprintln!("Client should send RTP capabilities before consuming");
                        return;
                    }
                };
                actix::spawn(async move {
                    let mut options = ConsumerOptions::new(producer_id, rtp_capabilities);
                    options.paused = true;

                    match transport.consume(options).await {
                        Ok(consumer) => {
                            let id = consumer.id();
                            let kind = consumer.kind();
                            let rtp_parameters = consumer.rtp_parameters().clone();
                            address.do_send(ServerMessage::Consumed {
                                id,
                                producer_id,
                                kind,
                                rtp_parameters,
                            });
                            address.do_send(InternalMessage::SaveConsumer(consumer));
                            println!("{kind:?} consumer created (AV1): {id}");
                        }
                        Err(error) => {
                            eprintln!("Failed to create consumer: {error}");
                            address.do_send(InternalMessage::Stop);
                        }
                    }
                });
            }
            ClientMessage::ConsumerResume { id } => {
                if let Some(consumer) = self.consumers.get(&id).cloned() {
                    actix::spawn(async move {
                        match consumer.resume().await {
                            Ok(_) => {
                                println!(
                                    "Successfully resumed {:?} consumer {}",
                                    consumer.kind(),
                                    consumer.id(),
                                );
                            }
                            Err(error) => {
                                println!(
                                    "Failed to resume {:?} consumer {}: {}",
                                    consumer.kind(),
                                    consumer.id(),
                                    error,
                                );
                            }
                        }
                    });
                }
            }
        }
    }
}

impl Handler<ServerMessage> for Av1StreamingConnection {
    type Result = ();

    fn handle(&mut self, message: ServerMessage, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&message).unwrap());
    }
}

impl Handler<InternalMessage> for Av1StreamingConnection {
    type Result = ();

    fn handle(&mut self, message: InternalMessage, ctx: &mut Self::Context) {
        match message {
            InternalMessage::Stop => {
                ctx.stop();
            }
            InternalMessage::SaveProducer(producer) => {
                self.producers.push(producer);
            }
            InternalMessage::SaveConsumer(consumer) => {
                self.consumers.insert(consumer.id(), consumer);
            }
        }
    }
}

async fn ws_index(
    request: HttpRequest,
    worker_manager: Data<WorkerManager>,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    match Av1StreamingConnection::new(&worker_manager).await {
        Ok(av1_connection) => ws::start(av1_connection, &request, stream),
        Err(error) => {
            eprintln!("{error}");

            Ok(HttpResponse::InternalServerError().finish())
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    println!("AV1 Streaming Server starting on http://127.0.0.1:3000");
    println!("AV1 codec provides:");
    println!("  - Superior compression efficiency (~30% better than VP9)");
    println!("  - Native SVC support via Dependency Descriptor");
    println!("  - Adaptive spatial/temporal layer selection");

    let worker_manager = Data::new(WorkerManager::new());
    HttpServer::new(move || {
        App::new()
            .app_data(worker_manager.clone())
            .route("/ws", web::get().to(ws_index))
    })
    .workers(2)
    .bind("127.0.0.1:3000")?
    .run()
    .await
}
