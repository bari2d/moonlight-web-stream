use std::{
    future::ready,
    pin::Pin,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use common::{
    api_bindings::{
        RtcIceCandidate, RtcSdpType, RtcSessionDescription, StreamClientMessage,
        StreamServerMessage, StreamSignalingMessage, TransportChannelId,
    },
    config::{PortRange, WebRtcConfig},
    ipc::{ServerIpcMessage, StreamerIpcMessage},
};
use moonlight_common::stream::{
    audio::{AudioConfig, OpusMultistreamConfig},
    video::{DecodeResult, VideoDecodeUnit, VideoFormats, VideoSetup},
};
use tokio::{
    runtime::Handle,
    spawn,
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    time::sleep,
};
use tracing::{debug, error, trace, warn};
use webrtc::{
    api::{
        APIBuilder, interceptor_registry::register_default_interceptors, media_engine::MediaEngine,
        setting_engine::SettingEngine,
    },
    data_channel::{
        RTCDataChannel, data_channel_init::RTCDataChannelInit,
        data_channel_message::DataChannelMessage,
    },
    ice::udp_network::{EphemeralUDP, UDPNetwork},
    ice_transport::{
        ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
        ice_connection_state::RTCIceConnectionState,
    },
    interceptor::registry::Registry,
    peer_connection::{
        RTCPeerConnection,
        configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::{sdp_type::RTCSdpType, session_description::RTCSessionDescription},
    },
};

use crate::{
    TIMEOUT_DURATION,
    convert::{
        from_webrtc_sdp, into_webrtc_ice, into_webrtc_ice_candidate, into_webrtc_network_type,
    },
    transport::{
        InboundPacket, OutboundPacket, TransportChannel, TransportError, TransportEvent,
        TransportEvents, TransportSender,
        webrtc::{
            audio::{WebRtcAudio, register_audio_codecs},
            sender::register_header_extensions,
            video::{WebRtcVideo, register_video_codecs},
        },
    },
};

mod audio;
mod sender;
mod video;

struct WebRtcInner {
    peer: Arc<RTCPeerConnection>,
    event_sender: Sender<TransportEvent>,
    general_channel: Arc<RTCDataChannel>,
    stats_channel: Arc<RTCDataChannel>,
    input_channels: Mutex<Vec<Arc<RTCDataChannel>>>,
    video: Mutex<WebRtcVideo>,
    audio: Mutex<WebRtcAudio>,
    // Timeout / Terminate
    termination_state: Mutex<TerminationState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TerminationRequest {
    generation: u64,
    requested_at: Instant,
}

#[derive(Debug, Default)]
struct TerminationState {
    next_generation: u64,
    pending: Option<TerminationRequest>,
}

impl TerminationState {
    fn request(&mut self, requested_at: Instant) -> TerminationRequest {
        self.next_generation = self.next_generation.wrapping_add(1);
        let request = TerminationRequest {
            generation: self.next_generation,
            requested_at,
        };
        self.pending = Some(request);
        request
    }

    fn clear(&mut self) {
        self.pending = None;
    }

    fn is_current_and_expired(&self, request: TerminationRequest, now: Instant) -> bool {
        self.pending.is_some_and(|pending| {
            pending.generation == request.generation
                && pending.requested_at == request.requested_at
                && now.saturating_duration_since(request.requested_at) > TIMEOUT_DURATION
        })
    }
}

pub async fn new(
    config: &WebRtcConfig,
    video_frame_queue_size: usize,
    audio_sample_queue_size: usize,
) -> Result<(WebRTCTransportSender, WebRTCTransportEvents), anyhow::Error> {
    // -- Configure WebRTC
    let rtc_config = RTCConfiguration {
        ice_servers: config
            .ice_servers
            .clone()
            .into_iter()
            .map(into_webrtc_ice)
            .collect(),
        ..Default::default()
    };
    let mut api_settings = SettingEngine::default();

    if let Some(PortRange { min, max }) = config.port_range {
        match EphemeralUDP::new(min, max) {
            Ok(udp) => {
                api_settings.set_udp_network(UDPNetwork::Ephemeral(udp));
            }
            Err(err) => {
                warn!("[Stream]: Invalid port range in config: {err:?}");
            }
        }
    }
    if let Some(mapping) = config.nat_1to1.as_ref() {
        api_settings.set_nat_1to1_ips(
            mapping.ips.clone(),
            into_webrtc_ice_candidate(mapping.ice_candidate_type),
        );
    }
    api_settings.set_network_types(
        config
            .network_types
            .iter()
            .copied()
            .map(into_webrtc_network_type)
            .collect(),
    );

    api_settings.set_include_loopback_candidate(config.include_loopback_candidates);

    // -- Register media codecs
    // TODO: register them based on the sdp
    let mut api_media = MediaEngine::default();
    register_audio_codecs(&mut api_media).expect("failed to register audio codecs");
    register_video_codecs(&mut api_media).expect("failed to register video codecs");
    register_header_extensions(&mut api_media).expect("failed to register header extensions");

    // -- Build Api
    let mut api_registry = Registry::new();

    // Use the default set of Interceptors
    api_registry = register_default_interceptors(api_registry, &mut api_media)
        .expect("failed to register webrtc default interceptors");

    let api = APIBuilder::new()
        .with_setting_engine(api_settings)
        .with_media_engine(api_media)
        .with_interceptor_registry(api_registry)
        .build();

    let (event_sender, event_receiver) = channel::<TransportEvent>(20);

    let peer = Arc::new(api.new_peer_connection(rtc_config).await?);

    let general_channel = peer.create_data_channel("general", None).await?;
    let stats_channel = peer.create_data_channel("stats", None).await?;

    let runtime = Handle::current();
    let this_owned = Arc::new(WebRtcInner {
        peer: peer.clone(),
        event_sender,
        general_channel: general_channel.clone(),
        stats_channel,
        input_channels: Default::default(),
        video: Mutex::new(WebRtcVideo::new(
            runtime.clone(),
            Arc::downgrade(&peer),
            video_frame_queue_size,
        )),
        audio: Mutex::new(WebRtcAudio::new(
            runtime,
            Arc::downgrade(&peer),
            audio_sample_queue_size,
        )),
        termination_state: Mutex::new(TerminationState::default()),
    });

    // Add all data channels. The server creates all data channels
    {
        let this = this_owned.clone();
        this.clone().on_data_channel(general_channel).await;

        struct Options {
            reliable: bool,
            ordered: bool,
        }
        #[rustfmt::skip]
        const INPUT_CHANNELS: &[(&str, Options)] = &[
            ("mouse_reliable", Options { reliable: true , ordered: true  }),
            ("mouse_absolute", Options { reliable: false, ordered: false }),
            ("mouse_relative", Options { reliable: true , ordered: false }),
            ("keyboard",       Options { reliable: true , ordered: true  }),
            ("touch",          Options { reliable: true , ordered: true  }),
            ("controllers",    Options { reliable: true , ordered: true  }),
            ("controller0",    Options { reliable: false, ordered: false }),
            ("controller1",    Options { reliable: false, ordered: false }),
            ("controller2",    Options { reliable: false, ordered: false }),
            ("controller3",    Options { reliable: false, ordered: false }),
            ("controller4",    Options { reliable: false, ordered: false }),
            ("controller5",    Options { reliable: false, ordered: false }),
            ("controller6",    Options { reliable: false, ordered: false }),
            ("controller7",    Options { reliable: false, ordered: false }),
            ("controller8",    Options { reliable: false, ordered: false }),
            ("controller9",    Options { reliable: false, ordered: false }),
            ("controller10",   Options { reliable: false, ordered: false }),
            ("controller11",   Options { reliable: false, ordered: false }),
            ("controller12",   Options { reliable: false, ordered: false }),
            ("controller13",   Options { reliable: false, ordered: false }),
            ("controller14",   Options { reliable: false, ordered: false }),
            ("controller15",   Options { reliable: false, ordered: false }),
        ];

        let mut input_channels = this.input_channels.lock().await;
        for (channel, options) in INPUT_CHANNELS {
            let data_channel = this
                .peer
                .create_data_channel(
                    channel,
                    Some(RTCDataChannelInit {
                        ordered: Some(options.ordered),
                        max_retransmits: (!options.reliable).then_some(0),
                        ..Default::default()
                    }),
                )
                .await?;

            this.clone().on_data_channel(data_channel.clone()).await;

            input_channels.push(data_channel);
        }
    }

    let this = Arc::downgrade(&this_owned);

    // -- Connection state
    peer.on_ice_connection_state_change(create_event_handler(
        this.clone(),
        async move |this, state| {
            this.on_ice_connection_state_change(state).await;
        },
    ));
    peer.on_peer_connection_state_change(create_event_handler(
        this.clone(),
        async move |this, state| {
            this.on_peer_connection_state_change(state).await;
        },
    ));

    // -- Signaling
    peer.on_ice_candidate(create_event_handler(
        this.clone(),
        async move |this, candidate| {
            this.on_ice_candidate(candidate).await;
        },
    ));

    drop(peer);

    Ok((
        WebRTCTransportSender {
            inner: this_owned.clone(),
        },
        WebRTCTransportEvents { event_receiver },
    ))
}

// It compiling...
#[allow(clippy::complexity)]
fn create_event_handler<F, Args>(
    inner: Weak<WebRtcInner>,
    f: F,
) -> Box<
    dyn FnMut(Args) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static,
>
where
    Args: Send + 'static,
    F: AsyncFn(Arc<WebRtcInner>, Args) + Send + Sync + Clone + 'static,
    for<'a> F::CallRefFuture<'a>: Send,
{
    Box::new(move |args: Args| {
        let inner = inner.clone();
        let Some(inner) = inner.upgrade() else {
            debug!("Called webrtc event handler while the main type is already deallocated");
            return Box::pin(ready(())) as Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
        };

        let future = f.clone();
        Box::pin(async move {
            future(inner, args).await;
        }) as Pin<Box<dyn Future<Output = ()> + Send + 'static>>
    })
        as Box<
            dyn FnMut(Args) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
                + Send
                + Sync
                + 'static,
        >
}
#[allow(clippy::complexity)]
fn create_channel_message_handler(
    inner: Weak<WebRtcInner>,
    channel: TransportChannel,
) -> Box<
    dyn FnMut(DataChannelMessage) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        + Send
        + Sync
        + 'static,
> {
    debug!("setting up channel {:?}", channel);
    create_event_handler(inner, async move |inner, message: DataChannelMessage| {
        let Some(packet) = InboundPacket::deserialize(channel, &message.data) else {
            return;
        };

        if let Err(err) = inner
            .event_sender
            .send(TransportEvent::RecvPacket(packet))
            .await
        {
            warn!("Failed to dispatch RecvPacket event: {err:?}");
        };
    })
}

fn transport_channel_for_label(label: &str) -> Option<TransportChannel> {
    let channel_id = match label {
        "general" => TransportChannelId::GENERAL,
        "mouse_reliable" => TransportChannelId::MOUSE_RELIABLE,
        "mouse_absolute" => TransportChannelId::MOUSE_ABSOLUTE,
        "mouse_relative" => TransportChannelId::MOUSE_RELATIVE,
        "touch" => TransportChannelId::TOUCH,
        "keyboard" => TransportChannelId::KEYBOARD,
        "controllers" => TransportChannelId::CONTROLLERS,
        _ => {
            let id = label.strip_prefix("controller")?.parse::<usize>().ok()?;
            *InboundPacket::CONTROLLER_CHANNELS.get(id)?
        }
    };

    Some(TransportChannel(channel_id))
}

impl WebRtcInner {
    // -- Handle Connection State
    async fn on_ice_connection_state_change(self: &Arc<Self>, _state: RTCIceConnectionState) {}
    async fn on_peer_connection_state_change(self: Arc<Self>, state: RTCPeerConnectionState) {
        #[allow(clippy::collapsible_if)]
        if matches!(state, RTCPeerConnectionState::Closed) {
            if let Err(err) = self.event_sender.send(TransportEvent::Closed).await {
                warn!("Failed to send peer closed event to stream: {err:?}");
                self.request_terminate().await;
            };
        } else if matches!(
            state,
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected
        ) {
            self.request_terminate().await;
        } else {
            self.clear_terminate_request().await;
        }
    }

    // -- Handle Signaling
    async fn send_offer(&self) -> bool {
        let local_description = match self.peer.create_offer(None).await {
            Err(err) => {
                error!("[Signaling]: failed to create offer: {err:?}");
                return false;
            }
            Ok(value) => value,
        };

        if let Err(err) = self
            .peer
            .set_local_description(local_description.clone())
            .await
        {
            error!("[Signaling]: failed to set local description: {err:?}");
            return false;
        }

        debug!(
            "[Signaling] Sending Local Description as Offer: {:?}",
            local_description.sdp
        );

        if let Err(err) = self
            .event_sender
            .send(TransportEvent::SendIpc(StreamerIpcMessage::WebSocket(
                StreamServerMessage::WebRtc(StreamSignalingMessage::Description(
                    RtcSessionDescription {
                        ty: from_webrtc_sdp(local_description.sdp_type),
                        sdp: local_description.sdp,
                    },
                )),
            )))
            .await
        {
            warn!("Failed to send local description (offer) via web socket from peer: {err:?}");
        };

        true
    }

    async fn on_ws_message(&self, message: StreamClientMessage) {
        match message {
            StreamClientMessage::StartStream { settings } => {
                let video_supported_formats = VideoFormats::from_bits(settings.supported_codecs)
                    .unwrap_or_else(|| {
                        warn!(
                            "Failed to deserialize VideoFormats: {}, falling back to only H264",
                            VideoFormats::from_bits_retain(settings.supported_codecs)
                        );
                        VideoFormats::H264
                    });
                {
                    let mut video = self.video.lock().await;
                    video.set_codecs(video_supported_formats).await;
                }

                // TODO: check peer for supported formats via sdp

                if let Err(err) = self
                    .event_sender
                    .send(TransportEvent::StartStream { settings })
                    .await
                {
                    error!("Failed to send start stream: {err}");
                }
            }
            StreamClientMessage::WebRtc(StreamSignalingMessage::Description(description)) => {
                debug!("[Signaling] Received Remote Description: {:?}", description);

                let description = match &description.ty {
                    RtcSdpType::Offer => RTCSessionDescription::offer(description.sdp),
                    RtcSdpType::Answer => RTCSessionDescription::answer(description.sdp),
                    RtcSdpType::Pranswer => RTCSessionDescription::pranswer(description.sdp),
                    _ => {
                        error!(
                            "[Signaling]: failed to handle RTCSdpType {:?}",
                            description.ty
                        );
                        return;
                    }
                };

                let Ok(description) = description else {
                    error!("[Signaling]: Received invalid RTCSessionDescription");
                    return;
                };

                let remote_ty = description.sdp_type;

                if remote_ty == RTCSdpType::Offer {
                    warn!(
                        "Received an offer from the client. This shouldn't be possible. Dropping the offer"
                    );
                    return;
                }
                if let Err(err) = self.peer.set_remote_description(description).await {
                    error!("[Signaling]: failed to set remote description: {err:?}");
                }
            }
            StreamClientMessage::WebRtc(StreamSignalingMessage::AddIceCandidate(description)) => {
                debug!("[Signaling] Received Ice Candidate");

                if let Err(err) = self
                    .peer
                    .add_ice_candidate(RTCIceCandidateInit {
                        candidate: description.candidate,
                        sdp_mid: description.sdp_mid,
                        sdp_mline_index: description.sdp_mline_index,
                        username_fragment: description.username_fragment,
                    })
                    .await
                {
                    warn!("[Signaling]: failed to add ice candidate: {err:?}");
                }
            }
            _ => {}
        }
    }

    async fn on_ice_candidate(&self, candidate: Option<RTCIceCandidate>) {
        let Some(candidate) = candidate else {
            return;
        };

        let Ok(candidate_json) = candidate.to_json() else {
            return;
        };

        debug!(
            "[Signaling] Sending Ice Candidate: {}",
            candidate_json.candidate
        );

        let message =
            StreamServerMessage::WebRtc(StreamSignalingMessage::AddIceCandidate(RtcIceCandidate {
                candidate: candidate_json.candidate,
                sdp_mid: candidate_json.sdp_mid,
                sdp_mline_index: candidate_json.sdp_mline_index,
                username_fragment: candidate_json.username_fragment,
            }));

        if let Err(err) = self
            .event_sender
            .send(TransportEvent::SendIpc(StreamerIpcMessage::WebSocket(
                message,
            )))
            .await
        {
            error!("Failed to send web socket message from peer: {err:?}");
        };
    }

    async fn on_data_channel(self: Arc<Self>, channel: Arc<RTCDataChannel>) {
        let label = channel.label();
        debug!("adding data channel: \"{label}\"");

        let inner = Arc::downgrade(&self);
        let Some(transport_channel) = transport_channel_for_label(label) else {
            return;
        };

        if label == "general" {
            debug!("setting up general channel message handler");
        }
        channel.on_message(create_channel_message_handler(inner, transport_channel));
    }

    // -- Termination
    async fn request_terminate(self: &Arc<Self>) {
        let this = self.clone();

        let request = self.termination_state.lock().await.request(Instant::now());

        spawn(async move {
            sleep(TIMEOUT_DURATION + Duration::from_millis(200)).await;

            let should_terminate = {
                this.termination_state
                    .lock()
                    .await
                    .is_current_and_expired(request, Instant::now())
            };

            if should_terminate {
                match send_closed_if_current(&this.event_sender, &this.termination_state, request)
                    .await
                {
                    Ok(_) => {}
                    Err(err) => warn!("Failed to send that the peer is closed: {err:?}"),
                }
            }
        });
    }
    async fn clear_terminate_request(&self) {
        self.termination_state.lock().await.clear();
    }
}

async fn send_closed_if_current(
    event_sender: &Sender<TransportEvent>,
    termination_state: &Mutex<TerminationState>,
    request: TerminationRequest,
) -> Result<bool, tokio::sync::mpsc::error::SendError<()>> {
    // Reserve capacity without holding the termination mutex so a recovered
    // peer can invalidate this request even while the event queue is full.
    let permit = event_sender.reserve().await?;

    // Revalidate the exact request after the await. Keep the guard through the
    // synchronous send so a reconnect cannot clear it between check and send.
    let state = termination_state.lock().await;
    if state.is_current_and_expired(request, Instant::now()) {
        permit.send(TransportEvent::Closed);
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;

    #[test]
    fn mouse_labels_map_to_their_logical_channels() {
        assert_eq!(
            transport_channel_for_label("mouse_reliable").map(|channel| channel.0),
            Some(TransportChannelId::MOUSE_RELIABLE)
        );
        assert_eq!(
            transport_channel_for_label("mouse_absolute").map(|channel| channel.0),
            Some(TransportChannelId::MOUSE_ABSOLUTE)
        );
        assert_eq!(
            transport_channel_for_label("mouse_relative").map(|channel| channel.0),
            Some(TransportChannelId::MOUSE_RELATIVE)
        );
    }

    #[test]
    fn controller_label_mapping_is_bounded() {
        assert_eq!(
            transport_channel_for_label("controller15").map(|channel| channel.0),
            Some(TransportChannelId::CONTROLLER15)
        );
        assert!(transport_channel_for_label("controller16").is_none());
        assert!(transport_channel_for_label("controller_invalid").is_none());
    }

    #[test]
    fn reconnect_cancels_a_terminal_event_waiting_for_queue_capacity() {
        tokio::runtime::Runtime::new()
            .expect("failed to create test runtime")
            .block_on(async {
                let (sender, mut receiver) = channel(1);
                sender
                    .send(TransportEvent::Closed)
                    .await
                    .expect("failed to fill test event queue");

                let state = Arc::new(Mutex::new(TerminationState::default()));
                let request = state
                    .lock()
                    .await
                    .request(Instant::now() - TIMEOUT_DURATION - Duration::from_millis(1));

                let task = spawn({
                    let sender = sender.clone();
                    let state = state.clone();
                    async move { send_closed_if_current(&sender, &state, request).await }
                });

                tokio::task::yield_now().await;
                assert!(
                    !task.is_finished(),
                    "terminal event should be waiting for queue capacity"
                );

                state.lock().await.clear();
                assert!(matches!(
                    receiver.recv().await,
                    Some(TransportEvent::Closed)
                ));
                assert_eq!(
                    task.await.expect("termination task panicked"),
                    Ok(false),
                    "a reconnect must invalidate the queued terminal event"
                );
                assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
            });
    }
}

pub struct WebRTCTransportEvents {
    event_receiver: Receiver<TransportEvent>,
}

#[async_trait]
impl TransportEvents for WebRTCTransportEvents {
    async fn poll_event(&mut self) -> Result<TransportEvent, TransportError> {
        trace!("Polling WebRTCEvents");
        self.event_receiver
            .recv()
            .await
            .ok_or(TransportError::Closed)
    }
}

pub struct WebRTCTransportSender {
    inner: Arc<WebRtcInner>,
}

#[async_trait]
impl TransportSender for WebRTCTransportSender {
    async fn setup_video(&self, setup: VideoSetup) -> i32 {
        let mut video = self.inner.video.lock().await;
        if video.setup(&self.inner, setup).await {
            0
        } else {
            -1
        }
    }
    async fn send_video_unit<'a>(
        &'a self,
        unit: VideoDecodeUnit<&'a [u8]>,
    ) -> Result<DecodeResult, TransportError> {
        let mut video = self.inner.video.lock().await;
        Ok(video.send_decode_unit(&unit).await)
    }

    async fn setup_audio(
        &self,
        audio_config: AudioConfig,
        stream_config: OpusMultistreamConfig,
    ) -> i32 {
        let mut audio = self.inner.audio.lock().await;

        audio.setup(&self.inner, audio_config, stream_config).await
    }
    async fn send_audio_sample(&self, data: &[u8]) -> Result<(), TransportError> {
        let mut audio = self.inner.audio.lock().await;

        audio.send_audio_sample(data).await;

        Ok(())
    }

    async fn on_setup_complete(&self) {
        if !self.inner.send_offer().await {
            error!("Failed to send offer to client. Requesting Termination");
            self.inner.request_terminate().await;
        }
    }

    async fn send(&self, packet: OutboundPacket) -> Result<(), TransportError> {
        let mut buffer = Vec::new();

        let Some((channel, range)) = packet.serialize(&mut buffer) else {
            warn!("Failed to serialize packet: {packet:?}");
            return Ok(());
        };

        let bytes = Bytes::from(buffer);
        let bytes = bytes.slice(range);

        match channel.0 {
            TransportChannelId::GENERAL => match self.inner.general_channel.send(&bytes).await {
                Ok(_) => {}
                Err(webrtc::Error::ErrDataChannelNotOpen) => {
                    return Err(TransportError::ChannelClosed);
                }
                _ => {}
            },
            TransportChannelId::STATS => {
                if let Err(err) = self.inner.stats_channel.send(&bytes).await {
                    debug!(error = ?err, "Failed to send stat message");
                }
            }
            _ => {
                warn!("Cannot send data on channel {channel:?}");
                return Err(TransportError::ChannelClosed);
            }
        }
        Ok(())
    }

    async fn on_ipc_message(&self, message: ServerIpcMessage) -> Result<(), TransportError> {
        if let ServerIpcMessage::WebSocket(message) = message {
            self.inner.on_ws_message(message).await;
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.inner
            .peer
            .close()
            .await
            .map_err(|err| TransportError::Implementation(err.into()))?;

        Ok(())
    }
}
