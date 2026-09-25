#![feature(async_fn_traits)]

use std::{
    collections::BTreeMap,
    future::Future,
    io, panic,
    process::exit,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use common::{
    api_bindings::{
        GeneralClientMessage, GeneralServerMessage, LogMessageType, StreamClientMessage,
        StreamPermissions, StreamSettings, TransportType,
    },
    apply_permissions_to_settings,
    ipc::{
        IpcReceiver, IpcSender, NetworkFeedback, ServerIpcMessage, StreamerConfig,
        StreamerIpcMessage, create_process_ipc,
    },
};
use moonlight_common::{
    MoonlightError, ServerVersion,
    crypto::openssl::OpenSSLCryptoBackend,
    high::{MoonlightClientError, StreamConfigError, tokio::MoonlightHost},
    http::{
        ClientIdentifier, ClientSecret, ServerIdentifier, client::tokio_hyper::TokioHyperClient,
    },
    stream::{
        AesIv, AesKey, EncryptionFlags, HostFeatures, MoonlightStreamSettings, StreamingConfig,
        audio::{AudioConfig, OpusMultistreamConfig},
        c::{
            MoonlightInstance, MoonlightStream,
            bindings::{ConnectionStatus, Stage},
            connection::ConnectionListenerC,
        },
        connection::ConnectionListener,
        control::{
            ActiveGamepads, ControllerButtons, ControllerCapabilities, ControllerType, KeyAction,
            KeyFlags, KeyModifiers, MouseButton, MouseButtonAction, TouchEventType,
        },
        video::{
            ColorRange, ColorSpace, ServerCodecModeSupport, SunshineHdrMetadata, VideoFormat,
            VideoFormats, VideoSetup,
        },
    },
};
use tokio::{
    io::{stdin, stdout},
    runtime::Handle,
    spawn,
    sync::{Mutex, Notify, RwLock, mpsc, oneshot, watch},
    task::spawn_blocking,
    time::{sleep, timeout},
};
use tracing::{Level, level_filters::LevelFilter, span};
use tracing::{debug, error, info, trace, warn};

use common::api_bindings::{StreamCapabilities, StreamServerMessage, StreamerStatsUpdate};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    adaptive_bitrate::{AdaptiveBitrateController, FeedbackSample},
    audio::StreamAudioDecoder,
    transport::{
        InboundPacket, OutboundPacket, TransportError, TransportEvent, TransportEvents,
        TransportSender, web_socket,
        webrtc::{self},
    },
    video::StreamVideoDecoder,
};

pub type RequestClient = TokioHyperClient;

pub const TIMEOUT_DURATION: Duration = Duration::from_secs(10);
const TRANSPORT_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const NATIVE_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const NATIVE_START_TIMEOUT: Duration = Duration::from_secs(10);
const IPC_STOP_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(500);
// Native Moonlight invokes audio on its receive thread. Never make that thread
// wait for Tokio or transport locks: doing so fills Moonlight's fixed packet
// queue and loses a burst of stateful Opus frames.
const AUDIO_DISPATCH_QUEUE_CAPACITY: usize = 8;

mod adaptive_bitrate;
mod audio;
mod buffer;
mod convert;
mod transport;
mod video;

#[tokio::main]
async fn main() {
    let default_panic = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_panic(info);
        exit(0);
    }));

    // At this point we're authenticated
    let span = span!(Level::TRACE, "ipc");
    let (ipc_sender, mut ipc_receiver) =
        create_process_ipc::<ServerIpcMessage, StreamerIpcMessage>(span, stdin(), stdout()).await;

    // Send stage
    ipc_sender
        .send(StreamerIpcMessage::WebSocket(
            StreamServerMessage::DebugLog {
                message: "Completed Stage: Launch Streamer".to_string(),
                ty: None,
            },
        ))
        .await;

    let (
        config,
        host_address,
        host_http_port,
        client_unique_id,
        client_private_key,
        client_certificate,
        server_certificate,
        app_id,
        video_frame_queue_size,
        audio_sample_queue_size,
        permissions,
    ) = loop {
        match ipc_receiver.recv().await {
            Some(ServerIpcMessage::Init {
                config,
                host_address,
                host_http_port,
                client_unique_id,
                client_private_key,
                client_certificate,
                server_certificate,
                app_id,
                video_frame_queue_size,
                audio_sample_queue_size,
                permissions,
            }) => {
                break (
                    config,
                    host_address,
                    host_http_port,
                    client_unique_id,
                    client_private_key,
                    client_certificate,
                    server_certificate,
                    app_id,
                    video_frame_queue_size,
                    audio_sample_queue_size,
                    permissions,
                );
            }
            Some(ServerIpcMessage::Stop) | None => return,
            Some(_) => continue,
        }
    };

    // -- Init logger
    let config_level_filter = match config.log_level {
        log::LevelFilter::Off => LevelFilter::OFF,
        log::LevelFilter::Error => LevelFilter::ERROR,
        log::LevelFilter::Info => LevelFilter::INFO,
        log::LevelFilter::Warn => LevelFilter::WARN,
        log::LevelFilter::Debug => LevelFilter::DEBUG,
        log::LevelFilter::Trace => LevelFilter::TRACE,
    };

    let env_filter = EnvFilter::builder()
        .with_default_directive(config_level_filter.into())
        .from_env_lossy()
        .add_directive(
            "webrtc_sctp=off"
                .parse()
                .expect("failed to parse webrtc directive"),
        );

    let stderr_output = fmt::layer().with_writer(io::stderr).with_ansi(false);

    Registry::default()
        .with(env_filter)
        .with(stderr_output)
        .init();

    // print permissions
    info!("Got Permissions: {permissions:?}");

    // Send stage
    ipc_sender
        .send(StreamerIpcMessage::WebSocket(
            StreamServerMessage::DebugLog {
                message: "Waiting for Transport to negotiate".to_string(),
                ty: None,
            },
        ))
        .await;

    // Windows resolves localhost to IPv6 first on many installations. Sunshine
    // commonly listens only on IPv4, and the refused IPv6 attempt can stall a
    // reconnect for seconds before the equivalent IPv4 loopback succeeds.
    let host_address = normalize_loopback_host(host_address);
    HOST_IS_LOCAL
        .set(host_is_local(&host_address))
        .ok();

    // -- Create the host and pair it
    let host = MoonlightHost::new(host_address, host_http_port, client_unique_id)
        .expect("failed to create host");

    host.set_identity(
        ClientIdentifier::from_pem(client_certificate),
        ClientSecret::from_pem(client_private_key),
        ServerIdentifier::from_pem(server_certificate),
    )
    .await
    .expect("failed to set pairing info");

    // -- Configure moonlight
    let moonlight = MoonlightInstance::global().expect("failed to find moonlight");

    // WebTransport/WebSocket startup must not wait on an unrelated remote ICE
    // script. The legacy explicit WebRTC path can still use configured static
    // ICE servers, but this low-latency fork deliberately skips dynamic ICE.
    let ice_servers = config.webrtc.ice_servers.clone();

    let connection = StreamConnection::new(
        moonlight,
        StreamInfo { host, app_id },
        ipc_sender.clone(),
        ipc_receiver,
        config,
        video_frame_queue_size,
        audio_sample_queue_size,
        permissions,
    )
    .await
    .expect("failed to create connection");

    // Send Info for streamer
    ipc_sender
        .send(StreamerIpcMessage::WebSocket(StreamServerMessage::Setup {
            ice_servers,
        }))
        .await;

    // Wait for termination
    connection.terminate.notified().await;

    info!("Terminating Self");
    // Exit streamer
    exit(0);
}

struct StreamInfo {
    host: MoonlightHost<RequestClient>,
    app_id: u32,
}

struct StreamSetup {
    video: Option<VideoSetup>,
    audio: Option<OpusMultistreamConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct HeldKey {
    key: u16,
    modifiers: KeyModifiers,
    flags: KeyFlags,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct HeldTouch {
    pointer_id: u32,
    x: f32,
    y: f32,
    pressure_or_distance: f32,
    contact_area_major: f32,
    contact_area_minor: f32,
    rotation: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ControllerInputState {
    buttons: ControllerButtons,
    left_trigger: u8,
    right_trigger: u8,
    left_stick_x: i16,
    left_stick_y: i16,
    right_stick_x: i16,
    right_stick_y: i16,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ControllerDescriptor {
    ty: ControllerType,
    supported_buttons: ControllerButtons,
    capabilities: ControllerCapabilities,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct HeldInputSnapshot {
    // These vectors preserve the user's original press order. This matters for
    // combinations such as Ctrl+click when state is rebuilt on a new native
    // Moonlight connection.
    keys: Vec<HeldKey>,
    mouse_buttons: Vec<MouseButton>,
    touches: BTreeMap<u32, HeldTouch>,
    controller_descriptors: [Option<ControllerDescriptor>; 16],
    controllers: [Option<ControllerInputState>; 16],
}

impl HeldInputSnapshot {
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
            && self.mouse_buttons.is_empty()
            && self.touches.is_empty()
            && self.controller_descriptors.iter().all(Option::is_none)
            && self.controllers.iter().all(Option::is_none)
    }
}

#[derive(Debug, Default)]
struct InputActivityState {
    held: HeldInputSnapshot,
}

impl InputActivityState {
    fn record(&mut self, packet: &InboundPacket) {
        match packet {
            InboundPacket::Key {
                action,
                modifiers,
                key,
                flags,
            } => {
                // Each keyboard event carries the complete current modifier
                // mask. Refresh every held key so releasing Ctrl while C stays
                // down cannot replay C with a stale Ctrl modifier.
                for held in &mut self.held.keys {
                    held.modifiers = *modifiers;
                }
                match action {
                    KeyAction::Down => {
                        if let Some(held) = self.held.keys.iter_mut().find(|held| held.key == *key)
                        {
                            held.modifiers = *modifiers;
                            held.flags = *flags;
                        } else {
                            self.held.keys.push(HeldKey {
                                key: *key,
                                modifiers: *modifiers,
                                flags: *flags,
                            });
                        }
                    }
                    KeyAction::Up => {
                        self.held.keys.retain(|held| held.key != *key);
                    }
                }
            }
            InboundPacket::MouseButton { action, button } => match action {
                MouseButtonAction::Press => {
                    if !self.held.mouse_buttons.contains(button) {
                        self.held.mouse_buttons.push(*button);
                    }
                }
                MouseButtonAction::Release => {
                    self.held
                        .mouse_buttons
                        .retain(|held_button| held_button != button);
                }
            },
            InboundPacket::Touch {
                pointer_id,
                x,
                y,
                pressure_or_distance,
                contact_area_major,
                contact_area_minor,
                rotation,
                event_type,
            } => match event_type {
                TouchEventType::Down => {
                    self.held.touches.insert(
                        *pointer_id,
                        HeldTouch {
                            pointer_id: *pointer_id,
                            x: *x,
                            y: *y,
                            pressure_or_distance: *pressure_or_distance,
                            contact_area_major: *contact_area_major,
                            contact_area_minor: *contact_area_minor,
                            rotation: *rotation,
                        },
                    );
                }
                TouchEventType::Up | TouchEventType::Cancel => {
                    self.held.touches.remove(pointer_id);
                }
                TouchEventType::CancelAll => {
                    self.held.touches.clear();
                }
                TouchEventType::Move | TouchEventType::ButtonOnly => {
                    if let Some(touch) = self.held.touches.get_mut(pointer_id) {
                        touch.x = *x;
                        touch.y = *y;
                        touch.pressure_or_distance = *pressure_or_distance;
                        touch.contact_area_major = *contact_area_major;
                        touch.contact_area_minor = *contact_area_minor;
                        touch.rotation = *rotation;
                    }
                }
                TouchEventType::Hover | TouchEventType::HoverLeave => {}
            },
            InboundPacket::ControllerState {
                id,
                buttons,
                left_trigger,
                right_trigger,
                left_stick_x,
                left_stick_y,
                right_stick_x,
                right_stick_y,
            } => {
                if let Some(state) = self.held.controllers.get_mut(usize::from(*id)) {
                    *state = Some(ControllerInputState {
                        buttons: *buttons,
                        left_trigger: *left_trigger,
                        right_trigger: *right_trigger,
                        left_stick_x: *left_stick_x,
                        left_stick_y: *left_stick_y,
                        right_stick_x: *right_stick_x,
                        right_stick_y: *right_stick_y,
                    });
                }
            }
            InboundPacket::ControllerConnected {
                id,
                ty,
                supported_buttons,
                capabilities,
            } => {
                if let Some(descriptor) = self.held.controller_descriptors.get_mut(usize::from(*id))
                {
                    *descriptor = Some(ControllerDescriptor {
                        ty: *ty,
                        supported_buttons: *supported_buttons,
                        capabilities: *capabilities,
                    });
                }
            }
            InboundPacket::ControllerDisconnected { id } => {
                if let Some(descriptor) = self.held.controller_descriptors.get_mut(usize::from(*id))
                {
                    *descriptor = None;
                }
                if let Some(state) = self.held.controllers.get_mut(usize::from(*id)) {
                    *state = None;
                }
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> HeldInputSnapshot {
        self.held.clone()
    }
}

fn native_media_ready_for_transport(
    current_generation: bool,
    data_transport_media_gate: bool,
    ready_generation: u64,
    generation: u64,
) -> bool {
    current_generation && (!data_transport_media_gate || ready_generation == generation)
}

#[derive(Debug, Default)]
struct AdaptiveSession {
    generation: u64,
    settings: Option<StreamSettings>,
    controller: AdaptiveBitrateController,
}

struct AdaptiveRestartPendingGuard<'a> {
    pending: &'a AtomicBool,
}

/// Whether Sunshine runs on this machine or the LAN. moonlight-common's Auto
/// mode treats 127.0.0.1 as remote, which costs 500 Kbps of bitrate, disables
/// QoS (DSCP) tagging and can lower audio quality.
static HOST_IS_LOCAL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn host_is_local(host_address: &str) -> bool {
    match host_address.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback() || ip.is_unique_local(),
        Err(_) => false,
    }
}

fn normalize_loopback_host(host_address: String) -> String {
    if host_address.eq_ignore_ascii_case("localhost") {
        "127.0.0.1".to_owned()
    } else {
        host_address
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NativeStartExpectation {
    request_generation: u64,
    transport_generation: u64,
    require_adaptive_transport: bool,
}

impl NativeStartExpectation {
    fn browser(request_generation: u64, transport_generation: u64) -> Self {
        Self {
            request_generation,
            transport_generation,
            require_adaptive_transport: false,
        }
    }

    fn adaptive(request_generation: u64, transport_generation: u64) -> Self {
        Self {
            request_generation,
            transport_generation,
            require_adaptive_transport: true,
        }
    }

    fn is_current(self, connection: &StreamConnection) -> bool {
        native_start_expectation_matches(
            self,
            connection.is_terminating.load(Ordering::Acquire),
            connection.adaptive_transport_active.load(Ordering::Acquire),
            connection
                .native_start_request_generation
                .load(Ordering::Acquire),
            connection.transport_generation.load(Ordering::Acquire),
        )
    }
}

fn native_start_expectation_matches(
    expectation: NativeStartExpectation,
    terminating: bool,
    adaptive_transport_active: bool,
    request_generation: u64,
    transport_generation: u64,
) -> bool {
    !terminating
        && expectation.request_generation == request_generation
        && expectation.transport_generation == transport_generation
        && (!expectation.require_adaptive_transport || adaptive_transport_active)
}

fn native_callback_context_matches(
    callback_generation: u64,
    current_native_generation: u64,
    callback_expectation: NativeStartExpectation,
    registered_expectation: NativeStartExpectation,
    terminating: bool,
    adaptive_transport_active: bool,
    request_generation: u64,
    transport_generation: u64,
) -> bool {
    callback_generation == current_native_generation
        && callback_expectation == registered_expectation
        && native_start_expectation_matches(
            callback_expectation,
            terminating,
            adaptive_transport_active,
            request_generation,
            transport_generation,
        )
}

#[derive(Clone, Copy, Debug)]
enum NativeReplaceOutcome {
    Ready {
        configured_gamepads: ActiveGamepads,
        native_generation: u64,
    },
    Superseded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartStreamOutcome {
    Started,
    Superseded,
}

impl Drop for AdaptiveRestartPendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug)]
struct HostCapabilityCache {
    server_version: ServerVersion,
    server_gfe_version: String,
    server_codec_mode_support: ServerCodecModeSupport,
}

type SharedTransportSender = Arc<dyn TransportSender + Send + Sync + 'static>;

struct StreamConnection {
    pub runtime: Handle,
    pub moonlight: MoonlightInstance,
    pub config: StreamerConfig,
    pub info: StreamInfo,
    pub ipc_sender: IpcSender<StreamerIpcMessage>,
    pub permissions: StreamPermissions,
    // Video
    pub video_frame_queue_size: usize,
    pub audio_sample_queue_size: usize,
    pub stream_setup: Mutex<StreamSetup>,
    // Stream
    pub stream: RwLock<Option<MoonlightStream>>,
    native_lifecycle: Mutex<()>,
    native_generation: AtomicU64,
    native_media_ready_generation: AtomicU64,
    native_start_in_progress_generation: AtomicU64,
    native_expected_request_generation: AtomicU64,
    native_expected_transport_generation: AtomicU64,
    native_expected_adaptive_transport: AtomicBool,
    native_start_request_generation: AtomicU64,
    adaptive_session: Mutex<AdaptiveSession>,
    adaptive_restart_pending: AtomicBool,
    host_capabilities: Mutex<Option<HostCapabilityCache>>,
    adaptive_transport_active: AtomicBool,
    input_activity: Mutex<InputActivityState>,
    native_input_gate: RwLock<()>,
    // Native reconnect publishes its stream handle before input can safely be
    // replayed. While this is set, packets still refresh held state and the
    // controller mask but are not delivered to either native generation.
    native_input_replay_pending: AtomicBool,
    pub active_gamepads: RwLock<ActiveGamepads>,
    pub transport_sender: Mutex<Option<SharedTransportSender>>,
    pub(crate) audio_dispatch_tx: mpsc::Sender<(u64, Bytes)>,
    transport_dispatch: RwLock<()>,
    transport_generation: AtomicU64,
    transport_cancel: Mutex<Option<oneshot::Sender<()>>>,
    // Timeout / Terminate
    pub timeout_terminate_request: Mutex<Option<Instant>>,
    pub terminate: Notify,
    is_terminating: AtomicBool,
    termination_signal: watch::Sender<bool>,
}

impl StreamConnection {
    pub async fn new(
        moonlight: MoonlightInstance,
        info: StreamInfo,
        ipc_sender: IpcSender<StreamerIpcMessage>,
        mut ipc_receiver: IpcReceiver<ServerIpcMessage>,
        config: StreamerConfig,
        video_frame_queue_size: usize,
        audio_sample_queue_size: usize,
        permissions: StreamPermissions,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let (audio_dispatch_tx, mut audio_dispatch_rx) =
            mpsc::channel::<(u64, Bytes)>(AUDIO_DISPATCH_QUEUE_CAPACITY);
        let this = Arc::new(Self {
            runtime: Handle::current(),
            moonlight,
            config,
            info,
            ipc_sender,
            permissions,
            stream_setup: Mutex::new(StreamSetup {
                video: None,
                audio: None,
            }),
            video_frame_queue_size,
            audio_sample_queue_size,
            stream: RwLock::new(None),
            native_lifecycle: Mutex::new(()),
            native_generation: AtomicU64::new(0),
            native_media_ready_generation: AtomicU64::new(0),
            native_start_in_progress_generation: AtomicU64::new(0),
            native_expected_request_generation: AtomicU64::new(0),
            native_expected_transport_generation: AtomicU64::new(0),
            native_expected_adaptive_transport: AtomicBool::new(false),
            native_start_request_generation: AtomicU64::new(0),
            adaptive_session: Mutex::new(AdaptiveSession::default()),
            adaptive_restart_pending: AtomicBool::new(false),
            host_capabilities: Mutex::new(None),
            adaptive_transport_active: AtomicBool::new(false),
            input_activity: Mutex::new(InputActivityState::default()),
            native_input_gate: RwLock::new(()),
            native_input_replay_pending: AtomicBool::new(false),
            active_gamepads: RwLock::new(ActiveGamepads::empty()),
            transport_sender: Mutex::new(None),
            audio_dispatch_tx,
            transport_dispatch: RwLock::new(()),
            transport_generation: AtomicU64::new(0),
            transport_cancel: Mutex::new(None),
            timeout_terminate_request: Default::default(),
            terminate: Notify::default(),
            is_terminating: AtomicBool::new(false),
            termination_signal: watch::channel(false).0,
        });

        // Preserve audio ordering on one async task while keeping the native
        // callback entirely non-blocking. Generation checks discard packets
        // left behind by an adaptive reconnect before they reach a new stream.
        spawn({
            let this = Arc::downgrade(&this);
            async move {
                while let Some((generation, data)) = audio_dispatch_rx.recv().await {
                    let Some(this) = this.upgrade() else {
                        return;
                    };
                    if !this.is_native_media_ready(generation) {
                        continue;
                    }
                    let sender = this.transport_sender.lock().await.clone();
                    if !this.is_native_media_ready(generation) {
                        continue;
                    }
                    if let Some(sender) = sender
                        && let Err(err) = sender.send_audio_sample(&data).await
                    {
                        warn!("Failed to send audio sample: {err}");
                    }
                }
            }
        });

        spawn({
            let this = Arc::downgrade(&this);

            async move {
                loop {
                    let message = match ipc_receiver.recv().await {
                        Some(message) => message,
                        None => {
                            let Some(this) = this.upgrade() else {
                                return;
                            };

                            info!("Parent IPC closed; stopping streamer");
                            this.stop().await;
                            return;
                        }
                    };

                    let Some(this) = this.upgrade() else {
                        debug!("Received ipc message while the main type is already deallocated");
                        return;
                    };

                    if let ServerIpcMessage::Stop = &message {
                        this.on_ipc_message(ServerIpcMessage::Stop).await;
                        return;
                    }

                    this.on_ipc_message(message).await;
                }
            }
        });

        Ok(this)
    }

    async fn set_transport(
        self: &Arc<Self>,
        new_sender: Box<dyn TransportSender + Send + Sync + 'static>,
        mut events: Box<dyn TransportEvents + Send + Sync + 'static>,
        closed_event_is_final: bool,
        data_transport_media_gate: bool,
    ) {
        let new_sender: SharedTransportSender = new_sender.into();
        let this = self.clone();
        // Transport generation changes and event dispatch share this gate. An
        // event that began on the old generation must finish before handoff;
        // after handoff, stale tasks cannot pass the read-side generation check.
        let dispatch_guard = this.transport_dispatch.write().await;
        let generation = this
            .transport_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);

        // Stop the previous event task before its sender is closed. Without an
        // explicit cancellation, the old task can observe Closed after the new
        // transport starts and arm the delayed stream-termination timer.
        let (cancel_sender, mut cancel_receiver) = oneshot::channel();
        if let Some(old_cancel) = this.transport_cancel.lock().await.replace(cancel_sender) {
            let _ = old_cancel.send(());
        }

        let old_transport = {
            let mut sender = this.transport_sender.lock().await;
            sender.replace(new_sender)
        };
        // Publish this while the transport dispatch write gate is held. A new
        // transport cannot emit StartStream until both its generation and its
        // media-gating mode describe the same installed sender.
        this.adaptive_transport_active
            .store(data_transport_media_gate, Ordering::Release);
        this.clear_terminate_request().await;

        spawn({
            let ipc_sender = this.ipc_sender.clone();
            let this = Arc::downgrade(&this);

            async move {
                loop {
                    trace!("Polling new transport event");
                    let event = tokio::select! {
                        _ = &mut cancel_receiver => {
                            debug!(generation, "Transport event task was replaced");
                            return;
                        }
                        event = events.poll_event() => event,
                    };
                    trace!("Polled transport event");

                    let Some(connection) = this.upgrade() else {
                        warn!("Failed to get stream connection, stopping listening to events");
                        return;
                    };
                    let dispatch_guard = connection.transport_dispatch.read().await;
                    if connection.transport_generation.load(Ordering::Acquire) != generation {
                        debug!(generation, "Ignoring event from stale transport generation");
                        return;
                    }

                    match event {
                        Ok(TransportEvent::SendIpc(message)) => {
                            ipc_sender.send(message).await;
                        }
                        Ok(TransportEvent::StartStream { settings }) => {
                            let request_generation = connection
                                .native_start_request_generation
                                .fetch_add(1, Ordering::AcqRel)
                                .wrapping_add(1);
                            let expectation =
                                NativeStartExpectation::browser(request_generation, generation);
                            let this = connection.clone();
                            spawn(async move {
                                // Validate against handoff under the dispatch
                                // gate, then release it before native startup.
                                // A slow Sunshine reconnect must not stop the
                                // transport event loop or a replacement sender.
                                let dispatch_guard = this.transport_dispatch.read().await;
                                if this.transport_generation.load(Ordering::Acquire) != generation {
                                    return;
                                }
                                drop(dispatch_guard);
                                this.clear_terminate_request().await;

                                match this.start_stream(settings, expectation).await {
                                    Ok(StartStreamOutcome::Started) => {}
                                    Ok(StartStreamOutcome::Superseded) => {
                                        debug!(
                                            request_generation,
                                            generation,
                                            "Discarded superseded stream start without stopping its replacement"
                                        );
                                    }
                                    Err(err) => {
                                        this.stop_failed_start_if_current(expectation, err).await;
                                    }
                                }
                            });
                        }
                        Ok(TransportEvent::RecvPacket(packet)) => {
                            if matches!(
                                &packet,
                                InboundPacket::General {
                                    message: GeneralClientMessage::Stop
                                }
                            ) {
                                // Do not wait for native_lifecycle while holding
                                // a dispatch reader. Native publication takes a
                                // short write gate for its final generation check.
                                drop(dispatch_guard);
                            }
                            connection.on_packet(packet).await;
                        }
                        Err(TransportError::Closed) => {
                            drop(dispatch_guard);
                            connection.request_terminate().await;

                            break;
                        }
                        Ok(TransportEvent::Closed) => {
                            drop(dispatch_guard);
                            if closed_event_is_final {
                                connection.stop().await;
                            } else {
                                connection.request_terminate().await;
                            }

                            break;
                        }
                        // It wouldn't make sense to return this
                        Err(TransportError::ChannelClosed) => unreachable!(),
                        Err(TransportError::Implementation(err)) => {
                            info!(
                                "Stopping stream because of transport implementation error: {err}"
                            );

                            drop(dispatch_guard);
                            connection.stop().await;
                            break;
                        }
                    }
                }
            }
        });

        drop(dispatch_guard);

        if let Some(old_transport) = old_transport {
            spawn(async move {
                if let Err(err) = old_transport.close().await {
                    warn!("Failed to close old transport: {err:?}");
                }
            });
        }
    }
    async fn try_send_packet(&self, packet: OutboundPacket, packet_ty: &str, should_warn: bool) {
        // Transport implementations are internally synchronized. Clone the
        // active handle so a backpressured send cannot block setup, shutdown,
        // or native audio/video callbacks from acquiring this short-lived lock.
        let sender = self.transport_sender.lock().await.clone();

        if let Some(sender) = sender {
            if let Err(err) = sender.send(packet).await {
                if should_warn {
                    warn!("Failed to send outbound packet: {packet_ty}, {err:?}");
                } else {
                    debug!("Failed to send outbound packet: {packet_ty}, {err:?}");
                }
            }
        } else {
            debug!("Dropping packet {packet:?} because no transport is selected!");
        }
    }

    async fn try_send_native_callback_packet(
        &self,
        generation: u64,
        expectation: NativeStartExpectation,
        packet: OutboundPacket,
        packet_ty: &str,
        should_warn: bool,
    ) {
        // Capture first. If transport handoff wins before the checks below, the
        // expectation is stale and this packet is discarded. If handoff wins
        // afterward, async backpressure remains bound to this old captured
        // handle and can never spill the callback into the replacement sender.
        let sender = self.transport_sender.lock().await.clone();
        if !self.native_callback_context_current(generation, expectation) {
            trace!(
                generation,
                packet_ty, "Dropping stale native callback packet"
            );
            return;
        }

        if let Some(sender) = sender {
            if let Err(err) = sender.send(packet).await {
                if should_warn {
                    warn!("Failed to send native callback packet: {packet_ty}, {err:?}");
                } else {
                    debug!("Failed to send native callback packet: {packet_ty}, {err:?}");
                }
            }
        } else {
            debug!("Dropping native callback packet {packet:?} because no transport is selected!");
        }
    }

    async fn await_lifecycle_io<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, anyhow::Error>
    where
        F: Future<Output = T>,
    {
        let mut termination = self.termination_signal.subscribe();
        if *termination.borrow() {
            anyhow::bail!("{operation} cancelled because the streamer is terminating");
        }

        tokio::select! {
            biased;
            changed = termination.changed() => {
                let _ = changed;
                anyhow::bail!("{operation} cancelled because the streamer is terminating")
            }
            result = timeout(TIMEOUT_DURATION, future) => {
                result.map_err(|_| anyhow::anyhow!("{operation} exceeded its {:?} deadline", TIMEOUT_DURATION))
            }
        }
    }

    async fn on_packet(&self, packet: InboundPacket) {
        if matches!(
            &packet,
            InboundPacket::General {
                message: GeneralClientMessage::Stop
            }
        ) {
            debug!("Received stop from client. Stopping stream now!");
            self.stop().await;
            return;
        }

        // The write side is held only for the two atomic boundaries around a
        // reconnect. Packets continue to enter here during the slow native
        // stop/start so releases and the newest analog/touch state are kept.
        let _input_gate = self.native_input_gate.read().await;
        self.input_activity.lock().await.record(&packet);
        let updated_controller_gamepads = match &packet {
            InboundPacket::ControllerConnected { id, .. } => {
                let Some(gamepad) = ActiveGamepads::from_id(*id) else {
                    warn!(id, "Failed to add gamepad because it is out of range");
                    return;
                };
                let mut active_gamepads = self.active_gamepads.write().await;
                active_gamepads.insert(gamepad);
                Some(*active_gamepads)
            }
            InboundPacket::ControllerDisconnected { id } => {
                let Some(gamepad) = ActiveGamepads::from_id(*id) else {
                    warn!(id, "Failed to remove gamepad because it is out of range");
                    return;
                };
                let mut active_gamepads = self.active_gamepads.write().await;
                active_gamepads.remove(gamepad);
                Some(*active_gamepads)
            }
            _ => None,
        };
        trace!(packet = packet.kind(), "received packet from client");

        // The replacement handle is published before replay completes. Do not
        // let a newer packet overtake the final held-state replay. Transient
        // movement, RTT, IDR, scroll, and text work is intentionally discarded
        // here; stateful input was already refreshed above.
        if self.native_input_replay_pending.load(Ordering::Acquire) {
            trace!(
                packet = packet.kind(),
                "Retained state but skipped native dispatch during reconnect"
            );
            return;
        }

        let stream_lock = self.stream.read().await;
        let Some(stream) = stream_lock.as_ref() else {
            warn!("Failed to send packet {packet:?} because of missing stream");
            return;
        };

        let err = match packet {
            InboundPacket::General { message } => {
                debug!("General message: {message:?}");

                // currently there are no packets associated with that
                match message {
                    GeneralClientMessage::Stop => {
                        debug!("Received stop from client. Stopping stream now!");

                        drop(stream_lock);

                        self.stop().await;

                        None
                    }
                }
            }
            InboundPacket::MousePosition {
                x,
                y,
                reference_width,
                reference_height,
            } => stream
                .send_mouse_position(x, y, reference_width, reference_height)
                .err(),
            InboundPacket::MouseButton { action, button } => {
                stream.send_mouse_button(action, button).err()
            }
            InboundPacket::MouseMove { delta_x, delta_y } => {
                stream.send_mouse_move(delta_x, delta_y).err()
            }
            InboundPacket::HighResScroll { delta_x, delta_y } => {
                let mut err = None;
                if delta_y != 0 {
                    err = stream.send_high_res_scroll(delta_y).err()
                }
                if delta_x != 0 {
                    err = stream.send_high_res_horizontal_scroll(delta_x).err()
                }
                err
            }
            InboundPacket::Scroll { delta_x, delta_y } => {
                let mut err = None;
                if delta_y != 0 {
                    err = stream.send_scroll(delta_y).err();
                }
                if delta_x != 0 {
                    err = stream.send_horizontal_scroll(delta_x).err();
                }
                err
            }
            InboundPacket::Key {
                action,
                modifiers,
                key,
                flags,
            } => stream
                .send_keyboard_event_non_standard(key as i16, action, modifiers, flags)
                .err(),
            InboundPacket::Text { text } => stream.send_text(&text).err(),
            InboundPacket::Touch {
                pointer_id,
                x,
                y,
                pressure_or_distance,
                contact_area_major,
                contact_area_minor,
                rotation,
                event_type,
            } => stream
                .send_touch(
                    pointer_id,
                    x,
                    y,
                    pressure_or_distance,
                    contact_area_major,
                    contact_area_minor,
                    rotation,
                    event_type,
                )
                .err(),
            InboundPacket::ControllerConnected {
                id,
                ty,
                supported_buttons,
                capabilities,
            } => {
                let Some(active_gamepads) = updated_controller_gamepads else {
                    return;
                };
                stream
                    .send_controller_arrival(
                        id,
                        active_gamepads,
                        ty,
                        supported_buttons,
                        capabilities,
                    )
                    .err()
            }
            InboundPacket::ControllerDisconnected { id } => {
                let Some(active_gamepads) = updated_controller_gamepads else {
                    return;
                };
                stream
                    .send_multi_controller(
                        id,
                        active_gamepads,
                        ControllerButtons::empty(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    )
                    .err()
            }
            InboundPacket::ControllerState {
                id,
                buttons,
                left_trigger,
                right_trigger,
                left_stick_x,
                left_stick_y,
                right_stick_x,
                right_stick_y,
            } => {
                let Some(gamepad) = ActiveGamepads::from_id(id) else {
                    warn!("Failed to update gamepad state because it is out of range: {id}");
                    return;
                };

                let active_gamepads = self.active_gamepads.read().await;
                if !active_gamepads.contains(gamepad) {
                    warn!(
                        "Failed to send gamepad event for not registered gamepad, gamepad: {id}, currently active: {:?}",
                        *active_gamepads
                    );
                    return;
                }

                stream
                    .send_multi_controller(
                        id,
                        *active_gamepads,
                        buttons,
                        left_trigger,
                        right_trigger,
                        left_stick_x,
                        left_stick_y,
                        right_stick_x,
                        right_stick_y,
                    )
                    .err()
            }
            _ => None,
        };

        if let Some(err) = err {
            warn!("Failed to handle packet: {err:?}");
        }
    }

    async fn on_ipc_message(self: &Arc<StreamConnection>, mut message: ServerIpcMessage) {
        // Congestion telemetry is streamer-internal. Never forward it to the
        // browser transport where it could contend with input or be reflected
        // back to the server.
        if let ServerIpcMessage::NetworkFeedback(feedback) = &message {
            self.on_network_feedback(*feedback).await;
            return;
        }

        match &mut message {
            ServerIpcMessage::WebSocket(StreamClientMessage::StartStream { settings }) => {
                // Apply restrictions
                apply_permissions_to_settings(&self.permissions, settings);

                info!("Applied permissions to settings");
            }
            ServerIpcMessage::WebSocket(StreamClientMessage::SetTransport(transport_type)) => {
                self.clear_terminate_request().await;

                match transport_type {
                    TransportType::WebRTC if self.permissions.allow_transport_webrtc => {
                        info!("Trying WebRTC transport");

                        let (sender, events) = match webrtc::new(
                            &self.config.webrtc,
                            self.video_frame_queue_size,
                            self.audio_sample_queue_size,
                        )
                        .await
                        {
                            Ok(value) => value,
                            Err(err) => {
                                error!("Failed to start webrtc transport: {err}");
                                return;
                            }
                        };
                        // WebRTC only emits Closed for a failed/disconnected
                        // peer after its own recovery grace has elapsed. Treat
                        // that event as final so shutdown does not wait through
                        // the same grace period a second time.
                        self.set_transport(Box::new(sender), Box::new(events), true, false)
                            .await;
                    }
                    TransportType::WebSocket | TransportType::WebTransport
                        if self.permissions.allow_transport_websockets =>
                    {
                        info!("Trying browser relay transport");

                        let (sender, events) = match web_socket::new(self.ipc_sender.clone()).await
                        {
                            Ok(value) => value,
                            Err(err) => {
                                error!("Failed to start web socket transport: {err}");
                                return;
                            }
                        };
                        // Cancel any decision made for the previous transport
                        // generation before installing this one. Feedback is
                        // enabled only after handoff completes.
                        self.set_transport(Box::new(sender), Box::new(events), false, true)
                            .await;
                    }
                    transport => {
                        warn!(
                            "Client tried to select {transport:?}, but it was specifically disabled in the permissions -> ignoring request."
                        );
                    }
                }
            }
            ServerIpcMessage::Stop => {
                self.stop().await;
            }
            _ => {}
        }

        let sender = self.transport_sender.lock().await.clone();
        if let Some(sender) = sender {
            if let Err(err) = sender.on_ipc_message(message).await {
                warn!("Failed to send ipc message: {err}");
            }
        } else {
            warn!("Failed to process ipc message because of missing transport: {message:?}");
        }
    }

    async fn on_network_feedback(self: &Arc<Self>, feedback: NetworkFeedback) {
        if self.is_terminating.load(Ordering::Acquire) {
            return;
        }
        // Surface the QUIC path RTT next to the application-level browser RTT
        // so a jumping number can be attributed to the network or the browser.
        if feedback.rtt_ms > 0 {
            self.try_send_packet(
                OutboundPacket::Stats(StreamerStatsUpdate::TransportRtt {
                    rtt_ms: f64::from(feedback.rtt_ms),
                }),
                "transport rtt stats",
                false,
            )
            .await;
        }
        if !self.adaptive_transport_active.load(Ordering::Acquire) {
            return;
        }

        let feedback = FeedbackSample {
            rtt_ms: feedback.rtt_ms,
            sent_packets: feedback.sent_packets,
            lost_packets: feedback.lost_packets,
            congestion_events: feedback.congestion_events,
            admission_drops: feedback.admission_drops,
            video_write_timeouts: feedback.video_write_timeouts,
            recovery_requests: feedback.recovery_requests,
        };

        let transport_generation = self.transport_generation.load(Ordering::Acquire);
        let restart = {
            let mut adaptive = self.adaptive_session.lock().await;
            if adaptive.settings.is_none() {
                return;
            }
            let now = Instant::now();
            if self.adaptive_restart_pending.load(Ordering::Acquire) {
                adaptive
                    .controller
                    .observe_while_restart_pending(feedback, now);
                return;
            }
            let generation = adaptive.generation;
            adaptive.controller.observe(feedback, now).map(|reduction| {
                // The controller commits its target when it returns a
                // reduction. Publish the pending marker before releasing the
                // controller lock so another report cannot stack a second one.
                self.adaptive_restart_pending.store(true, Ordering::Release);
                (
                    generation,
                    self.native_start_request_generation.load(Ordering::Acquire),
                    transport_generation,
                    reduction,
                )
            })
        };

        let Some((session_generation, start_request_generation, transport_generation, reduction)) =
            restart
        else {
            return;
        };

        info!(
            from_kbps = reduction.previous_bitrate_kbps,
            to_kbps = reduction.bitrate_kbps,
            "Sustained browser-path congestion triggered an adaptive bitrate restart"
        );
        let this = self.clone();
        spawn(async move {
            this.restart_with_adaptive_bitrate(
                session_generation,
                start_request_generation,
                transport_generation,
                reduction,
            )
            .await;
        });
    }

    async fn restart_with_adaptive_bitrate(
        self: &Arc<Self>,
        session_generation: u64,
        start_request_generation: u64,
        transport_generation: u64,
        reduction: adaptive_bitrate::BitrateReduction,
    ) {
        let _pending_guard = AdaptiveRestartPendingGuard {
            pending: &self.adaptive_restart_pending,
        };
        let expectation =
            NativeStartExpectation::adaptive(start_request_generation, transport_generation);
        if !self.adaptive_restart_context_current(expectation) {
            self.rollback_pending_adaptive_reduction(session_generation, reduction)
                .await;
            return;
        }

        let lifecycle_guard = self.native_lifecycle.lock().await;
        if !self.adaptive_restart_context_current(expectation) {
            drop(lifecycle_guard);
            self.rollback_pending_adaptive_reduction(session_generation, reduction)
                .await;
            debug!(
                session_generation,
                "Discarding adaptive restart after its stream or transport was superseded"
            );
            return;
        }

        let settings = {
            let adaptive = self.adaptive_session.lock().await;
            if adaptive.generation != session_generation
                || adaptive.controller.current_bitrate_kbps() != reduction.bitrate_kbps
            {
                None
            } else {
                adaptive.settings.clone()
            }
        };
        let Some(settings) = settings else {
            drop(lifecycle_guard);
            self.rollback_pending_adaptive_reduction(session_generation, reduction)
                .await;
            debug!(
                session_generation,
                "Discarding stale adaptive bitrate restart"
            );
            return;
        };
        let mut reduced_settings = settings.clone();
        let mut rollback_settings = settings;
        reduced_settings.bitrate_kbps = reduction.bitrate_kbps;
        rollback_settings.bitrate_kbps = reduction.previous_bitrate_kbps;

        // Establish the replay boundary after all existing input readers have
        // drained, then release it immediately. During the slow native work,
        // on_packet continues consuming transport input and refreshing held
        // state while suppressing dispatch to either native generation.
        self.pause_native_input_for_replay().await;

        let (applied_bitrate_kbps, configured_gamepads, native_generation) = match self
            .replace_native_stream_locked(reduced_settings, false, expectation)
            .await
        {
            Ok(NativeReplaceOutcome::Ready {
                configured_gamepads,
                native_generation,
            }) => (
                reduction.bitrate_kbps,
                configured_gamepads,
                native_generation,
            ),
            Ok(NativeReplaceOutcome::Superseded) => {
                self.resume_native_input_without_replay().await;
                drop(lifecycle_guard);
                self.rollback_pending_adaptive_reduction(session_generation, reduction)
                    .await;
                return;
            }
            Err(restart_error) => {
                if !expectation.is_current(self) {
                    let _ = self.discard_in_progress_native_start_locked().await;
                    self.resume_native_input_without_replay().await;
                    drop(lifecycle_guard);
                    self.rollback_pending_adaptive_reduction(session_generation, reduction)
                        .await;
                    return;
                }
                if self.is_terminating.load(Ordering::Acquire) {
                    return;
                }
                warn!(
                    "Adaptive bitrate reconnect at {} Kbps failed: {restart_error:#}; trying {} Kbps rollback",
                    reduction.bitrate_kbps, reduction.previous_bitrate_kbps
                );
                let mut adaptive = self.adaptive_session.lock().await;
                if adaptive.generation != session_generation {
                    drop(adaptive);
                    self.resume_native_input_without_replay().await;
                    drop(lifecycle_guard);
                    return;
                }
                adaptive
                    .controller
                    .rollback(reduction.previous_bitrate_kbps);
                drop(adaptive);

                match self
                    .replace_native_stream_locked(rollback_settings, false, expectation)
                    .await
                {
                    Ok(NativeReplaceOutcome::Ready {
                        configured_gamepads,
                        native_generation,
                    }) => (
                        reduction.previous_bitrate_kbps,
                        configured_gamepads,
                        native_generation,
                    ),
                    Ok(NativeReplaceOutcome::Superseded) => {
                        self.resume_native_input_without_replay().await;
                        drop(lifecycle_guard);
                        return;
                    }
                    Err(rollback_error) => {
                        if !expectation.is_current(self) {
                            let _ = self.discard_in_progress_native_start_locked().await;
                            self.resume_native_input_without_replay().await;
                            drop(lifecycle_guard);
                            return;
                        }
                        error!(
                            "Adaptive bitrate rollback failed after reconnect error: {rollback_error:#}"
                        );
                        if self.begin_termination_for_expected_start(expectation).await {
                            self.finish_stop_locked().await;
                        } else if !self.is_terminating.load(Ordering::Acquire) {
                            let _ = self.discard_in_progress_native_start_locked().await;
                            self.resume_native_input_without_replay().await;
                        }
                        drop(lifecycle_guard);
                        return;
                    }
                }
            }
        };

        // Take the write side one final time. It atomically snapshots/replays
        // the freshest state and clears replay_pending before queued packets
        // can resume, so no release can be overtaken by stale replay data.
        match self
            .replay_latest_input_and_resume(configured_gamepads, native_generation, expectation)
            .await
        {
            Ok(StartStreamOutcome::Started) => {}
            Ok(StartStreamOutcome::Superseded) => {
                drop(lifecycle_guard);
                self.rollback_pending_adaptive_reduction(session_generation, reduction)
                    .await;
                return;
            }
            Err(replay_error) => {
                error!("Failed to restore input after adaptive reconnect: {replay_error:#}");
                if self.begin_termination_for_expected_start(expectation).await {
                    self.finish_stop_locked().await;
                } else if !self.is_terminating.load(Ordering::Acquire) {
                    let _ = self
                        .discard_native_generation_locked(native_generation)
                        .await;
                    self.resume_native_input_without_replay().await;
                }
                drop(lifecycle_guard);
                return;
            }
        }

        drop(lifecycle_guard);
        let dispatch_guard = self.transport_dispatch.write().await;
        if !self.adaptive_restart_context_current(expectation) {
            return;
        }
        let status = StreamerIpcMessage::WebSocket(StreamServerMessage::DebugLog {
            message: if applied_bitrate_kbps == reduction.bitrate_kbps {
                format!(
                    "Adaptive bitrate adjusted the stream from {} to {} Kbps for network stability",
                    reduction.previous_bitrate_kbps, reduction.bitrate_kbps
                )
            } else {
                format!(
                    "Adaptive bitrate reconnect failed; restored the previous {} Kbps stream",
                    reduction.previous_bitrate_kbps
                )
            },
            ty: None,
        });
        if self.ipc_sender.try_send(status).is_err() {
            debug!("Adaptive bitrate status announcement was backpressured");
        }
        drop(dispatch_guard);
    }

    fn adaptive_restart_context_current(&self, expectation: NativeStartExpectation) -> bool {
        expectation.require_adaptive_transport && expectation.is_current(self)
    }

    async fn rollback_pending_adaptive_reduction(
        &self,
        session_generation: u64,
        reduction: adaptive_bitrate::BitrateReduction,
    ) {
        let mut adaptive = self.adaptive_session.lock().await;
        if adaptive.generation == session_generation {
            adaptive.controller.cancel_reduction(reduction);
        }
    }

    // Start Moonlight Stream. Browser-initiated starts reset the adaptive
    // session and are serialized with congestion-triggered reconnects.
    async fn start_stream(
        self: &Arc<Self>,
        settings: StreamSettings,
        expectation: NativeStartExpectation,
    ) -> Result<StartStreamOutcome, anyhow::Error> {
        let _lifecycle_guard = self.native_lifecycle.lock().await;
        if !expectation.is_current(self) {
            debug!(
                request_generation = expectation.request_generation,
                transport_generation = expectation.transport_generation,
                "Coalescing superseded stream start request"
            );
            return Ok(StartStreamOutcome::Superseded);
        }

        {
            let mut adaptive = self.adaptive_session.lock().await;
            adaptive.generation = adaptive.generation.wrapping_add(1);
            adaptive.controller.reset(
                settings.adaptive_bitrate,
                settings.bitrate_kbps,
                settings.minimum_bitrate_kbps,
                Instant::now(),
            );
            adaptive.settings = Some(settings.clone());
        }

        self.pause_native_input_for_replay().await;
        let (configured_gamepads, native_generation) = match self
            .replace_native_stream_locked(settings, true, expectation)
            .await
        {
            Ok(NativeReplaceOutcome::Ready {
                configured_gamepads,
                native_generation,
            }) => (configured_gamepads, native_generation),
            Ok(NativeReplaceOutcome::Superseded) => {
                self.resume_native_input_without_replay().await;
                return Ok(StartStreamOutcome::Superseded);
            }
            Err(err) => {
                if !expectation.is_current(self) {
                    let _ = self.discard_in_progress_native_start_locked().await;
                    self.resume_native_input_without_replay().await;
                    return Ok(StartStreamOutcome::Superseded);
                }
                if !self.is_terminating.load(Ordering::Acquire) {
                    self.resume_native_input_without_replay().await;
                }
                return Err(err);
            }
        };
        self.replay_latest_input_and_resume(configured_gamepads, native_generation, expectation)
            .await
    }

    async fn pause_native_input_for_replay(&self) {
        let _input_guard = self.native_input_gate.write().await;
        self.native_input_replay_pending
            .store(true, Ordering::Release);
    }

    async fn resume_native_input_without_replay(&self) {
        let _input_guard = self.native_input_gate.write().await;
        self.native_input_replay_pending
            .store(false, Ordering::Release);
    }

    async fn replay_latest_input_and_resume(
        &self,
        configured_gamepads: ActiveGamepads,
        native_generation: u64,
        expectation: NativeStartExpectation,
    ) -> Result<StartStreamOutcome, anyhow::Error> {
        let (replay_result, superseded) = {
            let _input_guard = self.native_input_gate.write().await;
            let current_before_replay = expectation.is_current(self)
                && self.native_generation.load(Ordering::Acquire) == native_generation;
            let replay_result = if current_before_replay {
                let held_input = self.input_activity.lock().await.snapshot();
                let active_gamepads = *self.active_gamepads.read().await;
                self.replay_held_input_locked(&held_input, active_gamepads, configured_gamepads)
                    .await
            } else {
                Ok(())
            };
            let superseded = !current_before_replay
                || !expectation.is_current(self)
                || self.native_generation.load(Ordering::Acquire) != native_generation;
            // Clear while the writer is still held. Packets queued behind this
            // boundary can never observe replay_pending from the obsolete task.
            self.native_input_replay_pending
                .store(false, Ordering::Release);
            (replay_result, superseded)
        };

        if superseded {
            self.discard_native_generation_locked(native_generation)
                .await?;
            return Ok(StartStreamOutcome::Superseded);
        }
        replay_result?;
        Ok(StartStreamOutcome::Started)
    }

    async fn replay_held_input_locked(
        &self,
        held_input: &HeldInputSnapshot,
        active_gamepads: ActiveGamepads,
        configured_gamepads: ActiveGamepads,
    ) -> Result<(), anyhow::Error> {
        if held_input.is_empty() && active_gamepads == configured_gamepads {
            return Ok(());
        }

        let stream = self.stream.read().await;
        let stream = stream
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("native stream disappeared before input replay"))?;

        // A controller can disconnect after MoonlightStreamSettings captures
        // its attached mask. Send the same neutral state used by the live
        // disconnect path so the new native stream sees the freshest mask.
        for id in 0..16 {
            let Some(gamepad) = ActiveGamepads::from_id(id) else {
                continue;
            };
            if configured_gamepads.contains(gamepad) && !active_gamepads.contains(gamepad) {
                if let Err(err) = stream.send_multi_controller(
                    id,
                    active_gamepads,
                    ControllerButtons::empty(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ) {
                    warn!(id, "Failed to replay controller disconnection: {err:?}");
                }
            }
        }

        for key in &held_input.keys {
            if let Err(err) = stream.send_keyboard_event_non_standard(
                key.key as i16,
                KeyAction::Down,
                key.modifiers,
                key.flags,
            ) {
                warn!(key = key.key, "Failed to replay held key: {err:?}");
            }
        }
        for button in &held_input.mouse_buttons {
            if let Err(err) = stream.send_mouse_button(MouseButtonAction::Press, *button) {
                warn!(?button, "Failed to replay held mouse button: {err:?}");
            }
        }
        for touch in held_input.touches.values() {
            if let Err(err) = stream.send_touch(
                touch.pointer_id,
                touch.x,
                touch.y,
                touch.pressure_or_distance,
                touch.contact_area_major,
                touch.contact_area_minor,
                touch.rotation,
                TouchEventType::Down,
            ) {
                warn!(
                    pointer_id = touch.pointer_id,
                    "Failed to replay held touch: {err:?}"
                );
            }
        }
        // MoonlightStreamSettings carries only the attached-controller mask.
        // Re-announce descriptors first so the host preserves controller type
        // and optional capabilities before the first state update arrives.
        for (id, descriptor) in held_input.controller_descriptors.iter().enumerate() {
            let id = id as u8;
            let Some(descriptor) = descriptor else {
                continue;
            };
            let Some(gamepad) = ActiveGamepads::from_id(id) else {
                continue;
            };
            if !active_gamepads.contains(gamepad) {
                continue;
            }
            if let Err(err) = stream.send_controller_arrival(
                id,
                active_gamepads,
                descriptor.ty,
                descriptor.supported_buttons,
                descriptor.capabilities,
            ) {
                warn!(id, "Failed to replay controller descriptor: {err:?}");
            }
        }
        for (id, state) in held_input.controllers.iter().enumerate() {
            let id = id as u8;
            let Some(state) = state else {
                continue;
            };
            let Some(gamepad) = ActiveGamepads::from_id(id) else {
                continue;
            };
            if !active_gamepads.contains(gamepad) {
                continue;
            }
            if let Err(err) = stream.send_multi_controller(
                id,
                active_gamepads,
                state.buttons,
                state.left_trigger,
                state.right_trigger,
                state.left_stick_x,
                state.left_stick_y,
                state.right_stick_x,
                state.right_stick_y,
            ) {
                warn!(id, "Failed to replay controller state: {err:?}");
            }
        }

        Ok(())
    }

    /// Replace the native Moonlight connection while retaining the selected
    /// browser transport. The caller must hold `native_lifecycle`; input is
    /// drained independently while `native_input_replay_pending` is set.
    async fn replace_native_stream_locked(
        self: &Arc<Self>,
        stream_settings: StreamSettings,
        report_fatal_errors: bool,
        expectation: NativeStartExpectation,
    ) -> Result<NativeReplaceOutcome, anyhow::Error> {
        if !expectation.is_current(self) {
            return Ok(NativeReplaceOutcome::Superseded);
        }
        let generation = self
            .native_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.mark_native_start_in_progress(generation, expectation);
        self.native_media_ready_generation
            .store(0, Ordering::Release);

        // Invalidate callbacks first, remove the shared handle second, and wait
        // for the native threads to finish before starting a replacement.
        if let Some(old_stream) = self.stream.write().await.take() {
            self.stop_native_stream_locked(old_stream).await?;
        }
        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            return Ok(NativeReplaceOutcome::Superseded);
        }
        {
            let mut setup = self.stream_setup.lock().await;
            setup.video = None;
            setup.audio = None;
        }

        info!(
            generation,
            bitrate_kbps = stream_settings.bitrate_kbps,
            "Starting Moonlight stream"
        );

        let ipc_sender = self.ipc_sender.clone();
        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            return Ok(NativeReplaceOutcome::Superseded);
        }
        self.await_lifecycle_io(
            "stream stage announcement",
            ipc_sender.send(StreamerIpcMessage::WebSocket(
                StreamServerMessage::DebugLog {
                    message: "Moonlight Stream".to_string(),
                    ty: None,
                },
            )),
        )
        .await?;
        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            return Ok(NativeReplaceOutcome::Superseded);
        }

        let video_decoder = StreamVideoDecoder {
            stream: Arc::downgrade(self),
            generation,
            supported_formats: VideoFormats::from_bits_retain(stream_settings.supported_codecs),
            stats: Default::default(),
        };
        let audio_decoder = StreamAudioDecoder {
            stream: Arc::downgrade(self),
            generation,
        };
        let connection_listener = StreamConnectionListener {
            stream: Arc::downgrade(self),
            generation,
            expectation,
            report_fatal_stage_errors: report_fatal_errors,
        };
        let connection_listener_c = StreamConnectionListener {
            stream: Arc::downgrade(self),
            generation,
            expectation,
            report_fatal_stage_errors: report_fatal_errors,
        };

        let mut encryption_flags = EncryptionFlags::NONE;
        if stream_settings.encrypt_host_video {
            encryption_flags |= EncryptionFlags::VIDEO;
        }
        if stream_settings.encrypt_host_audio {
            encryption_flags |= EncryptionFlags::AUDIO;
        }

        let gamepads_attached = *self.active_gamepads.read().await;
        let mut moonlight_settings = MoonlightStreamSettings {
            width: stream_settings.width,
            height: stream_settings.height,
            fps: stream_settings.fps,
            fps_x100: stream_settings.fps * 100,
            hdr: stream_settings.hdr,
            bitrate: stream_settings.bitrate_kbps,
            packet_size: 1024,
            encryption_flags,
            streaming_remotely: if HOST_IS_LOCAL.get().copied().unwrap_or(false) {
                StreamingConfig::Local
            } else {
                StreamingConfig::Auto
            },
            sops: true,
            supported_video_formats: VideoFormats::from_bits_truncate(
                stream_settings.supported_codecs,
            ),
            color_space: ColorSpace::Rec709,
            color_range: ColorRange::Limited,
            local_audio_play_mode: stream_settings.play_audio_local,
            audio_config: AudioConfig::STEREO,
            gamepads_attached,
            gamepads_persist_after_disconnect: false,
            enable_mic: false,
        };

        let host = &self.info.host;
        // End the cache read guard before the miss arm reacquires it to store
        // queried capabilities. Match scrutinee temporaries otherwise live
        // through the complete match expression.
        let cached_host_capabilities = { self.host_capabilities.lock().await.clone() };
        let host_capabilities = match cached_host_capabilities {
            Some(capabilities) => capabilities,
            None => {
                let capabilities = HostCapabilityCache {
                    server_version: self
                        .await_lifecycle_io("host version query", host.version())
                        .await??,
                    server_gfe_version: self
                        .await_lifecycle_io("host GFE version query", host.gfe_version())
                        .await??,
                    server_codec_mode_support: self
                        .await_lifecycle_io(
                            "host codec support query",
                            host.server_codec_mode_support(),
                        )
                        .await??,
                };
                self.host_capabilities
                    .lock()
                    .await
                    .replace(capabilities.clone());
                capabilities
            }
        };
        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            return Ok(NativeReplaceOutcome::Superseded);
        }

        match moonlight_settings.adjust_for_server(
            host_capabilities.server_version,
            &host_capabilities.server_gfe_version,
            host_capabilities.server_codec_mode_support,
        ) {
            Ok(_) => {}
            Err(StreamConfigError::NotSupportedHdr) => {
                if report_fatal_errors {
                    if !self.native_start_context_current(generation, expectation) {
                        self.invalidate_native_start_generation(generation);
                        return Ok(NativeReplaceOutcome::Superseded);
                    }
                    self.await_lifecycle_io(
                        "HDR failure announcement",
                        ipc_sender.send(StreamerIpcMessage::WebSocket(
                            StreamServerMessage::DebugLog {
                                message:
                                    "Failed to start stream because this app doesn't support HDR!"
                                        .to_string(),
                                ty: Some(LogMessageType::FatalDescription),
                            },
                        )),
                    )
                    .await?;
                }
                return Err(StreamConfigError::NotSupportedHdr.into());
            }
            Err(err) => return Err(err.into()),
        }

        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            return Ok(NativeReplaceOutcome::Superseded);
        }

        let aes_key = AesKey::new_random(&OpenSSLCryptoBackend)?;
        let aes_iv = AesIv::new_random(&OpenSSLCryptoBackend)?;
        let host_start = self
            .await_lifecycle_io(
                "host stream reconnect",
                host.start_stream(
                    self.info.app_id,
                    &moonlight_settings,
                    aes_key,
                    aes_iv,
                    self.moonlight.launch_query_parameters(),
                ),
            )
            .await?;
        let stream_config = match host_start {
            Ok(value) => value,
            Err(err) => {
                if !self.native_start_context_current(generation, expectation) {
                    self.invalidate_native_start_generation(generation);
                    return Ok(NativeReplaceOutcome::Superseded);
                }
                warn!("[Stream]: failed to start moonlight stream: {err}");
                if matches!(
                    err,
                    MoonlightClientError::Moonlight(MoonlightError::ConnectionAlreadyExists)
                ) {
                    if !self.native_start_context_current(generation, expectation) {
                        self.invalidate_native_start_generation(generation);
                        return Ok(NativeReplaceOutcome::Superseded);
                    }
                    let _ = self
                        .await_lifecycle_io(
                            "duplicate stream failure announcement",
                            ipc_sender.send(StreamerIpcMessage::WebSocket(
                            StreamServerMessage::DebugLog {
                                message: "Failed to start stream because this streamer is already streaming"
                                    .to_string(),
                                ty: None,
                            },
                            )),
                        )
                        .await;
                }
                if !self.native_start_context_current(generation, expectation) {
                    self.invalidate_native_start_generation(generation);
                    return Ok(NativeReplaceOutcome::Superseded);
                }
                return Err(err.into());
            }
        };

        let settings_for_native = moonlight_settings.clone();
        let moonlight_instance = self.moonlight.clone();
        let native_start = spawn_blocking(move || {
            moonlight_instance.start_connection(
                stream_config,
                settings_for_native,
                connection_listener,
                connection_listener_c,
                video_decoder,
                audio_decoder,
            )
        });
        let stream = match timeout(NATIVE_START_TIMEOUT, native_start).await {
            Ok(Ok(Ok(stream))) => stream,
            Ok(Ok(Err(err))) => {
                if !self.native_start_context_current(generation, expectation) {
                    self.invalidate_native_start_generation(generation);
                    return Ok(NativeReplaceOutcome::Superseded);
                }
                return Err(err.into());
            }
            Ok(Err(err)) => {
                self.terminate_stuck_native_operation_locked("native start task failed")
                    .await;
                return Err(err.into());
            }
            Err(_) => {
                self.terminate_stuck_native_operation_locked(
                    "native start exceeded its connection deadline",
                )
                .await;
                anyhow::bail!(
                    "native start exceeded its {:?} deadline; streamer is terminating",
                    NATIVE_START_TIMEOUT
                );
            }
        };

        if !self.native_start_context_current(generation, expectation) {
            self.invalidate_native_start_generation(generation);
            self.stop_native_stream_locked(stream).await?;
            return Ok(NativeReplaceOutcome::Superseded);
        }

        let host_features = stream.host_features().unwrap_or_else(|err| {
            warn!("[Stream]: failed to get host features: {err:?}");
            HostFeatures::default()
        });
        let capabilities = StreamCapabilities {
            touch: host_features.controller_touch,
        };
        let (video_setup, audio_setup) = {
            let setup = self.stream_setup.lock().await;
            let video = setup.video.unwrap_or_else(|| {
                warn!(
                    "failed to query video setup information. Giving the browser guessed information"
                );
                VideoSetup {
                    format: VideoFormat::H264,
                    width: stream_settings.width,
                    height: stream_settings.height,
                    redraw_rate: stream_settings.fps,
                }
            });
            let audio = setup.audio.clone().unwrap_or(OpusMultistreamConfig::STEREO);
            (video, audio)
        };

        info!(
            "Stream uses these settings: {:?} with {}x{}x{} at {} Kbps",
            video_setup.format,
            video_setup.width,
            video_setup.height,
            video_setup.redraw_rate,
            stream_settings.bitrate_kbps,
        );

        let completion = StreamerIpcMessage::WebSocket(StreamServerMessage::ConnectionComplete {
            capabilities,
            format: video_setup.format as u32,
            width: video_setup.width,
            height: video_setup.height,
            fps: video_setup.redraw_rate,
            audio_sample_rate: audio_setup.sample_rate,
            audio_channel_count: audio_setup.channel_count,
            audio_streams: audio_setup.streams,
            audio_coupled_streams: audio_setup.coupled_streams,
            audio_samples_per_frame: audio_setup.samples_per_frame,
            audio_mapping: audio_setup.mapping,
        });

        // Publication and completion enqueue are one short atomic section with
        // respect to transport handoff and newer StartStream events. No slow
        // native or transport setup work is performed while this gate is held.
        let dispatch_guard = self.transport_dispatch.write().await;
        if !self.native_start_context_current(generation, expectation) {
            drop(dispatch_guard);
            self.invalidate_native_start_generation(generation);
            self.stop_native_stream_locked(stream).await?;
            return Ok(NativeReplaceOutcome::Superseded);
        }
        self.stream.write().await.replace(stream);
        if ipc_sender.try_send(completion).is_err() {
            drop(dispatch_guard);
            self.discard_native_generation_locked(generation).await?;
            anyhow::bail!("stream completion announcement was backpressured");
        }
        self.native_media_ready_generation
            .store(generation, Ordering::Release);
        let transport_sender = self.transport_sender.lock().await.clone();
        drop(dispatch_guard);

        let Some(transport_sender) = transport_sender else {
            if !self.native_start_context_current(generation, expectation) {
                self.discard_native_generation_locked(generation).await?;
                return Ok(NativeReplaceOutcome::Superseded);
            }
            self.discard_native_generation_locked(generation).await?;
            anyhow::bail!("transport disappeared during native stream publication");
        };
        let setup_result = self
            .await_lifecycle_io(
                "transport setup completion",
                transport_sender.on_setup_complete(),
            )
            .await;
        if !self.native_start_context_current(generation, expectation) {
            self.discard_native_generation_locked(generation).await?;
            return Ok(NativeReplaceOutcome::Superseded);
        }
        setup_result?;

        // A second short gate closes the window while on_setup_complete was
        // awaiting the captured (old) sender. Only a still-current task may be
        // marked committed and proceed to input replay.
        let dispatch_guard = self.transport_dispatch.write().await;
        if !self.native_start_context_current(generation, expectation) {
            drop(dispatch_guard);
            self.discard_native_generation_locked(generation).await?;
            return Ok(NativeReplaceOutcome::Superseded);
        }
        self.native_start_in_progress_generation
            .store(0, Ordering::Release);
        drop(dispatch_guard);

        Ok(NativeReplaceOutcome::Ready {
            configured_gamepads: gamepads_attached,
            native_generation: generation,
        })
    }

    // -- Termination
    async fn request_terminate(self: &Arc<Self>) {
        debug!("Marking for termination");

        let this = self.clone();
        let generation = self.transport_generation.load(Ordering::Acquire);

        let mut terminate_request = self.timeout_terminate_request.lock().await;
        *terminate_request = Some(Instant::now());
        drop(terminate_request);

        spawn(async move {
            sleep(TIMEOUT_DURATION + Duration::from_millis(200)).await;

            // A replacement clears the request under the write side of this
            // gate. Never let a timeout armed by an old transport commit a
            // stop against the newly installed sender.
            let _dispatch_guard = this.transport_dispatch.read().await;
            if this.transport_generation.load(Ordering::Acquire) != generation {
                return;
            }

            let now = Instant::now();

            let should_stop = {
                let mut terminate_request = this.timeout_terminate_request.lock().await;
                let should_stop = terminate_request
                    .is_some_and(|requested_at| now - requested_at > TIMEOUT_DURATION);
                if should_stop {
                    *terminate_request = None;
                }
                should_stop
            };
            if should_stop {
                info!("Stopping because of timeout");

                this.stop().await;
            }
        });
    }
    async fn clear_terminate_request(&self) {
        debug!("Clearing termination timeout");

        let mut request = self.timeout_terminate_request.lock().await;

        *request = None;
    }

    fn mark_native_start_in_progress(&self, generation: u64, expectation: NativeStartExpectation) {
        self.native_expected_request_generation
            .store(expectation.request_generation, Ordering::Relaxed);
        self.native_expected_transport_generation
            .store(expectation.transport_generation, Ordering::Relaxed);
        self.native_expected_adaptive_transport
            .store(expectation.require_adaptive_transport, Ordering::Relaxed);
        self.native_start_in_progress_generation
            .store(generation, Ordering::Release);
    }

    fn registered_native_start_expectation(&self) -> NativeStartExpectation {
        NativeStartExpectation {
            request_generation: self
                .native_expected_request_generation
                .load(Ordering::Acquire),
            transport_generation: self
                .native_expected_transport_generation
                .load(Ordering::Acquire),
            require_adaptive_transport: self
                .native_expected_adaptive_transport
                .load(Ordering::Acquire),
        }
    }

    fn native_callback_context_current(
        &self,
        generation: u64,
        expectation: NativeStartExpectation,
    ) -> bool {
        native_callback_context_matches(
            generation,
            self.native_generation.load(Ordering::Acquire),
            expectation,
            self.registered_native_start_expectation(),
            self.is_terminating.load(Ordering::Acquire),
            self.adaptive_transport_active.load(Ordering::Acquire),
            self.native_start_request_generation.load(Ordering::Acquire),
            self.transport_generation.load(Ordering::Acquire),
        )
    }

    fn native_start_context_current(
        &self,
        generation: u64,
        expectation: NativeStartExpectation,
    ) -> bool {
        self.native_generation.load(Ordering::Acquire) == generation
            && self
                .native_start_in_progress_generation
                .load(Ordering::Acquire)
                == generation
            && self.registered_native_start_expectation() == expectation
            && expectation.is_current(self)
    }

    fn invalidate_native_start_generation(&self, generation: u64) {
        if self
            .native_generation
            .compare_exchange(
                generation,
                generation.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.native_media_ready_generation
                .store(0, Ordering::Release);
            let _ = self.native_start_in_progress_generation.compare_exchange(
                generation,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    async fn discard_native_generation_locked(&self, generation: u64) -> Result<(), anyhow::Error> {
        if self.native_generation.load(Ordering::Acquire) != generation {
            return Ok(());
        }
        let stream = self.stream.write().await.take();
        self.invalidate_native_start_generation(generation);
        if let Some(stream) = stream {
            self.stop_native_stream_locked(stream).await?;
        }
        Ok(())
    }

    async fn discard_in_progress_native_start_locked(&self) -> Result<(), anyhow::Error> {
        let generation = self
            .native_start_in_progress_generation
            .load(Ordering::Acquire);
        if generation == 0 {
            return Ok(());
        }
        self.discard_native_generation_locked(generation).await
    }

    async fn stop_failed_start_if_current(
        self: &Arc<Self>,
        expectation: NativeStartExpectation,
        err: anyhow::Error,
    ) {
        // Serialize the final check and termination commit with both transport
        // handoff and newer StartStream events. A stale task is nonfatal.
        let dispatch_guard = self.transport_dispatch.write().await;
        if !expectation.is_current(self) {
            debug!(
                request_generation = expectation.request_generation,
                transport_generation = expectation.transport_generation,
                error = %err,
                "Ignoring failure from superseded stream start"
            );
            return;
        }
        error!("Failed to start stream, stopping: {err}");
        let began_termination = self.begin_termination();
        drop(dispatch_guard);
        if !began_termination {
            return;
        }
        let _lifecycle_guard = self.native_lifecycle.lock().await;
        self.finish_stop_locked().await;
    }

    async fn begin_termination_for_expected_start(
        &self,
        expectation: NativeStartExpectation,
    ) -> bool {
        let dispatch_guard = self.transport_dispatch.write().await;
        let began = expectation.is_current(self) && self.begin_termination();
        drop(dispatch_guard);
        began
    }

    pub(crate) fn is_current_native_generation(&self, generation: u64) -> bool {
        if self.is_terminating.load(Ordering::Acquire)
            || self.native_generation.load(Ordering::Acquire) != generation
        {
            return false;
        }
        self.native_start_in_progress_generation
            .load(Ordering::Acquire)
            != generation
            || self.registered_native_start_expectation().is_current(self)
    }

    pub(crate) fn is_native_media_ready(&self, generation: u64) -> bool {
        native_media_ready_for_transport(
            self.is_current_native_generation(generation),
            self.adaptive_transport_active.load(Ordering::Acquire),
            self.native_media_ready_generation.load(Ordering::Acquire),
            generation,
        )
    }

    fn begin_termination(&self) -> bool {
        let began = self
            .is_terminating
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok();
        if began {
            self.termination_signal.send_replace(true);
        }
        began
    }

    /// Called while `native_lifecycle` is held after a blocking native task
    /// either panicked or exceeded its deadline. A timed-out `spawn_blocking`
    /// task cannot be cancelled, so this path must terminate the process and
    /// must never permit a rollback or another native start/stop to overlap it.
    async fn terminate_stuck_native_operation_locked(&self, reason: &'static str) {
        error!(
            reason,
            "Terminating after an unrecoverable native operation failure"
        );
        if self.begin_termination() {
            // The shared stream was removed before the blocking task started,
            // so this closes transport/IPC and wakes the process waiter without
            // launching a second native operation.
            self.finish_stop_locked().await;
        }
    }

    async fn stop_native_stream_locked(
        &self,
        stream: MoonlightStream,
    ) -> Result<(), anyhow::Error> {
        let native_stop = spawn_blocking(move || stream.stop());
        match timeout(NATIVE_STOP_TIMEOUT, native_stop).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                self.terminate_stuck_native_operation_locked("native stop task failed")
                    .await;
                Err(err.into())
            }
            Err(_) => {
                self.terminate_stuck_native_operation_locked(
                    "native stop exceeded its shutdown deadline",
                )
                .await;
                anyhow::bail!(
                    "native stop exceeded its {:?} deadline; streamer is terminating",
                    NATIVE_STOP_TIMEOUT
                )
            }
        }
    }

    async fn handle_native_termination(self: &Arc<Self>, generation: u64, error_code: i32) {
        let _lifecycle_guard = self.native_lifecycle.lock().await;
        if !self.is_current_native_generation(generation) {
            debug!(
                generation,
                error_code, "Ignoring stale native termination callback"
            );
            return;
        }
        if !self.begin_termination() {
            return;
        }

        if timeout(
            IPC_STOP_ENQUEUE_TIMEOUT,
            self.ipc_sender.send(StreamerIpcMessage::WebSocket(
                StreamServerMessage::ConnectionTerminated { error_code },
            )),
        )
        .await
        .is_err()
        {
            warn!("Connection termination announcement was backpressured");
        }
        self.finish_stop_locked().await;
    }

    async fn stop(&self) {
        if !self.begin_termination() {
            debug!("[Stream]: stream is already terminating, won't stop twice");
            return;
        }

        let _lifecycle_guard = self.native_lifecycle.lock().await;
        self.finish_stop_locked().await;
    }

    /// Complete process shutdown while holding `native_lifecycle` and after
    /// successfully transitioning `is_terminating` to true.
    async fn finish_stop_locked(&self) {
        debug!("[Stream]: Stopping...");
        self.adaptive_transport_active
            .store(false, Ordering::Release);
        self.native_generation.fetch_add(1, Ordering::AcqRel);

        // Remove both shared handles before native shutdown starts. Native
        // callbacks can then return immediately instead of waiting on a lock
        // held by this shutdown path.
        let stream = self.stream.write().await.take();
        let transport = { self.transport_sender.lock().await.take() };

        let native_stop = stream.map(|stream| {
            spawn_blocking(move || {
                stream.stop();
            })
        });

        if let Some(transport) = transport {
            match timeout(TRANSPORT_CLOSE_TIMEOUT, transport.close()).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!("Error whilst closing transport: {err}"),
                Err(_) => warn!(
                    "[Stream]: transport close exceeded {:?}; continuing shutdown",
                    TRANSPORT_CLOSE_TIMEOUT
                ),
            }
        }

        let ipc_sender = self.ipc_sender.clone();
        match timeout(
            IPC_STOP_ENQUEUE_TIMEOUT,
            ipc_sender.send_checked(StreamerIpcMessage::Stop),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => debug!("[Stream]: parent IPC is already closed"),
            Err(_) => warn!(
                "[Stream]: IPC stop enqueue exceeded {:?}; continuing local shutdown",
                IPC_STOP_ENQUEUE_TIMEOUT
            ),
        }

        if let Some(native_stop) = native_stop {
            match timeout(NATIVE_STOP_TIMEOUT, native_stop).await {
                Ok(Ok(())) => debug!("[Stream]: native stream stopped"),
                Ok(Err(err)) => warn!("[Stream]: native stop task failed: {err}"),
                Err(_) => warn!(
                    "[Stream]: native stream stop exceeded {:?}; forcing streamer exit",
                    NATIVE_STOP_TIMEOUT
                ),
            }
        }

        debug!("Notifying termination");
        // There is one process-lifetime waiter. notify_one stores a permit if
        // shutdown wins the race with main reaching notified().
        self.terminate.notify_one();
    }
}

struct StreamConnectionListener {
    stream: Weak<StreamConnection>,
    generation: u64,
    expectation: NativeStartExpectation,
    report_fatal_stage_errors: bool,
}

impl StreamConnectionListener {
    fn current_stream(&self) -> Option<Arc<StreamConnection>> {
        let stream = self.stream.upgrade()?;
        if !stream.is_current_native_generation(self.generation) {
            debug!(
                generation = self.generation,
                "Ignoring callback from stale native stream"
            );
            return None;
        }
        Some(stream)
    }
}

impl ConnectionListener for StreamConnectionListener {
    fn set_hdr_mode(&mut self, hdr_enabled: bool, _sunshine: Option<SunshineHdrMetadata>) {
        info!(
            "[HDR] Host called set_hdr_mode with enabled={}",
            hdr_enabled
        );

        let Some(stream) = self.current_stream() else {
            return;
        };

        let generation = self.generation;
        let expectation = self.expectation;
        stream.clone().runtime.block_on(async move {
            info!("[HDR] Sending HdrModeUpdate to client");
            stream
                .try_send_native_callback_packet(
                    generation,
                    expectation,
                    OutboundPacket::General {
                        message: GeneralServerMessage::HdrModeUpdate {
                            enabled: hdr_enabled,
                        },
                    },
                    "hdr mode update",
                    true,
                )
                .await
        })
    }

    fn controller_rumble(
        &mut self,
        controller_number: u16,
        low_frequency_motor: u16,
        high_frequency_motor: u16,
    ) {
        let Some(stream) = self.current_stream() else {
            return;
        };

        let generation = self.generation;
        let expectation = self.expectation;
        stream.runtime.clone().block_on(async move {
            stream
                .try_send_native_callback_packet(
                    generation,
                    expectation,
                    OutboundPacket::ControllerRumble {
                        controller_number: controller_number as u8,
                        low_frequency_motor,
                        high_frequency_motor,
                    },
                    "controller rumble",
                    true,
                )
                .await;
        });
    }

    fn controller_rumble_triggers(
        &mut self,
        controller_number: u16,
        left_trigger_motor: u16,
        right_trigger_motor: u16,
    ) {
        let Some(stream) = self.current_stream() else {
            return;
        };

        let generation = self.generation;
        let expectation = self.expectation;
        stream.runtime.clone().block_on(async move {
            stream
                .try_send_native_callback_packet(
                    generation,
                    expectation,
                    OutboundPacket::ControllerTriggerRumble {
                        controller_number: controller_number as u8,
                        left_trigger_motor,
                        right_trigger_motor,
                    },
                    "controller rumble triggers",
                    true,
                )
                .await;
        });
    }

    fn controller_set_motion_event_state(
        &mut self,
        _controller_number: u16,
        _motion_type: u8,
        _report_rate_hz: u16,
    ) {
        // unsupported: https://github.com/w3c/gamepad/issues/211
    }

    fn controller_set_adaptive_triggers(
        &mut self,
        _controller_number: u16,
        _event_flags: u8,
        _type_left: u8,
        _type_right: u8,
        _left: &mut u8,
        _right: &mut u8,
    ) {
        // unsupported
    }

    fn controller_set_led(&mut self, _controller_number: u16, _r: u8, _g: u8, _b: u8) {
        // unsupported
    }
}

impl ConnectionListenerC for StreamConnectionListener {
    fn stage_starting(&mut self, stage: Stage) {
        let Some(stream) = self.current_stream() else {
            return;
        };
        if !self.expectation.is_current(&stream) {
            return;
        }

        let ipc_sender = stream.ipc_sender.clone();
        let generation = self.generation;
        let expectation = self.expectation;
        let runtime = stream.runtime.clone();

        runtime.spawn(async move {
            if !stream.is_current_native_generation(generation) || !expectation.is_current(&stream)
            {
                return;
            }
            ipc_sender
                .send(StreamerIpcMessage::WebSocket(
                    StreamServerMessage::DebugLog {
                        message: format!("Starting Stage: {}", stage.name()),
                        ty: None,
                    },
                ))
                .await;
        });
    }

    fn stage_complete(&mut self, stage: Stage) {
        let Some(stream) = self.current_stream() else {
            return;
        };
        if !stream.is_current_native_generation(self.generation)
            || !self.expectation.is_current(&stream)
        {
            return;
        }

        if stream
            .ipc_sender
            .try_send(StreamerIpcMessage::WebSocket(
                StreamServerMessage::DebugLog {
                    message: format!("Completed Stage: {}", stage.name()),
                    ty: None,
                },
            ))
            .is_err()
        {
            debug!(
                stage = stage.name(),
                "Dropping completed-stage log because IPC is backpressured"
            );
        }
    }

    fn stage_failed(&mut self, stage: Stage, error_code: i32) {
        let Some(stream) = self.current_stream() else {
            return;
        };
        if !stream.is_current_native_generation(self.generation)
            || !self.expectation.is_current(&stream)
        {
            return;
        }

        if stream
            .ipc_sender
            .try_send(StreamerIpcMessage::WebSocket(
                StreamServerMessage::DebugLog {
                    message: format!(
                        "Failed Stage: {} with error code {}",
                        stage.name(),
                        error_code
                    ),
                    ty: self
                        .report_fatal_stage_errors
                        .then_some(LogMessageType::Fatal),
                },
            ))
            .is_err()
        {
            debug!(
                stage = stage.name(),
                error_code, "Dropping failed-stage log because IPC is backpressured"
            );
        }
    }

    fn connection_started(&mut self) {}

    fn connection_terminated(&mut self, error_code: i32) {
        let Some(stream) = self.current_stream() else {
            return;
        };

        let runtime = stream.runtime.clone();
        let generation = self.generation;
        runtime.spawn(async move {
            // Serialize this check with native replacement. A callback that was
            // current when queued may be stale by the time it reaches Tokio.
            stream
                .handle_native_termination(generation, error_code)
                .await;
        });
    }

    fn log_message(&mut self, message: &str) {
        info!(target: "moonlight", "{}", message.trim());
    }

    fn connection_status_update(&mut self, status: ConnectionStatus) {
        let Some(stream) = self.current_stream() else {
            return;
        };

        let generation = self.generation;
        let expectation = self.expectation;
        stream.clone().runtime.block_on(async move {
            stream
                .try_send_native_callback_packet(
                    generation,
                    expectation,
                    OutboundPacket::General {
                        message: GeneralServerMessage::ConnectionStatusUpdate {
                            status: status.into(),
                        },
                    },
                    "connection status update",
                    true,
                )
                .await
        })
    }
}

#[cfg(test)]
mod input_activity_tests {
    use super::*;

    #[test]
    fn native_start_expectation_rejects_late_handoffs_and_requests() {
        let browser = NativeStartExpectation::browser(7, 11);
        assert!(native_start_expectation_matches(
            browser, false, false, 7, 11
        ));
        assert!(!native_start_expectation_matches(
            browser, false, false, 8, 11
        ));
        assert!(!native_start_expectation_matches(
            browser, false, false, 7, 12
        ));
        assert!(!native_start_expectation_matches(
            browser, true, false, 7, 11
        ));
    }

    #[test]
    fn adaptive_start_expectation_expires_when_data_transport_is_replaced() {
        let adaptive = NativeStartExpectation::adaptive(3, 5);
        assert!(native_start_expectation_matches(
            adaptive, false, true, 3, 5
        ));
        assert!(!native_start_expectation_matches(
            adaptive, false, false, 3, 5
        ));
    }

    #[test]
    fn native_callback_context_stays_bound_to_its_generation_and_transport() {
        let callback = NativeStartExpectation::browser(7, 11);
        assert!(native_callback_context_matches(
            4, 4, callback, callback, false, false, 7, 11,
        ));
        assert!(!native_callback_context_matches(
            4, 5, callback, callback, false, false, 7, 11,
        ));
        assert!(!native_callback_context_matches(
            4,
            4,
            callback,
            NativeStartExpectation::browser(8, 11),
            false,
            false,
            8,
            11,
        ));
        assert!(!native_callback_context_matches(
            4, 4, callback, callback, false, false, 7, 12,
        ));
    }

    #[test]
    fn literal_localhost_uses_ipv4_loopback_without_dns_retry() {
        assert_eq!(normalize_loopback_host("localhost".to_owned()), "127.0.0.1");
        assert_eq!(normalize_loopback_host("LOCALHOST".to_owned()), "127.0.0.1");
    }

    #[test]
    fn non_localhost_addresses_are_preserved() {
        for address in ["sunshine.local", "192.168.1.20", "::1"] {
            assert_eq!(normalize_loopback_host(address.to_owned()), address);
        }
    }

    #[test]
    fn held_keys_preserve_press_order_and_latest_metadata() {
        let mut activity = InputActivityState::default();
        activity.record(&InboundPacket::Key {
            action: KeyAction::Down,
            modifiers: KeyModifiers::CTRL,
            key: 0x11,
            flags: KeyFlags::empty(),
        });
        activity.record(&InboundPacket::Key {
            action: KeyAction::Down,
            modifiers: KeyModifiers::CTRL,
            key: 0x43,
            flags: KeyFlags::empty(),
        });
        // A repeated Down updates metadata without moving the key later in the
        // replay order.
        activity.record(&InboundPacket::Key {
            action: KeyAction::Down,
            modifiers: KeyModifiers::CTRL | KeyModifiers::SHIFT,
            key: 0x43,
            flags: KeyFlags::SUNSHINE_NON_NORMALIZED,
        });

        assert_eq!(
            activity.snapshot().keys,
            vec![
                HeldKey {
                    key: 0x11,
                    modifiers: KeyModifiers::CTRL | KeyModifiers::SHIFT,
                    flags: KeyFlags::empty(),
                },
                HeldKey {
                    key: 0x43,
                    modifiers: KeyModifiers::CTRL | KeyModifiers::SHIFT,
                    flags: KeyFlags::SUNSHINE_NON_NORMALIZED,
                },
            ]
        );

        activity.record(&InboundPacket::Key {
            action: KeyAction::Up,
            modifiers: KeyModifiers::empty(),
            key: 0x11,
            flags: KeyFlags::empty(),
        });
        assert_eq!(activity.snapshot().keys.len(), 1);
        assert_eq!(activity.snapshot().keys[0].key, 0x43);
        assert_eq!(activity.snapshot().keys[0].modifiers, KeyModifiers::empty());
    }

    #[test]
    fn snapshot_retains_mouse_and_latest_active_touch_state() {
        let mut activity = InputActivityState::default();
        activity.record(&InboundPacket::MouseButton {
            action: MouseButtonAction::Press,
            button: MouseButton::Left,
        });
        activity.record(&InboundPacket::MouseButton {
            action: MouseButtonAction::Press,
            button: MouseButton::Left,
        });
        activity.record(&InboundPacket::MouseButton {
            action: MouseButtonAction::Press,
            button: MouseButton::Right,
        });
        activity.record(&InboundPacket::MouseButton {
            action: MouseButtonAction::Release,
            button: MouseButton::Left,
        });
        activity.record(&InboundPacket::Touch {
            pointer_id: 7,
            x: 0.25,
            y: 0.5,
            pressure_or_distance: 0.5,
            contact_area_major: 0.1,
            contact_area_minor: 0.2,
            rotation: Some(10),
            event_type: TouchEventType::Down,
        });
        activity.record(&InboundPacket::Touch {
            pointer_id: 7,
            x: 0.75,
            y: 0.8,
            pressure_or_distance: 0.9,
            contact_area_major: 0.3,
            contact_area_minor: 0.4,
            rotation: Some(20),
            event_type: TouchEventType::Move,
        });

        let snapshot = activity.snapshot();
        assert_eq!(snapshot.mouse_buttons, vec![MouseButton::Right]);
        assert_eq!(
            snapshot.touches.get(&7),
            Some(&HeldTouch {
                pointer_id: 7,
                x: 0.75,
                y: 0.8,
                pressure_or_distance: 0.9,
                contact_area_major: 0.3,
                contact_area_minor: 0.4,
                rotation: Some(20),
            })
        );

        activity.record(&InboundPacket::Touch {
            pointer_id: 7,
            x: 0.75,
            y: 0.8,
            pressure_or_distance: 0.0,
            contact_area_major: 0.0,
            contact_area_minor: 0.0,
            rotation: None,
            event_type: TouchEventType::Up,
        });
        assert!(activity.snapshot().touches.is_empty());
    }

    #[test]
    fn controller_snapshot_retains_descriptor_and_full_neutral_state() {
        let mut activity = InputActivityState::default();
        activity.record(&InboundPacket::ControllerConnected {
            id: 3,
            ty: ControllerType::PlayStation,
            supported_buttons: ControllerButtons::A | ControllerButtons::TOUCHPAD,
            capabilities: ControllerCapabilities::RUMBLE | ControllerCapabilities::GYRO,
        });
        assert_eq!(
            activity.snapshot().controller_descriptors[3],
            Some(ControllerDescriptor {
                ty: ControllerType::PlayStation,
                supported_buttons: ControllerButtons::A | ControllerButtons::TOUCHPAD,
                capabilities: ControllerCapabilities::RUMBLE | ControllerCapabilities::GYRO,
            })
        );

        activity.record(&InboundPacket::ControllerState {
            id: 3,
            buttons: ControllerButtons::A | ControllerButtons::LB,
            left_trigger: 17,
            right_trigger: 23,
            left_stick_x: -1234,
            left_stick_y: 2345,
            right_stick_x: -3456,
            right_stick_y: 4567,
        });
        assert_eq!(
            activity.snapshot().controllers[3],
            Some(ControllerInputState {
                buttons: ControllerButtons::A | ControllerButtons::LB,
                left_trigger: 17,
                right_trigger: 23,
                left_stick_x: -1234,
                left_stick_y: 2345,
                right_stick_x: -3456,
                right_stick_y: 4567,
            })
        );

        activity.record(&InboundPacket::ControllerState {
            id: 3,
            buttons: ControllerButtons::empty(),
            left_trigger: 0,
            right_trigger: 0,
            left_stick_x: 0,
            left_stick_y: 0,
            right_stick_x: 0,
            right_stick_y: 0,
        });
        assert_eq!(
            activity.snapshot().controllers[3],
            Some(ControllerInputState {
                buttons: ControllerButtons::empty(),
                left_trigger: 0,
                right_trigger: 0,
                left_stick_x: 0,
                left_stick_y: 0,
                right_stick_x: 0,
                right_stick_y: 0,
            })
        );

        activity.record(&InboundPacket::ControllerDisconnected { id: 3 });
        assert_eq!(activity.snapshot().controller_descriptors[3], None);
        assert_eq!(activity.snapshot().controllers[3], None);
    }

    #[test]
    fn captured_snapshot_is_immutable_while_newer_releases_update_live_state() {
        let mut activity = InputActivityState::default();
        activity.record(&InboundPacket::Key {
            action: KeyAction::Down,
            modifiers: KeyModifiers::empty(),
            key: 0x57,
            flags: KeyFlags::empty(),
        });
        let captured = activity.snapshot();
        activity.record(&InboundPacket::Key {
            action: KeyAction::Up,
            modifiers: KeyModifiers::empty(),
            key: 0x57,
            flags: KeyFlags::empty(),
        });

        assert_eq!(captured.keys.len(), 1);
        assert_eq!(captured.keys[0].key, 0x57);
        assert!(activity.snapshot().keys.is_empty());
    }

    #[test]
    fn media_readiness_gate_applies_only_to_data_transports() {
        let generation = 9;
        assert!(!native_media_ready_for_transport(true, true, 0, generation));
        assert!(native_media_ready_for_transport(
            true, true, generation, generation
        ));

        // WebRTC provides track-based PLI/recovery and must not lose its
        // initial IDR behind the data-channel setup gate.
        assert!(native_media_ready_for_transport(true, false, 0, generation));
        assert!(!native_media_ready_for_transport(
            false, false, generation, generation
        ));
    }
}
