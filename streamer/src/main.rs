#![feature(async_fn_traits)]

use std::{
    io, panic,
    process::exit,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use common::{
    api_bindings::{
        GeneralClientMessage, GeneralServerMessage, LogMessageType, StreamClientMessage,
        StreamPermissions, StreamSettings, TransportType,
    },
    apply_permissions_to_settings,
    ipc::{
        IpcReceiver, IpcSender, ServerIpcMessage, StreamerConfig, StreamerIpcMessage,
        create_process_ipc,
    },
};
use moonlight_common::{
    MoonlightError,
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
        control::{ActiveGamepads, ControllerButtons},
        video::{
            ColorRange, ColorSpace, SunshineHdrMetadata, VideoFormat, VideoFormats, VideoSetup,
        },
    },
};
use tokio::{
    io::{stdin, stdout},
    runtime::Handle,
    spawn,
    sync::{Mutex, Notify, RwLock, oneshot},
    task::spawn_blocking,
    time::{sleep, timeout},
};
use tracing::{Level, level_filters::LevelFilter, span};
use tracing::{debug, error, info, trace, warn};

use common::api_bindings::{StreamCapabilities, StreamServerMessage};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
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
const IPC_STOP_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(500);

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
    pub active_gamepads: RwLock<ActiveGamepads>,
    pub transport_sender: Mutex<Option<SharedTransportSender>>,
    transport_dispatch: RwLock<()>,
    transport_generation: AtomicU64,
    transport_cancel: Mutex<Option<oneshot::Sender<()>>>,
    // Timeout / Terminate
    pub timeout_terminate_request: Mutex<Option<Instant>>,
    pub terminate: Notify,
    is_terminating: AtomicBool,
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
            active_gamepads: RwLock::new(ActiveGamepads::empty()),
            transport_sender: Mutex::new(None),
            transport_dispatch: RwLock::new(()),
            transport_generation: AtomicU64::new(0),
            transport_cancel: Mutex::new(None),
            timeout_terminate_request: Default::default(),
            terminate: Notify::default(),
            is_terminating: AtomicBool::new(false),
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
                    let _dispatch_guard = connection.transport_dispatch.read().await;
                    if connection.transport_generation.load(Ordering::Acquire) != generation {
                        debug!(generation, "Ignoring event from stale transport generation");
                        return;
                    }

                    match event {
                        Ok(TransportEvent::SendIpc(message)) => {
                            ipc_sender.send(message).await;
                        }
                        Ok(TransportEvent::StartStream { settings }) => {
                            let this = connection.clone();
                            spawn(async move {
                                let _dispatch_guard = this.transport_dispatch.read().await;
                                if this.transport_generation.load(Ordering::Acquire) != generation {
                                    return;
                                }
                                this.clear_terminate_request().await;

                                if let Err(err) = this.start_stream(settings).await {
                                    error!("Failed to start stream, stopping: {err}");

                                    this.stop().await;
                                }
                            });
                        }
                        Ok(TransportEvent::RecvPacket(packet)) => {
                            connection.on_packet(packet).await;
                        }
                        Err(TransportError::Closed) => {
                            connection.request_terminate().await;

                            break;
                        }
                        Ok(TransportEvent::Closed) => {
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

    async fn on_packet(&self, packet: InboundPacket) {
        trace!(packet = packet.kind(), "received packet from client");

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
                let Some(gamepad) = ActiveGamepads::from_id(id) else {
                    warn!("Failed to add gamepad because it is out of range: {id}");
                    return;
                };

                let mut active_gamepads = self.active_gamepads.write().await;

                active_gamepads.insert(gamepad);

                stream
                    .send_controller_arrival(
                        id,
                        *active_gamepads,
                        ty,
                        supported_buttons,
                        capabilities,
                    )
                    .err()
            }
            InboundPacket::ControllerDisconnected { id } => {
                let Some(gamepad) = ActiveGamepads::from_id(id) else {
                    warn!("Failed to remove gamepad because it is out of range: {id}");
                    return;
                };

                let mut active_gamepads = self.active_gamepads.write().await;
                active_gamepads.remove(gamepad);

                stream
                    .send_multi_controller(
                        id,
                        *active_gamepads,
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
                        self.set_transport(Box::new(sender), Box::new(events), true)
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
                        self.set_transport(Box::new(sender), Box::new(events), false)
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

    // Start Moonlight Stream
    async fn start_stream(self: &Arc<Self>, settings: StreamSettings) -> Result<(), anyhow::Error> {
        // We might already be streaming -> remove and wait for connection close firstly
        {
            let mut stream = self.stream.write().await;
            if let Some(stream) = stream.take() {
                spawn_blocking(move || {
                    stream.stop();
                });
            }
        }
        info!("Starting Moonlight stream with settings: {settings:?}");

        // Send stage
        let ipc_sender = self.ipc_sender.clone();
        ipc_sender
            .send(StreamerIpcMessage::WebSocket(
                StreamServerMessage::DebugLog {
                    message: "Moonlight Stream".to_string(),
                    ty: None,
                },
            ))
            .await;

        let host = &self.info.host;

        let video_decoder = StreamVideoDecoder {
            stream: Arc::downgrade(self),
            supported_formats: VideoFormats::from_bits_retain(settings.supported_codecs),
            stats: Default::default(),
        };

        let audio_decoder = StreamAudioDecoder {
            stream: Arc::downgrade(self),
        };

        let connection_listener = StreamConnectionListener {
            stream: Arc::downgrade(self),
        };
        let connection_listener_c = StreamConnectionListener {
            stream: Arc::downgrade(self),
        };

        let mut settings = MoonlightStreamSettings {
            width: settings.width,
            height: settings.height,
            fps: settings.fps,
            fps_x100: settings.fps * 100,
            hdr: settings.hdr,
            bitrate: settings.bitrate_kbps,
            packet_size: 1024,
            encryption_flags: EncryptionFlags::ALL,
            streaming_remotely: StreamingConfig::Auto,
            sops: true,
            supported_video_formats: VideoFormats::from_bits_truncate(settings.supported_codecs),
            color_space: ColorSpace::Rec709,
            color_range: ColorRange::Limited,
            local_audio_play_mode: settings.play_audio_local,
            audio_config: AudioConfig::STEREO,
            gamepads_attached: ActiveGamepads::empty(),
            gamepads_persist_after_disconnect: false,
            enable_mic: false,
        };

        let server_version = host.version().await?;
        let server_gfe_version = host.gfe_version().await?;
        let server_codec_mode_support = host.server_codec_mode_support().await?;

        match settings.adjust_for_server(
            server_version,
            &server_gfe_version,
            server_codec_mode_support,
        ) {
            Ok(_) => {}
            Err(StreamConfigError::NotSupportedHdr) => {
                ipc_sender
                    .send(StreamerIpcMessage::WebSocket(
                        StreamServerMessage::DebugLog {
                            message: "Failed to start stream because this app doesn't support HDR!"
                                .to_string(),
                            ty: Some(LogMessageType::FatalDescription),
                        },
                    ))
                    .await;
                return Err(StreamConfigError::NotSupportedHdr.into());
            }
            Err(err) => return Err(err.into()),
        }

        let aes_key = AesKey::new_random(&OpenSSLCryptoBackend)?;
        let aes_iv = AesIv::new_random(&OpenSSLCryptoBackend)?;

        let stream_config = match host
            .start_stream(
                self.info.app_id,
                &settings,
                aes_key,
                aes_iv,
                self.moonlight.launch_query_parameters(),
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                warn!("[Stream]: failed to start moonlight stream: {err}");

                #[allow(clippy::single_match)]
                match err {
                    MoonlightClientError::Moonlight(MoonlightError::ConnectionAlreadyExists) => {
                        ipc_sender
                            .send(StreamerIpcMessage::WebSocket(
                                StreamServerMessage::DebugLog { message: "Failed to start stream because this streamer is already streaming".to_string(), ty: None },
                            ))
                            .await;
                    }
                    _ => {}
                }

                return Err(err.into());
            }
        };

        let settings_clone = settings.clone();
        let moonlight_instance = self.moonlight.clone();
        let stream = spawn_blocking(move || {
            moonlight_instance.start_connection(
                stream_config,
                settings_clone,
                connection_listener,
                connection_listener_c,
                video_decoder,
                audio_decoder,
            )
        })
        .await??;

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
                warn!("failed to query video setup information. Giving the browser guessed information");
                VideoSetup { format: VideoFormat::H264, width: settings.width, height: settings.height, redraw_rate: settings.fps }
            });

            let audio = setup.audio.clone().unwrap_or(OpusMultistreamConfig::STEREO);

            (video, audio)
        };

        info!(
            "Stream uses these settings: {:?} with {}x{}x{}",
            video_setup.format, video_setup.width, video_setup.height, video_setup.redraw_rate
        );

        spawn(async move {
            ipc_sender
                .send(StreamerIpcMessage::WebSocket(
                    StreamServerMessage::ConnectionComplete {
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
                    },
                ))
                .await;
        });

        let mut stream_guard = self.stream.write().await;
        stream_guard.replace(stream);
        drop(stream_guard);

        let sender = self.transport_sender.lock().await.clone();
        match sender {
            Some(sender) => {
                sender.on_setup_complete().await;
            }
            None => {
                warn!("No transport found after starting stream. Requesting Termination");
                self.request_terminate().await;
            }
        }

        Ok(())
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

    async fn stop(&self) {
        if self
            .is_terminating
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            debug!("[Stream]: stream is already terminating, won't stop twice");
            return;
        }

        debug!("[Stream]: Stopping...");

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
}

impl ConnectionListener for StreamConnectionListener {
    fn set_hdr_mode(&mut self, hdr_enabled: bool, _sunshine: Option<SunshineHdrMetadata>) {
        info!(
            "[HDR] Host called set_hdr_mode with enabled={}",
            hdr_enabled
        );

        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        stream.clone().runtime.block_on(async move {
            info!("[HDR] Sending HdrModeUpdate to client");
            stream
                .try_send_packet(
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
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        stream.runtime.clone().block_on(async move {
            stream
                .try_send_packet(
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
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        stream.runtime.clone().block_on(async move {
            stream
                .try_send_packet(
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
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        let ipc_sender = stream.ipc_sender.clone();

        stream.runtime.spawn(async move {
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
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        let ipc_sender = stream.ipc_sender.clone();
        ipc_sender.blocking_send(StreamerIpcMessage::WebSocket(
            StreamServerMessage::DebugLog {
                message: format!("Completed Stage: {}", stage.name()),
                ty: None,
            },
        ));
    }

    fn stage_failed(&mut self, stage: Stage, error_code: i32) {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        let ipc_sender = stream.ipc_sender.clone();
        ipc_sender.blocking_send(StreamerIpcMessage::WebSocket(
            StreamServerMessage::DebugLog {
                message: format!(
                    "Failed Stage: {} with error code {}",
                    stage.name(),
                    error_code
                ),
                ty: Some(LogMessageType::Fatal),
            },
        ));
    }

    fn connection_started(&mut self) {}

    fn connection_terminated(&mut self, error_code: i32) {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        let runtime = stream.runtime.clone();
        let ipc_sender = stream.ipc_sender.clone();
        runtime.spawn(async move {
            // Native code invokes this callback synchronously. Schedule both
            // operations onto Tokio so the callback returns before stop()
            // waits for the native connection threads to finish.
            tokio::join!(
                ipc_sender.send(StreamerIpcMessage::WebSocket(
                    StreamServerMessage::ConnectionTerminated { error_code },
                )),
                stream.stop(),
            );
        });
    }

    fn log_message(&mut self, message: &str) {
        info!(target: "moonlight", "{}", message.trim());
    }

    fn connection_status_update(&mut self, status: ConnectionStatus) {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to get stream because it is already deallocated");
            return;
        };

        stream.clone().runtime.block_on(async move {
            stream
                .try_send_packet(
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
