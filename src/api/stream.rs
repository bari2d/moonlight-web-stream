use std::{
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use actix_web::{
    Error, HttpRequest, HttpResponse, get,
    http::header,
    post, rt as actix_rt,
    web::{Bytes, Data, Json, Payload},
};
use actix_ws::{Closed, Message};
use common::{
    api_bindings::{
        LogMessageType, PostCancelRequest, PostCancelResponse, StreamClientMessage,
        StreamServerMessage, TransportChannelId, TransportType,
    },
    ipc::{IpcSender, ServerIpcMessage, StreamerConfig, StreamerIpcMessage, create_child_ipc},
    serialize_json,
};
use log::{debug, error, info, warn};
use tokio::{
    process::{Child, Command},
    spawn,
    sync::{mpsc, oneshot, watch},
    time::timeout,
};
use tracing::{Level, instrument, span};

use crate::app::{
    App, AppError,
    host::{AppId, HostId},
    user::AuthenticatedUser,
};
use crate::web_transport::{WebTransportBridge, WebTransportConnectionState, WebTransportHub};

use super::low_latency_ws::{self, BinarySendOutcome, LowLatencySession};

const IPC_SEND_TIMEOUT: Duration = Duration::from_millis(500);
const WEB_TRANSPORT_SELECTION_TIMEOUT: Duration = Duration::from_secs(5);
const CHILD_STOP_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(11);
const FORWARDER_JOIN_TIMEOUT: Duration = Duration::from_secs(14);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectedTransport {
    WebRtc,
    WebSocket,
    WebTransport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayRoute {
    WebSocket,
    WebTransport,
}

impl From<&TransportType> for SelectedTransport {
    fn from(transport: &TransportType) -> Self {
        match transport {
            TransportType::WebRTC => Self::WebRtc,
            TransportType::WebSocket => Self::WebSocket,
            TransportType::WebTransport => Self::WebTransport,
        }
    }
}

#[get("/host/stream")]
#[instrument(name = "start_host", skip(web_app, user, payload), fields(user_id = %user.id()))]
pub async fn start_host(
    web_app: Data<App>,
    web_transport: Data<Option<WebTransportHub>>,
    mut user: AuthenticatedUser,
    request: HttpRequest,
    payload: Payload,
) -> Result<HttpResponse, Error> {
    let web_transport_origin = single_origin_header(&request);
    let (response, mut session, mut stream) = low_latency_ws::handle(&request, payload)?;

    let client_unique_id = user.host_unique_id().await?;

    let permissions = user.role().await?.permissions().await?;

    let web_app = web_app.clone();
    actix_rt::spawn(async move {
        // -- Init and Configure
        let message;
        loop {
            message = match stream.recv().await {
                Some(Ok(Message::Text(text))) => text,
                Some(Ok(Message::Binary(_))) => {
                    return;
                }
                Some(Ok(_)) => continue,
                Some(Err(_)) => {
                    return;
                }
                None => {
                    return;
                }
            };
            break;
        }

        let message = match serde_json::from_str::<StreamClientMessage>(&message) {
            Ok(value) => value,
            Err(_) => {
                return;
            }
        };

        let StreamClientMessage::Init {
            host_id,
            app_id,
            video_frame_queue_size,
            audio_sample_queue_size,
        } = message
        else {
            let _ = session.close(None);

            warn!("WebSocket didn't send init as first message, closing it");
            return;
        };

        let host_id = HostId(host_id);
        let app_id = AppId(app_id);

        // -- Collect host data
        let mut host = match user.host(host_id).await {
            Ok(host) => host,
            Err(AppError::HostNotFound) => {
                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because the host was not found"
                            .to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
            Err(err) => {
                warn!("failed to start stream for host {host_id:?} (at host): {err}");

                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because of a server error".to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
        };

        let apps = match host.list_apps(&mut user).await {
            Ok(apps) => apps,
            Err(err) => {
                warn!("failed to start stream for host {host_id:?} (at list_apps): {err}");

                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because of a server error".to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
        };

        let Some(app) = apps.into_iter().find(|app| app.id == app_id) else {
            warn!("failed to start stream for host {host_id:?} because the app couldn't be found!");

            let _ = send_ws_message(
                &mut session,
                StreamServerMessage::DebugLog {
                    message: "Failed to start stream because the app was not found".to_string(),
                    ty: Some(LogMessageType::FatalDescription),
                },
            )
            .await;
            let _ = session.close(None);
            return;
        };

        let (address, http_port) = match host.address_port(&mut user).await {
            Ok(address_port) => address_port,
            Err(err) => {
                warn!("failed to start stream for host {host_id:?} (at get address_port): {err}");

                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because of a server error".to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
        };

        let pair_info = match host.pair_info(&mut user).await {
            Ok(pair_info) => pair_info,
            Err(err) => {
                warn!("failed to start stream for host {host_id:?} (at get pair_info): {err}");

                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because the host is not paired"
                            .to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
        };

        // -- Send App info
        let _ = send_ws_message(
            &mut session,
            StreamServerMessage::UpdateApp { app: app.into() },
        )
        .await;

        // -- Starting stage: launch streamer
        let _ = send_ws_message(
            &mut session,
            StreamServerMessage::DebugLog {
                message: "Launching streamer".to_string(),
                ty: None,
            },
        )
        .await;

        // Spawn child
        let (mut child, stdin, stdout) = match Command::new(&web_app.config().streamer_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(mut child) => {
                if let Some(stdin) = child.stdin.take()
                    && let Some(stdout) = child.stdout.take()
                {
                    (child, stdin, stdout)
                } else {
                    error!("[Stream]: streamer process didn't include a stdin or stdout");

                    let _ = send_ws_message(
                        &mut session,
                        StreamServerMessage::DebugLog {
                            message: "Failed to start stream because of a server error".to_string(),
                            ty: Some(LogMessageType::FatalDescription),
                        },
                    )
                    .await;
                    let _ = session.close(None);

                    if let Err(err) = child.kill().await {
                        warn!("[Stream]: failed to kill child: {err}");
                    }

                    return;
                }
            }
            Err(err) => {
                error!("[Stream]: failed to spawn streamer process: {err}");

                let _ = send_ws_message(
                    &mut session,
                    StreamServerMessage::DebugLog {
                        message: "Failed to start stream because of a server error".to_string(),
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )
                .await;
                let _ = session.close(None);
                return;
            }
        };

        // Create ipc
        static CHILD_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = CHILD_COUNTER.fetch_add(1, Ordering::Relaxed);
        let span = span!(Level::INFO, "ipc", child_id = id);

        let (ipc_sender, mut ipc_receiver) =
            create_child_ipc::<ServerIpcMessage, StreamerIpcMessage>(
                span,
                stdin,
                stdout,
                child.stderr.take(),
            )
            .await;

        let relay_route = Arc::new(Mutex::new(RelayRoute::WebSocket));
        let (web_transport_setup_tx, web_transport_setup_rx) = oneshot::channel();
        let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
        let (forwarder_done_tx, mut forwarder_done_rx) = mpsc::channel(1);
        let web_transport_hub = web_transport.get_ref().as_ref().cloned();
        let control_session = session.clone();

        // Redirect streamer output into the selected browser relay. JSON and
        // lifecycle messages always remain on the authenticated WebSocket. The
        // one-use WebTransport credential is intentionally minted only when the
        // streamer emits Setup, immediately before the browser begins transport
        // selection. Host discovery and dynamic ICE work therefore cannot use
        // up the token's lifetime.
        let mut forwarder = spawn({
            let ipc_sender = ipc_sender.clone();
            let relay_route = relay_route.clone();
            let mut web_transport_setup_tx = Some(web_transport_setup_tx);
            let mut session_shutdown_rx = session_shutdown_rx;
            async move {
                let mut web_transport_outbound = None;
                let mut request_child_stop = false;

                'forwarding: loop {
                    let message = tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut session_shutdown_rx) => {
                            request_child_stop = true;
                            break 'forwarding;
                        }
                        message = ipc_receiver.recv() => message,
                    };

                    let Some(message) = message else {
                        break;
                    };
                    match message {
                        StreamerIpcMessage::WebSocket(message) => {
                            if matches!(&message, StreamServerMessage::Setup { .. })
                                && web_transport_setup_tx.is_some()
                            {
                                let bridge = match (
                                    web_transport_hub.as_ref(),
                                    web_transport_origin.as_deref(),
                                ) {
                                    (Some(hub), Some(origin)) => {
                                        match hub.register_for_origin(origin) {
                                            Ok(bridge) => Some(bridge),
                                            Err(err) => {
                                                warn!(
                                                    "[Stream]: failed to register WebTransport bridge: {err}"
                                                );
                                                None
                                            }
                                        }
                                    }
                                    (Some(_), None) => {
                                        warn!(
                                            "[Stream]: WebTransport disabled for a control socket with a missing, invalid, or ambiguous Origin"
                                        );
                                        None
                                    }
                                    (None, _) => None,
                                };

                                let setup_url =
                                    bridge.as_ref().map(|bridge| bridge.setup_url.clone());
                                web_transport_outbound =
                                    bridge.as_ref().map(|bridge| bridge.outbound.clone());

                                let bridge_sender = web_transport_setup_tx
                                    .take()
                                    .expect("WebTransport bridge sender disappeared");
                                if let Err(returned_bridge) = bridge_sender.send(bridge) {
                                    if let Some(returned_bridge) = returned_bridge {
                                        returned_bridge.shutdown();
                                    }
                                    request_child_stop = true;
                                    break 'forwarding;
                                }

                                if let Some(url) = setup_url {
                                    let send_result = tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut session_shutdown_rx) => {
                                            request_child_stop = true;
                                            break 'forwarding;
                                        }
                                        result = send_ws_message(
                                            &mut session,
                                            StreamServerMessage::WebTransportSetup { url },
                                        ) => result,
                                    };
                                    if send_result.is_err() {
                                        warn!(
                                            "[Ipc]: control WebSocket closed while advertising WebTransport"
                                        );
                                        request_child_stop = true;
                                        break 'forwarding;
                                    }
                                }
                            }

                            let send_result = tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut session_shutdown_rx) => {
                                    request_child_stop = true;
                                    break 'forwarding;
                                }
                                result = send_ws_message(&mut session, message) => result,
                            };
                            if send_result.is_err() {
                                warn!(
                                    "[Ipc]: control WebSocket closed while forwarding a streamer message"
                                );
                                request_child_stop = true;
                                break 'forwarding;
                            }
                        }
                        StreamerIpcMessage::WebSocketTransport(data) => {
                            let mut data = Some(data);
                            let websocket_result = {
                                // This lock is also held while WebTransport is
                                // selected and the old WebSocket media slots are
                                // cleared. A frame can therefore land entirely
                                // before that barrier or entirely after it, but
                                // never race back into the old session.
                                let route = relay_route
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                if *route == RelayRoute::WebSocket {
                                    Some(
                                        session
                                            .binary(data.take().expect("relay frame disappeared")),
                                    )
                                } else {
                                    None
                                }
                            };

                            if let Some(websocket_result) = websocket_result {
                                match websocket_result {
                                    Ok(
                                        BinarySendOutcome::Enqueued | BinarySendOutcome::Dropped,
                                    ) => {}
                                    Ok(BinarySendOutcome::NeedIdr) => {
                                        if !send_ipc_bounded(
                                            &ipc_sender,
                                            ServerIpcMessage::WebSocketTransport(
                                                Bytes::from_static(&[
                                                    TransportChannelId::HOST_VIDEO,
                                                    0,
                                                ]),
                                            ),
                                        )
                                        .await
                                        {
                                            warn!(
                                                "[Ipc]: failed to request an IDR after WebSocket video congestion"
                                            );
                                            request_child_stop = true;
                                            break 'forwarding;
                                        }
                                    }
                                    Err(_) => {
                                        warn!(
                                            "[Ipc]: control WebSocket closed while forwarding transport data"
                                        );
                                        request_child_stop = true;
                                        break 'forwarding;
                                    }
                                }
                            } else {
                                let data = data.expect("WebTransport relay frame disappeared");
                                let send_result = match web_transport_outbound.as_ref() {
                                    Some(sender) => tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut session_shutdown_rx) => {
                                            request_child_stop = true;
                                            break 'forwarding;
                                        }
                                        result = sender.send(data) => result.map_err(|_| ()),
                                    },
                                    None => Err(()),
                                };
                                if send_result.is_err() {
                                    warn!(
                                        "[Ipc]: WebTransport relay closed after selection; ending the control session"
                                    );
                                    request_child_stop = true;
                                    break 'forwarding;
                                }
                            }
                        }
                        StreamerIpcMessage::Stop => {
                            debug!("[Ipc]: ipc receiver stopped by streamer");
                            break;
                        }
                    }
                }
                if let Some(bridge_sender) = web_transport_setup_tx.take() {
                    let _ = bridge_sender.send(None);
                }
                if request_child_stop {
                    let _ = timeout(
                        CHILD_STOP_SEND_TIMEOUT,
                        ipc_sender.send(ServerIpcMessage::Stop),
                    )
                    .await;
                }

                // Closing the authenticated control socket is the browser's
                // signal to discard this entire relay session and reconnect.
                if request_child_stop {
                    let _ = session.close_now(None);
                } else {
                    let _ = session.close(None);
                }
                let _ = forwarder_done_tx.try_send(());
                info!("[Ipc]: ipc receiver is closed");

                reap_streamer_child(&mut child).await;
            }
        });

        // Send init into ipc. A wedged child must not hold the authenticated
        // browser session open forever.
        let init_sent = send_ipc_bounded(
            &ipc_sender,
            ServerIpcMessage::Init {
                config: StreamerConfig {
                    webrtc: web_app.config().webrtc.clone(),
                    log_level: web_app.config().log.level_filter,
                },
                host_address: address,
                host_http_port: http_port,
                client_unique_id: Some(client_unique_id),
                client_private_key: pair_info.client_private_key,
                client_certificate: pair_info.client_certificate,
                server_certificate: pair_info.server_certificate,
                app_id: app_id.0,
                video_frame_queue_size,
                audio_sample_queue_size,
                permissions,
            },
        )
        .await;
        if !init_sent {
            warn!("[Stream]: streamer IPC stalled while sending Init");
        }

        let mut web_transport_setup_rx = Some(web_transport_setup_rx);
        let mut web_transport_bridge: Option<WebTransportBridge> = None;
        let mut web_transport_inbound: Option<mpsc::Receiver<Bytes>> = None;
        let mut web_transport_state = None;
        let mut web_transport_shutdown = None;

        // Redirect control WebSocket and WebTransport input into IPC. Once
        // WebTransport is selected, this session cannot switch its bytes back
        // to WebSocket: fallback always uses a fresh authenticated session.
        let mut selected_transport = None;
        'control: while init_sent {
            tokio::select! {
                biased;
                bridge = receive_web_transport_bridge(&mut web_transport_setup_rx) => {
                    web_transport_setup_rx = None;
                    match bridge {
                        Ok(Some(mut bridge)) => {
                            if selected_transport.is_some() {
                                bridge.shutdown();
                                continue 'control;
                            }
                            web_transport_state = Some(bridge.state.clone());
                            web_transport_shutdown = Some(bridge.shutdown_signal());
                            let (_unused_sender, empty_receiver) = mpsc::channel(1);
                            web_transport_inbound = Some(std::mem::replace(
                                &mut bridge.inbound,
                                empty_receiver,
                            ));
                            web_transport_bridge = Some(bridge);
                        }
                        Ok(None) => {}
                        Err(_) => {
                            warn!("[Stream]: streamer ended before WebTransport setup");
                        }
                    }
                }
                _ = forwarder_done_rx.recv() => {
                    break 'control;
                }
                state = receive_web_transport_state(&mut web_transport_state) => {
                    match state {
                        Some(WebTransportConnectionState::Connected) => {}
                        Some(WebTransportConnectionState::Waiting) => {}
                        Some(WebTransportConnectionState::Closed) | None => {
                            web_transport_state = None;
                            web_transport_inbound = None;
                            web_transport_shutdown = None;
                            if let Some(bridge) = web_transport_bridge.take() {
                                bridge.shutdown();
                            }
                            if selected_transport == Some(SelectedTransport::WebTransport) {
                                warn!(
                                    "[Stream]: WebTransport connection closed after selection"
                                );
                                break 'control;
                            }
                        }
                    }
                }
                shutdown = receive_web_transport_shutdown(&mut web_transport_shutdown) => {
                    web_transport_shutdown = None;
                    if shutdown.unwrap_or(true) {
                        web_transport_inbound = None;
                        web_transport_state = None;
                        if let Some(bridge) = web_transport_bridge.take() {
                            bridge.shutdown();
                        }
                        if selected_transport == Some(SelectedTransport::WebTransport) {
                            warn!(
                                "[Stream]: WebTransport bridge shut down after selection"
                            );
                            break 'control;
                        }
                    }
                }
                message = stream.recv() => {
                    let Some(Ok(message)) = message else {
                        break 'control;
                    };
                    match message {
                        Message::Text(text) => {
                            let Ok(message) = serde_json::from_str::<StreamClientMessage>(&text) else {
                                warn!("[Stream]: failed to deserialize from json");
                                break 'control;
                            };

                            if let StreamClientMessage::SetTransport(transport) = &message {
                                let requested = SelectedTransport::from(transport);
                                if let Some(selected) = selected_transport {
                                    if selected == requested {
                                        // Transport creation is not idempotent in
                                        // the child. Ignore duplicate selection
                                        // instead of recreating it in place.
                                        continue 'control;
                                    }
                                    warn!(
                                        "[Stream]: refusing an in-place transport switch from {selected:?} to {requested:?}"
                                    );
                                    break 'control;
                                }

                                if requested == SelectedTransport::WebTransport {
                                    let connected = match web_transport_bridge.as_ref() {
                                        Some(bridge) => tokio::select! {
                                            result = timeout(
                                                WEB_TRANSPORT_SELECTION_TIMEOUT,
                                                wait_for_web_transport_connection(bridge.state.clone()),
                                            ) => result.unwrap_or(false),
                                            _ = forwarder_done_rx.recv() => false,
                                        },
                                        None => false,
                                    };
                                    if !connected {
                                        warn!(
                                            "[Stream]: WebTransport was selected without a live bridge; ending the session"
                                        );
                                        break 'control;
                                    }

                                    let mut route = relay_route
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                                    *route = RelayRoute::WebTransport;
                                    control_session.clear_media();
                                } else {
                                    // A one-use bearer URL that will never be
                                    // consumed should not remain valid until TTL.
                                    web_transport_inbound = None;
                                    web_transport_state = None;
                                    web_transport_shutdown = None;
                                    if let Some(bridge) = web_transport_bridge.take() {
                                        bridge.shutdown();
                                    }
                                }
                                selected_transport = Some(requested);
                            }
                            if !send_ipc_bounded(
                                &ipc_sender,
                                ServerIpcMessage::WebSocket(message),
                            )
                            .await
                            {
                                warn!("[Stream]: streamer IPC stalled on a control message");
                                break 'control;
                            }
                        }
                        Message::Binary(binary) => {
                            // Binary frames belong to the WebSocket fallback.
                            if selected_transport == Some(SelectedTransport::WebSocket)
                                && !send_ipc_bounded(
                                    &ipc_sender,
                                    ServerIpcMessage::WebSocketTransport(binary),
                                )
                                .await
                            {
                                warn!("[Stream]: streamer IPC stalled on WebSocket input");
                                break 'control;
                            }
                        }
                        Message::Ping(bytes) => {
                            if control_session.pong(&bytes).is_err() {
                                break 'control;
                            }
                        }
                        Message::Close(_) => break 'control,
                        _ => {}
                    }
                }
                inbound = async {
                    match web_transport_inbound.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match inbound {
                        Some(frame) if selected_transport == Some(SelectedTransport::WebTransport) => {
                            // The select is biased toward state/shutdown above,
                            // so queued input can never replay after QUIC close.
                            if !send_ipc_bounded(
                                &ipc_sender,
                                ServerIpcMessage::WebSocketTransport(frame),
                            )
                            .await
                            {
                                warn!("[Stream]: streamer IPC stalled on WebTransport input");
                                break 'control;
                            }
                        }
                        Some(_) => {}
                        None => {
                            web_transport_inbound = None;
                            web_transport_state = None;
                            web_transport_shutdown = None;
                            if let Some(bridge) = web_transport_bridge.take() {
                                bridge.shutdown();
                            }
                            if selected_transport == Some(SelectedTransport::WebTransport) {
                                warn!(
                                    "[Stream]: WebTransport input relay closed after selection"
                                );
                                break 'control;
                            }
                        },
                    }
                }
            }
        }

        let _ = control_session.close(None);
        session_shutdown_tx.send_replace(true);
        if let Some(bridge) = web_transport_bridge.as_ref() {
            bridge.shutdown();
        }
        let _ = send_ipc_bounded(&ipc_sender, ServerIpcMessage::Stop).await;

        match timeout(FORWARDER_JOIN_TIMEOUT, &mut forwarder).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("[Stream]: streamer forwarder failed: {err}"),
            Err(_) => {
                warn!("[Stream]: streamer forwarder did not terminate; aborting it");
                forwarder.abort();
                let _ = forwarder.await;
            }
        }

        // The forwarder may have registered a bridge just as the control
        // socket closed, leaving it queued in the one-shot receiver rather than
        // installed above. Explicitly shut that bridge down too.
        if let Some(mut receiver) = web_transport_setup_rx
            && let Ok(Some(bridge)) = receiver.try_recv()
        {
            bridge.shutdown();
        }
    });

    Ok(response)
}

fn single_origin_header(request: &HttpRequest) -> Option<String> {
    let mut origins = request.headers().get_all(header::ORIGIN);
    let origin = origins.next()?.to_str().ok()?.to_owned();
    if origins.next().is_some() {
        return None;
    }
    Some(origin)
}

async fn send_ws_message(
    sender: &LowLatencySession,
    message: StreamServerMessage,
) -> Result<(), Closed> {
    let Some(json) = serialize_json(&message) else {
        return Ok(());
    };

    sender.text(json)
}

async fn send_ipc_bounded(sender: &IpcSender<ServerIpcMessage>, message: ServerIpcMessage) -> bool {
    matches!(
        timeout(IPC_SEND_TIMEOUT, sender.send_checked(message)).await,
        Ok(Ok(()))
    )
}

async fn wait_for_shutdown(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn receive_web_transport_bridge(
    receiver: &mut Option<oneshot::Receiver<Option<WebTransportBridge>>>,
) -> Result<Option<WebTransportBridge>, oneshot::error::RecvError> {
    match receiver {
        Some(receiver) => receiver.await,
        None => std::future::pending().await,
    }
}

async fn receive_web_transport_state(
    receiver: &mut Option<watch::Receiver<WebTransportConnectionState>>,
) -> Option<WebTransportConnectionState> {
    let Some(receiver) = receiver else {
        return std::future::pending().await;
    };
    receiver.changed().await.ok()?;
    Some(*receiver.borrow_and_update())
}

async fn receive_web_transport_shutdown(
    receiver: &mut Option<watch::Receiver<bool>>,
) -> Option<bool> {
    let Some(receiver) = receiver else {
        return std::future::pending().await;
    };
    receiver.changed().await.ok()?;
    Some(*receiver.borrow_and_update())
}

async fn wait_for_web_transport_connection(
    mut receiver: watch::Receiver<WebTransportConnectionState>,
) -> bool {
    loop {
        match *receiver.borrow_and_update() {
            WebTransportConnectionState::Connected => return true,
            WebTransportConnectionState::Closed => return false,
            WebTransportConnectionState::Waiting => {}
        }
        if receiver.changed().await.is_err() {
            return false;
        }
    }
}

async fn reap_streamer_child(child: &mut Child) {
    match timeout(CHILD_REAP_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) => debug!("[Stream]: streamer child exited with {status}"),
        Ok(Err(err)) => warn!("[Stream]: failed waiting for streamer child: {err}"),
        Err(_) => {
            warn!("[Stream]: streamer child did not exit after Stop; killing it");
            if let Err(err) = child.kill().await {
                warn!("[Stream]: failed to kill streamer child: {err}");
            }
        }
    }
}

#[post("/host/cancel")]
pub async fn cancel_host(
    mut user: AuthenticatedUser,
    Json(request): Json<PostCancelRequest>,
) -> Result<Json<PostCancelResponse>, AppError> {
    let host_id = HostId(request.host_id);

    let mut host = user.host(host_id).await?;

    host.cancel_app(&mut user).await?;

    Ok(Json(PostCancelResponse { success: true }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn web_transport_requires_exactly_one_control_origin() {
        let missing = TestRequest::default().to_http_request();
        assert_eq!(single_origin_header(&missing), None);

        let one = TestRequest::default()
            .append_header((header::ORIGIN, "https://example.test"))
            .to_http_request();
        assert_eq!(
            single_origin_header(&one).as_deref(),
            Some("https://example.test")
        );

        let ambiguous = TestRequest::default()
            .append_header((header::ORIGIN, "https://example.test"))
            .append_header((header::ORIGIN, "https://evil.test"))
            .to_http_request();
        assert_eq!(single_origin_header(&ambiguous), None);
    }
}
