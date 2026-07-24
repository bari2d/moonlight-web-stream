use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use actix_http::uri::Authority;
use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use common::{api_bindings::TransportChannelId, config::WebTransportConfig, ipc::NetworkFeedback};
use openssl::rand::rand_bytes;
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, mpsc, watch};
use tracing::{info, warn};
use wtransport::{Connection, Endpoint, Identity, ServerConfig, VarInt, endpoint::IncomingSession};

const PROTOCOL_VERSION: u8 = 4;
const DRAFT02_REQUEST_HEADER: &str = "sec-webtransport-http3-draft02";
const DRAFT02_RESPONSE_HEADER: &str = "sec-webtransport-http3-draft";
const DRAFT02_RESPONSE_VALUE: &str = "draft02";
const LANE_VIDEO: u8 = 1;
const LANE_AUDIO: u8 = 2;
const LANE_OTHER: u8 = 3;
const LANE_RELIABLE_INBOUND: u8 = 4;

const DATAGRAM_MAGIC: u8 = 0xf0;
const DATAGRAM_RELATIVE_MOTION: u8 = 0;
const DATAGRAM_HIGH_RES_SCROLL: u8 = 1;
const DATAGRAM_SNAPSHOT: u8 = 2;

const OUTBOUND_QUEUE_CAPACITY: usize = 32;
const INBOUND_QUEUE_CAPACITY: usize = 64;
const OTHER_QUEUE_CAPACITY: usize = 32;
const VIDEO_QUEUE_CAPACITY: usize = 4;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_RELIABLE_INBOUND_FRAME_BYTES: usize = 128 * 1024;
// Keep several frame writes live so one flow-controlled stream cannot stall
// every later frame. The byte cap below still bounds application-owned data.
const MAX_VIDEO_IN_FLIGHT_STREAMS: usize = 8;
const MAX_VIDEO_IN_FLIGHT_BYTES: usize = 32 * 1024 * 1024;
// Ordinary deltas may use only part of the writer window. The remaining stream
// and byte budget lets a recovery IDR overtake a congested GOP.
const MAX_VIDEO_DELTA_IN_FLIGHT_STREAMS: usize = MAX_VIDEO_IN_FLIGHT_STREAMS - 1;
const MAX_VIDEO_DELTA_IN_FLIGHT_BYTES: usize = MAX_VIDEO_IN_FLIGHT_BYTES - MAX_FRAME_BYTES;
const MAX_DATAGRAM_BYTES: usize = 64 * 1024;
const MAX_SPLIT_PACKETS: usize = 64;
const MAX_PENDING_TOKENS: usize = 1_024;
const CLIENT_LANE_SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_RELIABLE_BODY_TIMEOUT: Duration = Duration::from_secs(2);
const VIDEO_STREAM_OPEN_TIMEOUT: Duration = Duration::from_millis(500);
const VIDEO_STREAM_WRITE_INACTIVITY_TIMEOUT: Duration = Duration::from_millis(500);
const VIDEO_IDR_MAX_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(2);
const VIDEO_IDR_RTT_TIMEOUT_MULTIPLIER: u32 = 4;
// WebTransport stream application errors occupy a remapped HTTP/3 range.
// Send the mapped wire values here so the browser can recover the original
// application codes through WebTransportError.streamErrorCode.
const WEBTRANSPORT_APPLICATION_ERROR_FIRST: u64 = 0x52e4_a40f_a8db;
const VIDEO_STREAM_TIMEOUT_APPLICATION_CODE: u32 = 0x10;
const VIDEO_STREAM_SUPERSEDED_APPLICATION_CODE: u32 = 0x11;
const NETWORK_FEEDBACK_INTERVAL: Duration = Duration::from_millis(500);

fn webtransport_application_error_code(code: u32) -> VarInt {
    let code = u64::from(code);
    // WebTransport-over-HTTP/3 section 4.3 skips every grease value in
    // the reserved application-error range.
    VarInt::try_from_u64(WEBTRANSPORT_APPLICATION_ERROR_FIRST + code + code / 0x1e)
        .expect("mapped WebTransport application error must fit in a QUIC varint")
}

fn video_stream_timeout_code() -> VarInt {
    webtransport_application_error_code(VIDEO_STREAM_TIMEOUT_APPLICATION_CODE)
}

fn video_stream_superseded_code() -> VarInt {
    webtransport_application_error_code(VIDEO_STREAM_SUPERSEDED_APPLICATION_CODE)
}

// Quinn transmits locally buffered data from higher-priority streams first.
// Audio and control must not sit behind bulk video retransmissions, and an IDR
// must be able to overtake ordinary deltas during recovery.
const PRIORITY_VIDEO_DELTA: i32 = 0;
const PRIORITY_VIDEO_IDR: i32 = 10;
const PRIORITY_RELIABLE_OTHER: i32 = 20;
const PRIORITY_AUDIO: i32 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebTransportConnectionState {
    Waiting,
    Connected,
    Closed,
}

/// The WebSocket/control-session side of one WebTransport connection.
///
/// `outbound` and `inbound` carry the existing channel-prefixed wire frames,
/// so the streamer does not need a second media protocol. Dropping the control
/// session should be paired with a call to [`Self::shutdown`].
pub struct WebTransportBridge {
    pub setup_url: String,
    pub outbound: mpsc::Sender<Bytes>,
    pub inbound: mpsc::Receiver<Bytes>,
    pub state: watch::Receiver<WebTransportConnectionState>,
    pub feedback: watch::Receiver<NetworkFeedback>,
    shutdown_tx: watch::Sender<bool>,
}

impl fmt::Debug for WebTransportBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The setup URL contains a bearer token and must never reach a log.
        formatter
            .debug_struct("WebTransportBridge")
            .field("setup_url", &"<redacted>")
            .field("state", &*self.state.borrow())
            .field("feedback", &*self.feedback.borrow())
            .finish_non_exhaustive()
    }
}

impl WebTransportBridge {
    pub fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
    }

    /// Allows the control session to observe a transport-side close and close
    /// itself in response.
    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

#[derive(Clone)]
pub struct WebTransportHub {
    inner: Arc<HubInner>,
}

impl fmt::Debug for WebTransportHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebTransportHub")
            .field("authority", &self.inner.endpoint.authority)
            .field("path", &self.inner.endpoint.path)
            .finish_non_exhaustive()
    }
}

struct HubInner {
    endpoint: PublicEndpoint,
    token_ttl: Duration,
    tokens: Mutex<TokenStore>,
    shutdown_tx: watch::Sender<bool>,
}

struct PendingBridge {
    expires_at: Instant,
    expected_origin: String,
    outbound_rx: mpsc::Receiver<Bytes>,
    inbound_tx: mpsc::Sender<Bytes>,
    state_tx: watch::Sender<WebTransportConnectionState>,
    feedback_tx: watch::Sender<NetworkFeedback>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

#[derive(Default)]
struct TokenStore {
    entries: HashMap<[u8; 32], PendingBridge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenConsumeRejection {
    Unknown,
    OriginMismatch,
}

impl TokenStore {
    fn purge_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    fn insert(&mut self, hash: [u8; 32], entry: PendingBridge, now: Instant) -> Result<()> {
        self.purge_expired(now);
        if self.entries.len() >= MAX_PENDING_TOKENS {
            bail!("too many pending WebTransport registrations");
        }
        self.entries.insert(hash, entry);
        Ok(())
    }

    fn consume(
        &mut self,
        hash: &[u8; 32],
        origin: &str,
        now: Instant,
    ) -> std::result::Result<PendingBridge, TokenConsumeRejection> {
        self.purge_expired(now);
        let Some(entry) = self.entries.get(hash) else {
            return Err(TokenConsumeRejection::Unknown);
        };
        let is_shutdown = *entry.shutdown_rx.borrow();
        let origin_matches = entry.expected_origin == origin;

        if is_shutdown {
            self.entries.remove(hash);
            return Err(TokenConsumeRejection::Unknown);
        }
        if !origin_matches {
            // Do not burn a valid one-use credential when a request from the
            // wrong site reaches the endpoint first.
            return Err(TokenConsumeRejection::OriginMismatch);
        }

        self.entries
            .remove(hash)
            .ok_or(TokenConsumeRejection::Unknown)
    }
}

#[derive(Debug, Clone)]
struct PublicEndpoint {
    public_url: String,
    authority: String,
    host: String,
    port: u16,
    path: String,
}

impl PublicEndpoint {
    fn parse(public_url: &str) -> Result<Self> {
        let remainder = public_url
            .strip_prefix("https://")
            .ok_or_else(|| anyhow!("WebTransport public_url must use https"))?;
        if remainder.contains('#') || remainder.contains('?') {
            bail!("WebTransport public_url must not contain a query or fragment");
        }

        let (authority, path) = match remainder.find('/') {
            Some(index) => (&remainder[..index], &remainder[index..]),
            None => (remainder, "/"),
        };
        if authority.is_empty() || authority.contains('@') {
            bail!("WebTransport public_url has an invalid authority");
        }
        let parsed_authority = authority
            .parse::<Authority>()
            .context("WebTransport public_url has an invalid authority")?;
        let Some(port) = parsed_authority.port_u16() else {
            bail!("WebTransport public_url must explicitly include a port");
        };
        if !path.starts_with('/') || path.contains('\r') || path.contains('\n') {
            bail!("WebTransport public_url has an invalid path");
        }

        let public_url = if path == "/" && !public_url.ends_with('/') {
            format!("{public_url}/")
        } else {
            public_url.to_owned()
        };
        Ok(Self {
            public_url,
            authority: authority.to_owned(),
            host: parsed_authority.host().to_owned(),
            port,
            path: path.to_owned(),
        })
    }

    fn setup_url(&self, token: &str) -> String {
        format!("{}?v={PROTOCOL_VERSION}&token={token}", self.public_url)
    }

    fn validate_request(&self, authority: &str, path: &str) -> Option<[u8; 32]> {
        if !self.matches_authority(authority) {
            return None;
        }

        let (request_path, query) = path.split_once('?')?;
        if request_path != self.path {
            return None;
        }

        let mut version_ok = false;
        let mut token = None;
        for field in query.split('&') {
            let (name, value) = field.split_once('=')?;
            match name {
                "v" if !version_ok && value == PROTOCOL_VERSION.to_string() => version_ok = true,
                "token" if token.is_none() => token = decode_token(value),
                _ => return None,
            }
        }
        version_ok.then_some(token?)
    }

    fn matches_authority(&self, authority: &str) -> bool {
        if authority.contains('@') {
            return false;
        }
        let Ok(authority) = authority.parse::<Authority>() else {
            return false;
        };
        authority.host().eq_ignore_ascii_case(&self.host)
            && authority.port_u16().unwrap_or(443) == self.port
    }
}

/// Validates and canonicalizes a browser Origin without accepting a URL path,
/// credentials, query, fragment, opaque origin, or non-TLS scheme.
fn canonicalize_https_origin(origin: &str) -> Option<String> {
    let (scheme, authority_text) = origin.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("https")
        || authority_text.is_empty()
        || authority_text.contains(&['/', '?', '#', '@'][..])
    {
        return None;
    }

    let authority = authority_text.parse::<Authority>().ok()?;
    let host = authority.host();
    if host.is_empty() {
        return None;
    }
    let explicit_port = if authority_text.starts_with('[') {
        let closing_bracket = authority_text.find(']')?;
        match &authority_text[closing_bracket + 1..] {
            "" => None,
            suffix => Some(suffix.strip_prefix(':')?.parse::<u16>().ok()?),
        }
    } else {
        match authority_text.rsplit_once(':') {
            Some((_, port)) => Some(port.parse::<u16>().ok()?),
            None => None,
        }
    };
    let port = explicit_port.unwrap_or(443);
    let host = host.to_ascii_lowercase();
    let host = if host.starts_with('[') && host.ends_with(']') {
        host
    } else if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    Some(if port == 443 {
        format!("https://{host}")
    } else {
        format!("https://{host}:{port}")
    })
}

/// Starts the process-wide UDP/HTTP3 endpoint. Disabled configuration is a
/// successful no-op, making integration into the existing server startup easy.
pub async fn start(config: WebTransportConfig) -> Result<Option<WebTransportHub>> {
    if !config.enabled {
        return Ok(None);
    }
    if config.token_ttl.is_zero() {
        bail!("WebTransport token_ttl must be greater than zero");
    }

    let public_endpoint = PublicEndpoint::parse(&config.public_url)?;
    let identity = Identity::load_pemfiles(&config.certificate_pem, &config.private_key_pem)
        .await
        .context("failed to load WebTransport TLS identity")?;
    let server_config = ServerConfig::builder()
        .with_bind_address(config.bind_address)
        .with_identity(identity)
        .max_idle_timeout(Some(Duration::from_secs(20)))
        .context("invalid WebTransport idle timeout")?
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .allow_migration(true)
        .build();
    let endpoint =
        Endpoint::server(server_config).context("failed to bind WebTransport endpoint")?;
    let local_address = endpoint.local_addr()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let inner = Arc::new(HubInner {
        endpoint: public_endpoint,
        token_ttl: config.token_ttl,
        tokens: Mutex::new(TokenStore::default()),
        shutdown_tx,
    });
    tokio::spawn(run_listener(endpoint, inner.clone(), shutdown_rx));
    info!(address = %local_address, "WebTransport endpoint listening");

    Ok(Some(WebTransportHub { inner }))
}

impl WebTransportHub {
    /// Registers one short-lived, one-use bridge for the authenticated control
    /// session's browser Origin and returns its bearer URL. Only the SHA-256
    /// digest of the random token remains in server memory.
    pub fn register_for_origin(&self, origin: &str) -> Result<WebTransportBridge> {
        let expected_origin = canonicalize_https_origin(origin)
            .ok_or_else(|| anyhow!("control session did not have a valid HTTPS Origin"))?;
        let mut token = [0_u8; 32];
        rand_bytes(&mut token).context("failed to generate WebTransport token")?;
        let token_hash = hash_token(&token);
        let token_text = encode_token(&token);

        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (state_tx, state_rx) = watch::channel(WebTransportConnectionState::Waiting);
        let (feedback_tx, feedback_rx) = watch::channel(NetworkFeedback::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let now = Instant::now();
        let entry = PendingBridge {
            expires_at: now + self.inner.token_ttl,
            expected_origin,
            outbound_rx,
            inbound_tx,
            state_tx,
            feedback_tx,
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx,
        };
        self.inner
            .tokens
            .lock()
            .map_err(|_| anyhow!("WebTransport token registry is unavailable"))?
            .insert(token_hash, entry, now)?;
        info!("WebTransport one-use bridge registered");

        Ok(WebTransportBridge {
            setup_url: self.inner.endpoint.setup_url(&token_text),
            outbound: outbound_tx,
            inbound: inbound_rx,
            state: state_rx,
            feedback: feedback_rx,
            shutdown_tx,
        })
    }

    pub fn shutdown(&self) {
        self.inner.shutdown_tx.send_replace(true);
    }
}

async fn run_listener(
    endpoint: Endpoint<wtransport::endpoint::endpoint_side::Server>,
    inner: Arc<HubInner>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                endpoint.close(VarInt::from_u32(0), b"server shutdown");
                break;
            }
            incoming = endpoint.accept() => {
                tokio::spawn(handle_incoming(incoming, inner.clone()));
            }
        }
    }
    endpoint.wait_idle().await;
}

async fn handle_incoming(incoming: IncomingSession, inner: Arc<HubInner>) {
    let request = match incoming.await {
        Ok(request) => request,
        Err(_) => {
            warn!("WebTransport handshake failed before an HTTP/3 request was established");
            return;
        }
    };
    let remote = request.remote_address();
    info!(remote = %remote, "WebTransport HTTP/3 request received");
    let Some(token_hash) = inner
        .endpoint
        .validate_request(request.authority(), request.path())
    else {
        warn!(
            remote = %remote,
            authority_matches = inner.endpoint.matches_authority(request.authority()),
            "WebTransport request rejected before token lookup"
        );
        request.forbidden().await;
        return;
    };

    let Some(origin) = request.origin().and_then(canonicalize_https_origin) else {
        warn!(
            remote = %remote,
            origin_present = request.origin().is_some(),
            "WebTransport request had a missing or invalid Origin"
        );
        request.forbidden().await;
        return;
    };

    // Reduce the mutex result to a guard-free value before any await point.
    let pending = {
        match inner.tokens.lock() {
            Ok(mut tokens) => Some(tokens.consume(&token_hash, &origin, Instant::now())),
            Err(_) => None,
        }
    };
    let Some(pending) = pending else {
        warn!(remote = %remote, "WebTransport token registry was unavailable");
        request.forbidden().await;
        return;
    };
    let pending = match pending {
        Ok(pending) => pending,
        Err(TokenConsumeRejection::Unknown) => {
            warn!(remote = %remote, "WebTransport token was unknown, expired, or already used");
            request.forbidden().await;
            return;
        }
        Err(TokenConsumeRejection::OriginMismatch) => {
            warn!(remote = %remote, "WebTransport token did not belong to the requesting Origin");
            request.forbidden().await;
            return;
        }
    };

    // Chromium-family clients still advertise the draft-02 negotiation
    // request field. Echo the corresponding response field for older Chrome,
    // Brave, and managed Chromebooks that otherwise reject a valid 200 before
    // exposing `transport.ready`. The patched protocol dependency guarantees
    // `:status` is encoded ahead of this regular header.
    let draft02_requested = requests_draft02_response(request.headers());
    let accepted = if draft02_requested {
        request
            .accept_with_headers([(DRAFT02_RESPONSE_HEADER, DRAFT02_RESPONSE_VALUE)])
            .await
    } else {
        request.accept().await
    };
    let connection = match accepted {
        Ok(connection) => connection,
        Err(error) => {
            warn!(remote = %remote, error = %error, "WebTransport request acceptance failed");
            pending.shutdown_tx.send_replace(true);
            pending
                .state_tx
                .send_replace(WebTransportConnectionState::Closed);
            return;
        }
    };
    info!(remote = %remote, "WebTransport session accepted");

    if let Err(failure) = run_connection(connection, pending).await {
        // Transport/stream errors do not contain the CONNECT request path, so
        // retaining the source is safe and makes a real QUIC close distinguishable
        // from whichever lane happened to observe it first.
        warn!(
            remote = %remote,
            stage = %failure.stage,
            error = %failure.source,
            "WebTransport session ended with an error"
        );
    }
}

fn requests_draft02_response(headers: &HashMap<String, String>) -> bool {
    headers
        .get(DRAFT02_REQUEST_HEADER)
        .is_some_and(|value| value == "1")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionStage {
    WaitReliableClientLane,
    OpenAudioLane,
    OpenReliableServerLane,
    DispatchOutbound,
    WriteVideo,
    WriteAudio,
    WriteReliableServer,
    ReadReliableClient,
    ReadDatagrams,
}

#[derive(Debug)]
struct ConnectionFailure {
    stage: ConnectionStage,
    source: anyhow::Error,
}

impl ConnectionFailure {
    fn new(stage: ConnectionStage, source: impl Into<anyhow::Error>) -> Self {
        Self {
            stage,
            source: source.into(),
        }
    }
}

impl fmt::Display for ConnectionStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WaitReliableClientLane => "wait_reliable_client_lane",
            Self::OpenAudioLane => "open_audio_lane",
            Self::OpenReliableServerLane => "open_reliable_server_lane",
            Self::DispatchOutbound => "dispatch_outbound",
            Self::WriteVideo => "write_video",
            Self::WriteAudio => "write_audio",
            Self::WriteReliableServer => "write_reliable_server",
            Self::ReadReliableClient => "read_reliable_client",
            Self::ReadDatagrams => "read_datagrams",
        })
    }
}

#[derive(Debug, Default)]
struct CongestionCounters {
    admission_drops: AtomicU64,
    video_write_timeouts: AtomicU64,
    recovery_requests: AtomicU64,
}

impl CongestionCounters {
    fn snapshot(
        &self,
        rtt: Duration,
        sent_packets: u64,
        lost_packets: u64,
        congestion_events: u64,
    ) -> NetworkFeedback {
        NetworkFeedback {
            rtt_ms: u32::try_from(rtt.as_millis()).unwrap_or(u32::MAX),
            sent_packets,
            lost_packets,
            congestion_events,
            admission_drops: self.admission_drops.load(Ordering::Relaxed),
            video_write_timeouts: self.video_write_timeouts.load(Ordering::Relaxed),
            recovery_requests: self.recovery_requests.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct RecoveryRequestState {
    enqueued: AtomicBool,
}

impl RecoveryRequestState {
    fn clear(&self) {
        self.enqueued.store(false, Ordering::Release);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct VideoWriterBudget {
    streams: usize,
    bytes: usize,
    delta_streams: usize,
    delta_bytes: usize,
}

impl VideoWriterBudget {
    fn can_admit(&self, frame_bytes: usize, is_idr: bool) -> bool {
        if frame_bytes > MAX_VIDEO_IN_FLIGHT_BYTES
            || self.streams >= MAX_VIDEO_IN_FLIGHT_STREAMS
            || self.bytes.saturating_add(frame_bytes) > MAX_VIDEO_IN_FLIGHT_BYTES
        {
            return false;
        }
        is_idr
            || (self.delta_streams < MAX_VIDEO_DELTA_IN_FLIGHT_STREAMS
                && self.delta_bytes.saturating_add(frame_bytes) <= MAX_VIDEO_DELTA_IN_FLIGHT_BYTES)
    }

    fn admit(&mut self, frame_bytes: usize, is_idr: bool) {
        self.streams += 1;
        self.bytes += frame_bytes;
        if !is_idr {
            self.delta_streams += 1;
            self.delta_bytes += frame_bytes;
        }
    }

    fn release(&mut self, frame_bytes: usize, is_idr: bool) {
        self.streams = self.streams.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(frame_bytes);
        if !is_idr {
            self.delta_streams = self.delta_streams.saturating_sub(1);
            self.delta_bytes = self.delta_bytes.saturating_sub(frame_bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliableReplacementKey {
    StatsVideo,
    StatsRtt,
    StatsBrowserRtt,
    Rtt,
}

fn reliable_replacement_key(frame: &[u8]) -> Option<ReliableReplacementKey> {
    match *frame.first()? {
        TransportChannelId::STATS => {
            // Stats frames contain a two-byte string length followed by the
            // externally tagged JSON enum. Each variant updates independent
            // browser state, so coalesce only within the same variant.
            let declared_length = usize::from(u16::from_be_bytes([*frame.get(1)?, *frame.get(2)?]));
            let json = frame.get(3..)?;
            if json.len() != declared_length {
                return None;
            }
            if json.starts_with(br#"{"Video":"#) {
                Some(ReliableReplacementKey::StatsVideo)
            } else if json.starts_with(br#"{"Rtt":"#) {
                Some(ReliableReplacementKey::StatsRtt)
            } else if json.starts_with(br#"{"BrowserRtt":"#) {
                Some(ReliableReplacementKey::StatsBrowserRtt)
            } else {
                None
            }
        }
        TransportChannelId::RTT if frame.len() == 4 && frame[1] == 0 => {
            Some(ReliableReplacementKey::Rtt)
        }
        // Controller rumble stays strict because the current normal and
        // trigger-rumble serializers use the same on-wire kind byte. Unknown
        // and future channels (including STREAM_CONTROL) are strict by default.
        _ => None,
    }
}

struct ReliableOutboundQueue {
    state: Mutex<ReliableOutboundState>,
    items_available: Notify,
    space_available: Notify,
    capacity: usize,
}

struct ReliableOutboundState {
    frames: VecDeque<ReliableOutboundFrame>,
    producer_closed: bool,
    consumer_closed: bool,
}

struct ReliableOutboundFrame {
    bytes: Bytes,
    replacement_key: Option<ReliableReplacementKey>,
}

struct ReliableOutboundSender {
    queue: Arc<ReliableOutboundQueue>,
}

struct ReliableOutboundReceiver {
    queue: Arc<ReliableOutboundQueue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliableOutboundSendError {
    Closed,
}

fn reliable_outbound_channel(
    capacity: usize,
) -> (ReliableOutboundSender, ReliableOutboundReceiver) {
    assert!(capacity > 0, "reliable outbound queue must have capacity");
    let queue = Arc::new(ReliableOutboundQueue {
        state: Mutex::new(ReliableOutboundState {
            frames: VecDeque::with_capacity(capacity),
            producer_closed: false,
            consumer_closed: false,
        }),
        items_available: Notify::new(),
        space_available: Notify::new(),
        capacity,
    });
    (
        ReliableOutboundSender {
            queue: queue.clone(),
        },
        ReliableOutboundReceiver { queue },
    )
}

impl ReliableOutboundSender {
    async fn send(&self, frame: Bytes) -> std::result::Result<(), ReliableOutboundSendError> {
        let replacement_key = reliable_replacement_key(&frame);
        loop {
            let space_available = self.queue.space_available.notified();
            {
                let mut state = self
                    .queue
                    .state
                    .lock()
                    .map_err(|_| ReliableOutboundSendError::Closed)?;
                if state.consumer_closed {
                    return Err(ReliableOutboundSendError::Closed);
                }

                if let Some(key) = replacement_key
                    && let Some(index) = state
                        .frames
                        .iter()
                        .position(|queued| queued.replacement_key == Some(key))
                {
                    // Move the fresh snapshot to the tail. This preserves the
                    // relative order of every non-replaceable frame while
                    // removing telemetry that was generated before them.
                    state.frames.remove(index);
                    state.frames.push_back(ReliableOutboundFrame {
                        bytes: frame,
                        replacement_key,
                    });
                    drop(state);
                    self.queue.items_available.notify_one();
                    return Ok(());
                }

                if state.frames.len() >= self.queue.capacity
                    && let Some(index) = state
                        .frames
                        .iter()
                        .position(|queued| queued.replacement_key.is_some())
                {
                    // A stale snapshot must never make a strict transition
                    // wait. When the new snapshot has a different key, prefer
                    // the newly observed value over the oldest telemetry.
                    state.frames.remove(index);
                }

                if state.frames.len() < self.queue.capacity {
                    state.frames.push_back(ReliableOutboundFrame {
                        bytes: frame,
                        replacement_key,
                    });
                    drop(state);
                    self.queue.items_available.notify_one();
                    return Ok(());
                }

                if replacement_key.is_some() {
                    // A snapshot must not hold up video/audio dispatch when
                    // every queued slot is an ordered transition. There is no
                    // stale snapshot to replace, so discard this observation.
                    return Ok(());
                }
            }
            // Critical ordered frames alone apply backpressure. They are never
            // discarded, and the existing bridge queue keeps that pressure
            // bounded instead of treating a short stall as a dead session.
            space_available.await;
        }
    }
}

impl Drop for ReliableOutboundSender {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.state.lock() {
            state.producer_closed = true;
        }
        // There is one receiver. notify_one stores a permit if it is between
        // checking the close flag and polling its notification future.
        self.queue.items_available.notify_one();
    }
}

impl ReliableOutboundReceiver {
    async fn recv(&mut self) -> Option<Bytes> {
        loop {
            let items_available = self.queue.items_available.notified();
            {
                let Ok(mut state) = self.queue.state.lock() else {
                    return None;
                };
                if let Some(frame) = state.frames.pop_front() {
                    drop(state);
                    self.queue.space_available.notify_one();
                    return Some(frame.bytes);
                }
                if state.producer_closed {
                    return None;
                }
            }
            items_available.await;
        }
    }
}

impl Drop for ReliableOutboundReceiver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.state.lock() {
            state.consumer_closed = true;
            state.frames.clear();
        }
        // There is one producer, so a stored single permit closes the race with
        // a sender that has checked capacity but not polled its waiter yet.
        self.queue.space_available.notify_one();
    }
}

struct VideoAdmissionQueue {
    state: Mutex<VideoAdmissionState>,
    changed: Notify,
}

struct VideoAdmissionState {
    frames: VecDeque<Bytes>,
    producer_closed: bool,
    consumer_closed: bool,
}

struct VideoAdmissionSender {
    queue: Arc<VideoAdmissionQueue>,
}

struct VideoAdmissionReceiver {
    queue: Arc<VideoAdmissionQueue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoAdmissionError {
    Full,
    Closed,
}

fn video_admission_channel() -> (VideoAdmissionSender, VideoAdmissionReceiver) {
    let queue = Arc::new(VideoAdmissionQueue {
        state: Mutex::new(VideoAdmissionState {
            frames: VecDeque::with_capacity(VIDEO_QUEUE_CAPACITY),
            producer_closed: false,
            consumer_closed: false,
        }),
        changed: Notify::new(),
    });
    (
        VideoAdmissionSender {
            queue: queue.clone(),
        },
        VideoAdmissionReceiver { queue },
    )
}

impl VideoAdmissionSender {
    /// Admits a frame without waiting for QUIC flow control or peer ACKs. A
    /// newly encoded IDR supersedes every unsent frame already in this short
    /// queue, so recovery can never be rejected behind stale deltas.
    fn try_send(&self, frame: Bytes) -> std::result::Result<usize, VideoAdmissionError> {
        let mut state = self
            .queue
            .state
            .lock()
            .map_err(|_| VideoAdmissionError::Closed)?;
        if state.consumer_closed {
            return Err(VideoAdmissionError::Closed);
        }

        let evicted = if frame.get(1) == Some(&1) {
            let evicted = state.frames.len();
            state.frames.clear();
            evicted
        } else {
            if state.frames.len() >= VIDEO_QUEUE_CAPACITY {
                return Err(VideoAdmissionError::Full);
            }
            0
        };
        state.frames.push_back(frame);
        drop(state);
        self.queue.changed.notify_one();
        Ok(evicted)
    }
}

impl Drop for VideoAdmissionSender {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.state.lock() {
            state.producer_closed = true;
        }
        self.queue.changed.notify_waiters();
    }
}

impl VideoAdmissionReceiver {
    async fn recv(&mut self) -> Option<Bytes> {
        loop {
            let changed = self.queue.changed.notified();
            {
                let Ok(mut state) = self.queue.state.lock() else {
                    return None;
                };
                if let Some(frame) = state.frames.pop_front() {
                    return Some(frame);
                }
                if state.producer_closed {
                    return None;
                }
            }
            changed.await;
        }
    }
}

impl Drop for VideoAdmissionReceiver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.state.lock() {
            state.consumer_closed = true;
            state.frames.clear();
        }
        self.queue.changed.notify_waiters();
    }
}

async fn run_connection(
    connection: Connection,
    mut bridge: PendingBridge,
) -> std::result::Result<(), ConnectionFailure> {
    // Keep cleanup outside the fallible setup/body so an audio/other lane-open
    // failure cannot leave the control session waiting forever in `Waiting`.
    let result = run_connection_inner(&connection, &mut bridge).await;

    connection.close(VarInt::from_u32(0), b"bridge closed");
    bridge
        .state_tx
        .send_replace(WebTransportConnectionState::Closed);
    bridge.shutdown_tx.send_replace(true);
    result
}

async fn run_connection_inner(
    connection: &Connection,
    bridge: &mut PendingBridge,
) -> std::result::Result<(), ConnectionFailure> {
    // Chrome does not expose the session to JavaScript until it has processed
    // the successful CONNECT response. Wait for the browser's lane marker as
    // that acknowledgement before opening any server-initiated streams. This
    // avoids racing media streams ahead of the CONNECT response on the wire.
    let mut setup_shutdown_rx = bridge.shutdown_rx.clone();
    let mut reliable_client_stream =
        match accept_reliable_inbound(connection, &mut setup_shutdown_rx).await {
            Ok(Some(stream)) => stream,
            Ok(None) => return Ok(()),
            Err(error) => {
                warn!(error = %error, "WebTransport browser acknowledgement failed");
                return Err(ConnectionFailure::new(
                    ConnectionStage::WaitReliableClientLane,
                    error,
                ));
            }
        };

    let mut audio_stream = open_lane(&connection, LANE_AUDIO, PRIORITY_AUDIO)
        .await
        .map_err(|error| ConnectionFailure::new(ConnectionStage::OpenAudioLane, error))?;
    let mut other_stream = open_lane(&connection, LANE_OTHER, PRIORITY_RELIABLE_OTHER)
        .await
        .map_err(|error| ConnectionFailure::new(ConnectionStage::OpenReliableServerLane, error))?;
    let (video_tx, video_rx) = video_admission_channel();
    let (audio_tx, audio_rx) = watch::channel(None::<Bytes>);
    let (other_tx, other_rx) = reliable_outbound_channel(OTHER_QUEUE_CAPACITY);
    let datagram_state = Arc::new(Mutex::new(DatagramState::default()));
    let congestion_counters = Arc::new(CongestionCounters::default());
    let recovery_request = Arc::new(RecoveryRequestState::default());

    bridge
        .state_tx
        .send_replace(WebTransportConnectionState::Connected);

    let result = tokio::select! {
        result = dispatch_outbound(
            &mut bridge.outbound_rx,
            video_tx,
            audio_tx.clone(),
            other_tx,
            bridge.inbound_tx.clone(),
            congestion_counters.clone(),
            recovery_request.clone(),
        ) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::DispatchOutbound, error)),
        result = write_video_streams(
            connection.clone(),
            video_rx,
            bridge.inbound_tx.clone(),
            congestion_counters.clone(),
            recovery_request,
        ) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::WriteVideo, error)),
        result = write_latest_lane(&mut audio_stream, audio_rx) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::WriteAudio, error)),
        result = write_queue_lane(&mut other_stream, other_rx) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::WriteReliableServer, error)),
        result = read_reliable_inbound(&mut reliable_client_stream, bridge.inbound_tx.clone(), bridge.shutdown_rx.clone(), datagram_state.clone()) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::ReadReliableClient, error)),
        result = read_datagrams(connection.clone(), bridge.inbound_tx.clone(), bridge.shutdown_rx.clone(), datagram_state) => result.map_err(|error| ConnectionFailure::new(ConnectionStage::ReadDatagrams, error)),
        _ = publish_network_feedback(
            connection.clone(),
            bridge.feedback_tx.clone(),
            congestion_counters,
            bridge.shutdown_rx.clone(),
        ) => Ok(()),
        _ = bridge.shutdown_rx.changed() => Ok(()),
        error = connection.closed() => {
            info!(error = %error, "WebTransport peer connection closed");
            Ok(())
        },
    };
    result
}

async fn open_lane(
    connection: &Connection,
    lane: u8,
    priority: i32,
) -> Result<wtransport::SendStream> {
    let mut stream = connection.open_uni().await?.await?;
    stream.set_priority(priority);
    stream.write_all(&[lane]).await?;
    Ok(stream)
}

async fn accept_reliable_inbound(
    connection: &Connection,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<Option<wtransport::RecvStream>> {
    if *shutdown_rx.borrow() {
        return Ok(None);
    }

    let receive_lane = async {
        let mut stream = connection.accept_uni().await?;
        let mut lane = [0_u8; 1];
        stream.read_exact(&mut lane).await?;
        if lane[0] != LANE_RELIABLE_INBOUND {
            bail!("unexpected WebTransport inbound lane");
        }
        Ok(Some(stream))
    };

    tokio::select! {
        result = receive_lane => result,
        _ = shutdown_rx.changed() => Ok(None),
        _ = tokio::time::sleep(CLIENT_LANE_SETUP_TIMEOUT) => Err(anyhow!("browser acknowledgement timed out")),
    }
}

async fn dispatch_outbound(
    outbound_rx: &mut mpsc::Receiver<Bytes>,
    video_tx: VideoAdmissionSender,
    audio_tx: watch::Sender<Option<Bytes>>,
    other_tx: ReliableOutboundSender,
    inbound_tx: mpsc::Sender<Bytes>,
    congestion_counters: Arc<CongestionCounters>,
    recovery_request: Arc<RecoveryRequestState>,
) -> Result<()> {
    let mut drop_video_until_idr = false;
    while let Some(frame) = outbound_rx.recv().await {
        if frame.is_empty() || frame.len() > MAX_FRAME_BYTES {
            continue;
        }

        match frame[0] {
            TransportChannelId::HOST_VIDEO => {
                let is_idr = frame.get(1) == Some(&1);
                if drop_video_until_idr && !is_idr {
                    congestion_counters
                        .admission_drops
                        .fetch_add(1, Ordering::Relaxed);
                    try_enqueue_video_recovery_request(
                        &inbound_tx,
                        &recovery_request,
                        &congestion_counters,
                    );
                    continue;
                }

                match video_tx.try_send(frame) {
                    Ok(evicted) => {
                        if is_idr {
                            if evicted > 0 {
                                info!(
                                    evicted_frames = evicted,
                                    "WebTransport IDR superseded unsent video frames"
                                );
                            }
                            drop_video_until_idr = false;
                            recovery_request.clear();
                        }
                    }
                    Err(VideoAdmissionError::Full) => {
                        congestion_counters
                            .admission_drops
                            .fetch_add(1, Ordering::Relaxed);
                        let newly_congested = !drop_video_until_idr;
                        // A dropped encoded frame invalidates all later deltas
                        // in this GOP. Keep the short already-admitted prefix,
                        // discard new deltas, and resume only at a fresh IDR.
                        // Four pending frames tolerate scheduler bursts without
                        // building a visible video backlog.
                        if is_idr {
                            // The previous request was serviced, but this IDR
                            // could not be admitted. Request one replacement.
                            recovery_request.clear();
                        }
                        if newly_congested {
                            warn!(
                                queue_capacity = VIDEO_QUEUE_CAPACITY,
                                "WebTransport video admission queue filled; requesting IDR"
                            );
                        }
                        drop_video_until_idr = true;
                        try_enqueue_video_recovery_request(
                            &inbound_tx,
                            &recovery_request,
                            &congestion_counters,
                        );
                    }
                    Err(VideoAdmissionError::Closed) => {
                        bail!("WebTransport video admission queue is closed");
                    }
                }
            }
            TransportChannelId::HOST_AUDIO => {
                // Audio must never wait behind video. If the browser stops
                // consuming, recent audio is more useful than delayed audio.
                audio_tx.send_replace(Some(frame));
            }
            _ => {
                // Lane 3 is reliable and ordered. Replace stale snapshots when
                // possible; otherwise wait for bounded capacity so a short
                // client stall cannot kill the transport or lose a transition.
                other_tx
                    .send(frame)
                    .await
                    .map_err(|_| anyhow!("WebTransport reliable outbound queue is closed"))?;
            }
        }
    }
    Ok(())
}

fn try_enqueue_video_recovery_request(
    inbound_tx: &mpsc::Sender<Bytes>,
    recovery_request: &RecoveryRequestState,
    congestion_counters: &CongestionCounters,
) {
    if recovery_request
        .enqueued
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        if inbound_tx
            .try_send(Bytes::from_static(&[TransportChannelId::HOST_VIDEO, 0]))
            .is_ok()
        {
            congestion_counters
                .recovery_requests
                .fetch_add(1, Ordering::Relaxed);
        } else {
            recovery_request.enqueued.store(false, Ordering::Release);
        }
    }
}

async fn publish_network_feedback(
    connection: Connection,
    feedback_tx: watch::Sender<NetworkFeedback>,
    counters: Arc<CongestionCounters>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(NETWORK_FEEDBACK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval` ticks immediately once; feedback is intentionally paced so a
    // just-created session cannot produce a meaningless all-zero sample.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let stats = connection.quic_connection().stats();
                feedback_tx.send_replace(counters.snapshot(
                    stats.path.rtt,
                    stats.path.sent_packets,
                    stats.path.lost_packets,
                    stats.path.congestion_events,
                ));
            }
            _ = shutdown_rx.changed() => return,
            _ = connection.closed() => return,
        }
    }
}

async fn write_latest_lane(
    stream: &mut wtransport::SendStream,
    mut changed: watch::Receiver<Option<Bytes>>,
) -> Result<()> {
    loop {
        changed.changed().await?;
        // `send_replace(None)` from this consumer used to increment the same
        // watch version it was waiting on. Once audio started, None -> None
        // therefore woke the task forever and hot-looped a Tokio worker. A
        // receiver-local version cursor coalesces producer updates without
        // publishing anything back into the channel.
        let Some(frame) = changed.borrow_and_update().clone() else {
            continue;
        };
        write_frame(stream, &frame).await?;
    }
}

/// Sends each encoded video frame on its own QUIC stream. A loss on one stream
/// can therefore delay only that frame rather than every later frame in the
/// GOP. The browser performs the final sequence/recovery decision.
async fn write_video_streams(
    connection: Connection,
    mut video_rx: VideoAdmissionReceiver,
    inbound_tx: mpsc::Sender<Bytes>,
    congestion_counters: Arc<CongestionCounters>,
    recovery_request: Arc<RecoveryRequestState>,
) -> Result<()> {
    let mut next_sequence = 0_u32;
    let mut budget = VideoWriterBudget::default();
    let mut tasks = tokio::task::JoinSet::new();
    // Only an IDR may wait here. A delta that cannot enter the reserved writer
    // budget is discarded immediately so it can never hide a later IDR.
    let mut pending_idr: Option<Bytes> = None;
    let mut queue_closed = false;
    let mut gop_generation = 0_u64;
    let (generation_tx, _generation_rx) = watch::channel(gop_generation);
    let (guard_stopped_tx, mut guard_stopped_rx) = mpsc::unbounded_channel::<u64>();
    let mut recovering = false;

    loop {
        if let Some(frame) = pending_idr.take() {
            if budget.can_admit(frame.len(), true) {
                let frame_bytes = frame.len();
                budget.admit(frame_bytes, true);
                spawn_video_write_task(
                    &mut tasks,
                    &connection,
                    &generation_tx,
                    next_sequence,
                    gop_generation,
                    frame,
                    true,
                    guard_stopped_tx.clone(),
                );
                next_sequence = next_sequence.wrapping_add(1);
            } else {
                pending_idr = Some(frame);
            }
        }

        if queue_closed && pending_idr.is_none() && tasks.is_empty() {
            return Ok(());
        }

        tokio::select! {
            frame = video_rx.recv(), if !queue_closed && pending_idr.is_none() => {
                match frame {
                    Some(frame) => {
                        let is_idr = frame.get(1) == Some(&1);
                        if is_idr {
                            advance_video_generation(&mut gop_generation, &generation_tx);
                            recovering = false;
                            recovery_request.clear();
                        } else if recovering {
                            congestion_counters.admission_drops.fetch_add(1, Ordering::Relaxed);
                            try_enqueue_video_recovery_request(
                                &inbound_tx,
                                &recovery_request,
                                &congestion_counters,
                            );
                            continue;
                        }

                        let frame_bytes = frame.len();
                        if budget.can_admit(frame_bytes, is_idr) {
                            budget.admit(frame_bytes, is_idr);
                            spawn_video_write_task(
                                &mut tasks,
                                &connection,
                                &generation_tx,
                                next_sequence,
                                gop_generation,
                                frame,
                                is_idr,
                                guard_stopped_tx.clone(),
                            );
                            next_sequence = next_sequence.wrapping_add(1);
                        } else if is_idr {
                            // Updating the generation above wakes every older
                            // stream. Keep only this IDR while their explicit
                            // resets release the reserved writer window.
                            pending_idr = Some(frame);
                        } else {
                            congestion_counters.admission_drops.fetch_add(1, Ordering::Relaxed);
                            // The missing delta makes every sibling in this
                            // GOP obsolete. Reset them now instead of letting
                            // their retransmissions compete with recovery.
                            advance_video_generation(&mut gop_generation, &generation_tx);
                            recovering = true;
                            try_enqueue_video_recovery_request(
                                &inbound_tx,
                                &recovery_request,
                                &congestion_counters,
                            );
                        }
                    }
                    None => queue_closed = true,
                }
            }
            completed = tasks.join_next(), if !tasks.is_empty() => {
                match completed {
                    Some(Ok((frame_bytes, is_idr, task_generation, result))) => {
                        budget.release(frame_bytes, is_idr);
                        let outcome = result?;
                        if outcome == VideoStreamWriteOutcome::TimedOut {
                            congestion_counters.video_write_timeouts.fetch_add(1, Ordering::Relaxed);
                        }
                        if video_write_outcome_requires_recovery(
                            outcome,
                            task_generation,
                            gop_generation,
                        ) {
                            // One missing delta invalidates the rest of this
                            // GOP. Retire every sibling stream immediately so
                            // obsolete retransmissions cannot compete with the
                            // requested recovery IDR.
                            advance_video_generation(&mut gop_generation, &generation_tx);
                            if !recovering {
                                match outcome {
                                    VideoStreamWriteOutcome::TimedOut => warn!(
                                        sequence_generation = task_generation,
                                        "WebTransport video stream timed out; requesting IDR"
                                    ),
                                    VideoStreamWriteOutcome::Stopped => warn!(
                                        sequence_generation = task_generation,
                                        "WebTransport peer stopped a current video stream; requesting IDR"
                                    ),
                                    _ => {}
                                }
                            }
                            recovering = true;
                            try_enqueue_video_recovery_request(
                                &inbound_tx,
                                &recovery_request,
                                &congestion_counters,
                            );
                        }
                    }
                    Some(Err(error)) if error.is_cancelled() => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => {}
                }
            }
            stopped_generation = guard_stopped_rx.recv() => {
                if let Some(stopped_generation) = stopped_generation
                    && video_write_outcome_requires_recovery(
                        VideoStreamWriteOutcome::Stopped,
                        stopped_generation,
                        gop_generation,
                    )
                {
                    advance_video_generation(&mut gop_generation, &generation_tx);
                    if !recovering {
                        warn!(
                            sequence_generation = stopped_generation,
                            "WebTransport peer stopped a current video stream; requesting IDR"
                        );
                    }
                    recovering = true;
                    try_enqueue_video_recovery_request(
                        &inbound_tx,
                        &recovery_request,
                        &congestion_counters,
                    );
                }
            }
        }
    }
}

fn advance_video_generation(generation: &mut u64, generation_tx: &watch::Sender<u64>) {
    *generation = generation.wrapping_add(1);
    generation_tx.send_replace(*generation);
}

fn video_write_outcome_requires_recovery(
    outcome: VideoStreamWriteOutcome,
    task_generation: u64,
    current_generation: u64,
) -> bool {
    task_generation == current_generation
        && matches!(
            outcome,
            VideoStreamWriteOutcome::Stopped | VideoStreamWriteOutcome::TimedOut
        )
}

type VideoWriteTaskResult = (usize, bool, u64, Result<VideoStreamWriteOutcome>);

fn spawn_video_write_task(
    tasks: &mut tokio::task::JoinSet<VideoWriteTaskResult>,
    connection: &Connection,
    generation_tx: &watch::Sender<u64>,
    sequence: u32,
    generation: u64,
    frame: Bytes,
    is_idr: bool,
    guard_stopped_tx: mpsc::UnboundedSender<u64>,
) {
    let task_connection = connection.clone();
    let generation_rx = generation_tx.subscribe();
    tasks.spawn(async move {
        let frame_bytes = frame.len();
        let result = write_video_stream(
            &task_connection,
            sequence,
            &frame,
            generation,
            generation_rx,
            guard_stopped_tx,
        )
        .await;
        (frame_bytes, is_idr, generation, result)
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoStreamWriteOutcome {
    Complete,
    Stopped,
    Superseded,
    TimedOut,
}

async fn write_video_stream(
    connection: &Connection,
    sequence: u32,
    frame: &[u8],
    generation: u64,
    mut generation_rx: watch::Receiver<u64>,
    guard_stopped_tx: mpsc::UnboundedSender<u64>,
) -> Result<VideoStreamWriteOutcome> {
    let is_idr = frame.get(1) == Some(&1);
    // Delta frames always use the fixed latency budget. Avoid taking Quinn's
    // connection-state lock at frame rate just to compute that constant; only
    // the less frequent IDR path needs the live RTT-scaled allowance.
    let inactivity_timeout = if is_idr {
        video_stream_inactivity_timeout(true, connection.rtt())
    } else {
        VIDEO_STREAM_WRITE_INACTIVITY_TIMEOUT
    };
    let mut stream = tokio::select! {
        biased;
        _ = wait_for_video_supersession(&mut generation_rx, generation) => {
            return Ok(VideoStreamWriteOutcome::Superseded);
        }
        result = tokio::time::timeout(inactivity_timeout, async {
            let opening = connection.open_uni().await?;
            Ok::<_, anyhow::Error>(opening.await?)
        }) => match result {
            Ok(result) => result?,
            Err(_) => return Ok(VideoStreamWriteOutcome::TimedOut),
        },
    };
    stream.set_priority(if is_idr {
        PRIORITY_VIDEO_IDR
    } else {
        PRIORITY_VIDEO_DELTA
    });
    let header = encode_video_stream_header(sequence, frame.len())?;
    let outcome = write_video_stream_part(
        &mut stream,
        &header,
        generation,
        &mut generation_rx,
        inactivity_timeout,
    )
    .await?;
    if outcome != VideoStreamWriteOutcome::Complete {
        return Ok(outcome);
    }
    let outcome = write_video_stream_part(
        &mut stream,
        frame,
        generation,
        &mut generation_rx,
        inactivity_timeout,
    )
    .await?;
    if outcome != VideoStreamWriteOutcome::Complete {
        return Ok(outcome);
    }
    if *generation_rx.borrow() != generation {
        let _ = stream.reset(video_stream_superseded_code());
        return Ok(VideoStreamWriteOutcome::Superseded);
    }

    // Signal FIN through Quinn without awaiting acknowledgement. A detached,
    // allocation-free guard retains only the stream handle so a future IDR can
    // explicitly RESET_STREAM and stop obsolete retransmissions.
    if stream.quic_stream_mut().finish().is_err() {
        return Ok(VideoStreamWriteOutcome::Stopped);
    }
    tokio::spawn(retire_video_stream_on_ack_or_supersession(
        stream,
        generation,
        generation_rx,
        guard_stopped_tx,
    ));
    Ok(VideoStreamWriteOutcome::Complete)
}

async fn write_video_stream_part(
    stream: &mut wtransport::SendStream,
    bytes: &[u8],
    generation: u64,
    generation_rx: &mut watch::Receiver<u64>,
    inactivity_timeout: Duration,
) -> Result<VideoStreamWriteOutcome> {
    let mut written = 0;
    while written < bytes.len() {
        let write_result = tokio::select! {
            biased;
            _ = wait_for_video_supersession(generation_rx, generation) => {
                let _ = stream.reset(video_stream_superseded_code());
                return Ok(VideoStreamWriteOutcome::Superseded);
            }
            result = tokio::time::timeout(
                inactivity_timeout,
                stream.write(&bytes[written..]),
            ) => result,
        };
        match write_result {
            Ok(Ok(0)) => {
                let _ = stream.reset(video_stream_timeout_code());
                return Ok(VideoStreamWriteOutcome::TimedOut);
            }
            Ok(Ok(progress)) => written += progress,
            // A peer may stop an obsolete per-frame stream after it has
            // already advanced to an IDR. That recovers only video and is not
            // a connection failure; persistent audio/other lane failures
            // remain fatal.
            Ok(Err(wtransport::error::StreamWriteError::Stopped(_))) => {
                return Ok(VideoStreamWriteOutcome::Stopped);
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                // This deadline resets after every successful partial write.
                // Large IDRs can therefore take longer than one inactivity
                // interval in total on a constrained flow-control window while
                // a truly stuck stream is still reclaimed promptly.
                let _ = stream.reset(video_stream_timeout_code());
                return Ok(VideoStreamWriteOutcome::TimedOut);
            }
        }
    }
    Ok(VideoStreamWriteOutcome::Complete)
}

fn video_stream_inactivity_timeout(is_idr: bool, rtt: Duration) -> Duration {
    if !is_idr {
        return VIDEO_STREAM_WRITE_INACTIVITY_TIMEOUT;
    }

    rtt.saturating_mul(VIDEO_IDR_RTT_TIMEOUT_MULTIPLIER)
        .clamp(VIDEO_STREAM_OPEN_TIMEOUT, VIDEO_IDR_MAX_INACTIVITY_TIMEOUT)
}

async fn wait_for_video_supersession(generation_rx: &mut watch::Receiver<u64>, generation: u64) {
    loop {
        if *generation_rx.borrow_and_update() != generation {
            return;
        }
        if generation_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn retire_video_stream_on_ack_or_supersession(
    mut stream: wtransport::SendStream,
    generation: u64,
    mut generation_rx: watch::Receiver<u64>,
    guard_stopped_tx: mpsc::UnboundedSender<u64>,
) {
    let outcome = if *generation_rx.borrow() != generation {
        None
    } else {
        tokio::select! {
            biased;
            _ = wait_for_video_supersession(&mut generation_rx, generation) => None,
            outcome = stream.stopped() => Some(outcome),
        }
    };
    match outcome {
        None => {
            let _ = stream.reset(video_stream_superseded_code());
        }
        Some(wtransport::error::StreamWriteError::Stopped(_)) => {
            let _ = guard_stopped_tx.send(generation);
        }
        Some(_) => {}
    }
}

async fn write_queue_lane(
    stream: &mut wtransport::SendStream,
    mut queue: ReliableOutboundReceiver,
) -> Result<()> {
    while let Some(frame) = queue.recv().await {
        write_frame(stream, &frame).await?;
    }
    Ok(())
}

async fn write_frame(stream: &mut wtransport::SendStream, frame: &[u8]) -> Result<()> {
    let header = encode_frame_length(frame.len())?;
    stream.write_all(&header).await?;
    stream.write_all(frame).await?;
    Ok(())
}

async fn read_reliable_inbound(
    stream: &mut wtransport::RecvStream,
    inbound_tx: mpsc::Sender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
    datagram_state: Arc<Mutex<DatagramState>>,
) -> Result<()> {
    loop {
        let frame = tokio::select! {
            result = read_frame(stream) => result?,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        if is_snapshot_envelope(&frame) {
            // Reserve capacity before advancing the shared sequence state.
            // If this reliable snapshot was delayed behind a newer datagram,
            // decoding it after the wait rejects it as stale. Conversely, the
            // permit makes decode + delivery atomic with respect to datagram
            // admission without holding a mutex across an await.
            let permit = tokio::select! {
                result = inbound_tx.reserve() => result?,
                _ = shutdown_rx.changed() => return Ok(()),
            };
            let decoded = {
                let mut state = datagram_state
                    .lock()
                    .map_err(|_| anyhow!("WebTransport datagram state lock is poisoned"))?;
                state.decode(&frame)
            };
            if let Some(mut frames) = decoded
                && frames.len() == 1
            {
                permit.send(frames.remove(0));
            }
        } else {
            // Ordinary reliable frames retain their existing byte-for-byte,
            // ordered channel semantics. Only the reserved snapshot envelope
            // is interpreted by the transport.
            inbound_tx.send(frame).await?;
        }
    }
}

async fn read_frame(stream: &mut wtransport::RecvStream) -> Result<Bytes> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = decode_reliable_inbound_frame_length(length)?;
    let mut frame = vec![0_u8; length];
    tokio::time::timeout(CLIENT_RELIABLE_BODY_TIMEOUT, stream.read_exact(&mut frame))
        .await
        .context("WebTransport reliable inbound frame body timed out")??;
    Ok(Bytes::from(frame))
}

fn encode_frame_length(length: usize) -> Result<[u8; 4]> {
    if length == 0 || length > MAX_FRAME_BYTES {
        bail!("invalid WebTransport frame length");
    }
    Ok((length as u32).to_be_bytes())
}

fn encode_video_stream_header(sequence: u32, length: usize) -> Result<[u8; 9]> {
    let length = encode_frame_length(length)?;
    let mut header = [0_u8; 9];
    header[0] = LANE_VIDEO;
    header[1..5].copy_from_slice(&sequence.to_be_bytes());
    header[5..9].copy_from_slice(&length);
    Ok(header)
}

#[cfg(test)]
fn decode_frame_length(encoded: [u8; 4]) -> Result<usize> {
    let length = u32::from_be_bytes(encoded) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        bail!("invalid WebTransport frame length");
    }
    Ok(length)
}

fn decode_reliable_inbound_frame_length(encoded: [u8; 4]) -> Result<usize> {
    let length = u32::from_be_bytes(encoded) as usize;
    if length == 0 || length > MAX_RELIABLE_INBOUND_FRAME_BYTES {
        bail!("invalid WebTransport reliable inbound frame length");
    }
    Ok(length)
}

async fn read_datagrams(
    connection: Connection,
    inbound_tx: mpsc::Sender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
    datagram_state: Arc<Mutex<DatagramState>>,
) -> Result<()> {
    loop {
        let datagram = tokio::select! {
            result = connection.receive_datagram() => result?,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        let bytes: &[u8] = &datagram;
        if bytes.len() > MAX_DATAGRAM_BYTES {
            continue;
        }

        {
            let mut state = datagram_state
                .lock()
                .map_err(|_| anyhow!("WebTransport datagram state lock is poisoned"))?;
            // Retain the previous cumulative baseline if the bounded input
            // queue cannot accept the whole recovered delta. A later
            // cumulative datagram can then recover it instead of silently
            // losing motion.
            let old_state = state.clone();
            let Some(frames) = state.decode(bytes) else {
                continue;
            };
            if frames.is_empty() {
                continue;
            }
            let Ok(permits) = inbound_tx.try_reserve_many(frames.len()) else {
                *state = old_state;
                continue;
            };
            for (permit, frame) in permits.zip(frames) {
                permit.send(frame);
            }
        }
    }
}

fn is_snapshot_envelope(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == DATAGRAM_MAGIC && bytes[1] == DATAGRAM_SNAPSHOT
}

#[derive(Debug, Clone, Copy)]
struct CumulativePoint {
    epoch: u16,
    sequence: u32,
    x: i32,
    y: i32,
}

#[derive(Debug, Clone, Copy)]
struct SequencePoint {
    epoch: u16,
    sequence: u32,
}

#[derive(Debug, Clone, Default)]
struct DatagramState {
    relative: Option<CumulativePoint>,
    scroll: Option<CumulativePoint>,
    snapshots: HashMap<u8, SequencePoint>,
}

impl DatagramState {
    fn decode(&mut self, bytes: &[u8]) -> Option<Vec<Bytes>> {
        if bytes.len() < 2 || bytes[0] != DATAGRAM_MAGIC {
            return None;
        }
        match bytes[1] {
            DATAGRAM_RELATIVE_MOTION | DATAGRAM_HIGH_RES_SCROLL if bytes.len() == 16 => {
                let epoch = u16::from_be_bytes(bytes[2..4].try_into().ok()?);
                let sequence = u32::from_be_bytes(bytes[4..8].try_into().ok()?);
                let x = i32::from_be_bytes(bytes[8..12].try_into().ok()?);
                let y = i32::from_be_bytes(bytes[12..16].try_into().ok()?);
                let point = CumulativePoint {
                    epoch,
                    sequence,
                    x,
                    y,
                };
                let slot = if bytes[1] == DATAGRAM_RELATIVE_MOTION {
                    &mut self.relative
                } else {
                    &mut self.scroll
                };
                let (delta_x, delta_y) = advance_cumulative(slot, point)?;
                let packet_type = if bytes[1] == DATAGRAM_RELATIVE_MOTION {
                    0
                } else {
                    3
                };
                Some(split_mouse_delta(packet_type, delta_x, delta_y))
            }
            DATAGRAM_SNAPSHOT if bytes.len() >= 10 => {
                let channel = bytes[2];
                let is_snapshot_channel = channel == TransportChannelId::MOUSE_ABSOLUTE
                    || (TransportChannelId::CONTROLLER0..=TransportChannelId::CONTROLLER15)
                        .contains(&channel);
                if !is_snapshot_channel {
                    return None;
                }
                let epoch = u16::from_be_bytes(bytes[3..5].try_into().ok()?);
                let sequence = u32::from_be_bytes(bytes[5..9].try_into().ok()?);
                let point = SequencePoint { epoch, sequence };
                match self.snapshots.entry(channel) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(point);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if !advance_sequence(entry.get_mut(), point) {
                            return None;
                        }
                    }
                }
                let mut frame = Vec::with_capacity(bytes.len() - 8);
                frame.push(channel);
                frame.extend_from_slice(&bytes[9..]);
                Some(vec![Bytes::from(frame)])
            }
            _ => None,
        }
    }
}

fn advance_cumulative(
    previous: &mut Option<CumulativePoint>,
    current: CumulativePoint,
) -> Option<(i32, i32)> {
    let delta = match *previous {
        None => (current.x, current.y),
        Some(old) if current.epoch == old.epoch => {
            if !is_newer_u32(current.sequence, old.sequence) {
                return None;
            }
            (current.x.wrapping_sub(old.x), current.y.wrapping_sub(old.y))
        }
        Some(old) => {
            if !is_newer_u16(current.epoch, old.epoch) {
                return None;
            }
            (current.x, current.y)
        }
    };
    *previous = Some(current);
    Some(delta)
}

fn advance_sequence(previous: &mut SequencePoint, current: SequencePoint) -> bool {
    if previous.epoch == current.epoch {
        if !is_newer_u32(current.sequence, previous.sequence) {
            return false;
        }
    } else if !is_newer_u16(current.epoch, previous.epoch) {
        return false;
    }
    *previous = current;
    true
}

fn is_newer_u32(current: u32, previous: u32) -> bool {
    let distance = current.wrapping_sub(previous);
    distance != 0 && distance < (1_u32 << 31)
}

fn is_newer_u16(current: u16, previous: u16) -> bool {
    let distance = current.wrapping_sub(previous);
    distance != 0 && distance < (1_u16 << 15)
}

fn split_mouse_delta(packet_type: u8, delta_x: i32, delta_y: i32) -> Vec<Bytes> {
    let max_x = i16::MAX as i32 * MAX_SPLIT_PACKETS as i32;
    let min_x = i16::MIN as i32 * MAX_SPLIT_PACKETS as i32;
    let mut remaining_x = delta_x.clamp(min_x, max_x);
    let mut remaining_y = delta_y.clamp(min_x, max_x);
    let mut frames = Vec::with_capacity(
        chunks_for(remaining_x)
            .max(chunks_for(remaining_y))
            .min(MAX_SPLIT_PACKETS),
    );
    while (remaining_x != 0 || remaining_y != 0) && frames.len() < MAX_SPLIT_PACKETS {
        let x = remaining_x.clamp(i16::MIN as i32, i16::MAX as i32) as i16;
        let y = remaining_y.clamp(i16::MIN as i32, i16::MAX as i32) as i16;
        let mut frame = Vec::with_capacity(6);
        frame.push(TransportChannelId::MOUSE_RELATIVE);
        frame.push(packet_type);
        frame.extend_from_slice(&x.to_be_bytes());
        frame.extend_from_slice(&y.to_be_bytes());
        frames.push(Bytes::from(frame));
        remaining_x -= i32::from(x);
        remaining_y -= i32::from(y);
    }
    frames
}

fn chunks_for(value: i32) -> usize {
    if value == 0 {
        0
    } else if value > 0 {
        ((i64::from(value) + i64::from(i16::MAX) - 1) / i64::from(i16::MAX)) as usize
    } else {
        ((-i64::from(value) + -i64::from(i16::MIN) - 1) / -i64::from(i16::MIN)) as usize
    }
}

fn hash_token(token: &[u8; 32]) -> [u8; 32] {
    Sha256::digest(token).into()
}

fn encode_token(token: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in token {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_token(encoded: &str) -> Option<[u8; 32]> {
    if encoded.len() != 64 || !encoded.is_ascii() {
        return None;
    }
    let mut token = [0_u8; 32];
    for (index, output) in token.iter_mut().enumerate() {
        let high = decode_nibble(encoded.as_bytes()[index * 2])?;
        let low = decode_nibble(encoded.as_bytes()[index * 2 + 1])?;
        *output = (high << 4) | low;
    }
    Some(hash_token(&token))
}

fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_reset_codes_use_webtransport_application_error_mapping() {
        assert_eq!(
            video_stream_timeout_code().into_inner(),
            WEBTRANSPORT_APPLICATION_ERROR_FIRST + u64::from(VIDEO_STREAM_TIMEOUT_APPLICATION_CODE)
        );
        assert_eq!(
            video_stream_superseded_code().into_inner(),
            WEBTRANSPORT_APPLICATION_ERROR_FIRST
                + u64::from(VIDEO_STREAM_SUPERSEDED_APPLICATION_CODE)
        );

        // Crossing application code 0x1e must skip the HTTP/3 grease value.
        assert_eq!(
            webtransport_application_error_code(0x1e).into_inner(),
            WEBTRANSPORT_APPLICATION_ERROR_FIRST + 0x1f
        );
    }

    #[test]
    fn draft02_response_requires_exact_client_advertisement() {
        let mut headers = HashMap::new();
        assert!(!requests_draft02_response(&headers));

        headers.insert(DRAFT02_REQUEST_HEADER.to_owned(), "0".to_owned());
        assert!(!requests_draft02_response(&headers));

        headers.insert(DRAFT02_REQUEST_HEADER.to_owned(), "1".to_owned());
        assert!(requests_draft02_response(&headers));
    }

    fn pending(expires_at: Instant) -> PendingBridge {
        let (_outbound_tx, outbound_rx) = mpsc::channel(1);
        let (inbound_tx, _inbound_rx) = mpsc::channel(1);
        let (state_tx, _state_rx) = watch::channel(WebTransportConnectionState::Waiting);
        let (feedback_tx, _feedback_rx) = watch::channel(NetworkFeedback::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        PendingBridge {
            expires_at,
            expected_origin: "https://example.test".to_owned(),
            outbound_rx,
            inbound_tx,
            state_tx,
            feedback_tx,
            shutdown_tx,
            shutdown_rx,
        }
    }

    #[test]
    fn token_is_one_use_and_expires() {
        let now = Instant::now();
        let first = hash_token(&[1; 32]);
        let expired = hash_token(&[2; 32]);
        let shut_down = hash_token(&[3; 32]);
        let mut store = TokenStore::default();
        store
            .insert(first, pending(now + Duration::from_secs(1)), now)
            .unwrap();
        store.insert(expired, pending(now), now).unwrap();
        let shut_down_entry = pending(now + Duration::from_secs(1));
        let shutdown_tx = shut_down_entry.shutdown_tx.clone();
        store.insert(shut_down, shut_down_entry, now).unwrap();
        shutdown_tx.send_replace(true);

        assert!(store.consume(&first, "https://example.test", now).is_ok());
        assert!(matches!(
            store.consume(&first, "https://example.test", now),
            Err(TokenConsumeRejection::Unknown)
        ));
        assert!(matches!(
            store.consume(&expired, "https://example.test", now),
            Err(TokenConsumeRejection::Unknown)
        ));
        assert!(matches!(
            store.consume(&shut_down, "https://example.test", now),
            Err(TokenConsumeRejection::Unknown)
        ));
    }

    #[test]
    fn wrong_origin_does_not_consume_token() {
        let now = Instant::now();
        let token = hash_token(&[4; 32]);
        let mut store = TokenStore::default();
        store
            .insert(token, pending(now + Duration::from_secs(1)), now)
            .unwrap();

        assert!(matches!(
            store.consume(&token, "https://evil.test", now),
            Err(TokenConsumeRejection::OriginMismatch)
        ));
        assert!(store.consume(&token, "https://example.test", now).is_ok());
    }

    #[test]
    fn frame_length_codec_is_big_endian_and_bounded() {
        assert_eq!(encode_frame_length(0x01_02_03).unwrap(), [0, 1, 2, 3]);
        assert_eq!(decode_frame_length([0, 1, 2, 3]).unwrap(), 0x01_02_03);
        assert!(encode_frame_length(0).is_err());
        assert!(encode_frame_length(MAX_FRAME_BYTES + 1).is_err());
        assert!(decode_frame_length([0, 0, 0, 0]).is_err());
        assert!(decode_frame_length(u32::MAX.to_be_bytes()).is_err());
        assert_eq!(
            decode_reliable_inbound_frame_length(
                (MAX_RELIABLE_INBOUND_FRAME_BYTES as u32).to_be_bytes()
            )
            .unwrap(),
            MAX_RELIABLE_INBOUND_FRAME_BYTES
        );
        assert!(
            decode_reliable_inbound_frame_length(
                (MAX_RELIABLE_INBOUND_FRAME_BYTES as u32 + 1).to_be_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn video_stream_header_matches_browser_wire_format() {
        assert_eq!(
            encode_video_stream_header(0x0102_0304, 0x0006_0708).unwrap(),
            [LANE_VIDEO, 1, 2, 3, 4, 0, 6, 7, 8]
        );
        assert_eq!(
            encode_video_stream_header(u32::MAX, MAX_FRAME_BYTES).unwrap(),
            [LANE_VIDEO, 0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0,]
        );
        assert!(encode_video_stream_header(0, 0).is_err());
        assert!(encode_video_stream_header(0, MAX_FRAME_BYTES + 1).is_err());
    }

    #[test]
    fn video_writer_reserves_stream_and_byte_budget_for_an_idr() {
        let mut budget = VideoWriterBudget::default();
        for _ in 0..MAX_VIDEO_DELTA_IN_FLIGHT_STREAMS {
            budget.admit(1, false);
        }

        assert!(!budget.can_admit(1, false));
        assert!(budget.can_admit(MAX_FRAME_BYTES, true));

        let mut byte_limited = VideoWriterBudget::default();
        byte_limited.admit(MAX_VIDEO_DELTA_IN_FLIGHT_BYTES, false);
        assert!(!byte_limited.can_admit(1, false));
        assert!(byte_limited.can_admit(MAX_FRAME_BYTES, true));
    }

    #[test]
    fn idr_inactivity_timeout_scales_with_rtt_and_is_bounded() {
        assert_eq!(
            video_stream_inactivity_timeout(true, Duration::from_millis(10)),
            Duration::from_millis(500)
        );
        assert_eq!(
            video_stream_inactivity_timeout(true, Duration::from_millis(250)),
            Duration::from_secs(1)
        );
        assert_eq!(
            video_stream_inactivity_timeout(true, Duration::from_millis(600)),
            Duration::from_secs(2)
        );
        assert_eq!(
            video_stream_inactivity_timeout(true, Duration::MAX),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn delta_inactivity_timeout_stays_latency_bounded() {
        assert_eq!(
            video_stream_inactivity_timeout(false, Duration::from_secs(10)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn feedback_snapshot_saturates_rtt_and_reads_cumulative_counters() {
        let counters = CongestionCounters::default();
        counters.admission_drops.store(5, Ordering::Relaxed);
        counters.video_write_timeouts.store(2, Ordering::Relaxed);
        counters.recovery_requests.store(3, Ordering::Relaxed);

        assert_eq!(
            counters.snapshot(Duration::from_millis(47), 1_000, 7, 4),
            NetworkFeedback {
                rtt_ms: 47,
                sent_packets: 1_000,
                lost_packets: 7,
                congestion_events: 4,
                admission_drops: 5,
                video_write_timeouts: 2,
                recovery_requests: 3,
            }
        );
        assert_eq!(
            counters
                .snapshot(Duration::from_millis(u64::from(u32::MAX) + 1), 0, 0, 0)
                .rtt_ms,
            u32::MAX
        );
    }

    #[tokio::test]
    async fn feedback_watch_coalesces_to_the_latest_snapshot() {
        let (sender, mut receiver) = watch::channel(NetworkFeedback::default());
        let first = NetworkFeedback {
            rtt_ms: 10,
            sent_packets: 1,
            ..NetworkFeedback::default()
        };
        let latest = NetworkFeedback {
            rtt_ms: 30,
            sent_packets: 9,
            lost_packets: 2,
            ..NetworkFeedback::default()
        };

        sender.send_replace(first);
        sender.send_replace(latest);
        receiver
            .changed()
            .await
            .expect("feedback sender should remain open");

        assert_eq!(*receiver.borrow_and_update(), latest);
    }

    #[tokio::test]
    async fn recovery_generation_advance_wakes_obsolete_sibling_writers() {
        let (generation_tx, mut generation_rx) = watch::channel(7_u64);
        let mut generation = 7_u64;
        // This is the same transition used after a current-generation timeout
        // or admission drop, before an IDR has arrived.
        advance_video_generation(&mut generation, &generation_tx);
        wait_for_video_supersession(&mut generation_rx, 7).await;
        assert_eq!(generation, 8);
        assert_eq!(*generation_rx.borrow(), 8);
    }

    #[test]
    fn only_current_timeout_or_peer_stop_requires_video_recovery() {
        assert!(video_write_outcome_requires_recovery(
            VideoStreamWriteOutcome::TimedOut,
            9,
            9,
        ));
        assert!(video_write_outcome_requires_recovery(
            VideoStreamWriteOutcome::Stopped,
            9,
            9,
        ));
        assert!(!video_write_outcome_requires_recovery(
            VideoStreamWriteOutcome::Stopped,
            8,
            9,
        ));
        assert!(!video_write_outcome_requires_recovery(
            VideoStreamWriteOutcome::Complete,
            9,
            9,
        ));
        assert!(!video_write_outcome_requires_recovery(
            VideoStreamWriteOutcome::Superseded,
            9,
            9,
        ));
    }

    #[test]
    fn public_endpoint_requires_exact_authority_path_and_protocol() {
        assert!(PublicEndpoint::parse("https://example.test/transport").is_err());
        let endpoint = PublicEndpoint::parse("https://example.test:443/transport").unwrap();
        let token = [7_u8; 32];
        let encoded = encode_token(&token);
        let path = format!("/transport?v={PROTOCOL_VERSION}&token={encoded}");
        assert_eq!(
            endpoint.setup_url("setup-token"),
            "https://example.test:443/transport?v=4&token=setup-token"
        );
        assert_eq!(
            endpoint.validate_request("example.test", &path),
            Some(hash_token(&token))
        );
        assert_eq!(
            endpoint.validate_request("example.test:443", &path),
            Some(hash_token(&token))
        );
        assert!(endpoint.validate_request("evil.test", &path).is_none());
        assert!(
            endpoint
                .validate_request("example.test", &format!("/transport?v=3&token={encoded}"),)
                .is_none()
        );
        assert!(
            endpoint
                .validate_request("example.test", "/other?v=1&token=x")
                .is_none()
        );
    }

    #[test]
    fn https_origins_are_strictly_canonicalized() {
        assert_eq!(
            canonicalize_https_origin("HTTPS://Example.TEST:443"),
            Some("https://example.test".to_owned())
        );
        assert_eq!(
            canonicalize_https_origin("https://Example.TEST:8443"),
            Some("https://example.test:8443".to_owned())
        );
        assert_eq!(
            canonicalize_https_origin("https://[::1]:443"),
            Some("https://[::1]".to_owned())
        );
        for invalid in [
            "null",
            "http://example.test",
            "https://user@example.test",
            "https://example.test/",
            "https://example.test?x=1",
            "https://example.test#fragment",
            "https://example.test:not-a-port",
        ] {
            assert_eq!(canonicalize_https_origin(invalid), None, "{invalid}");
        }
    }

    #[tokio::test]
    async fn reliable_outbound_congestion_applies_backpressure_without_dropping() {
        let (outbound_tx, mut outbound_rx) = mpsc::channel(2);
        let (video_tx, _video_rx) = video_admission_channel();
        let (audio_tx, _audio_rx) = watch::channel(None);
        let (other_tx, mut other_rx) = reliable_outbound_channel(1);
        let (inbound_tx, _inbound_rx) = mpsc::channel(1);

        let first = Bytes::from_static(&[TransportChannelId::GENERAL, 1]);
        let second = Bytes::from_static(&[TransportChannelId::GENERAL, 2]);
        outbound_tx.send(first.clone()).await.unwrap();
        outbound_tx.send(second.clone()).await.unwrap();
        drop(outbound_tx);

        let dispatch = dispatch_outbound(
            &mut outbound_rx,
            video_tx,
            audio_tx,
            other_tx,
            inbound_tx,
            Arc::new(CongestionCounters::default()),
            Arc::new(RecoveryRequestState::default()),
        );
        tokio::pin!(dispatch);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut dispatch)
                .await
                .is_err(),
            "the second critical frame should wait for queue capacity"
        );
        assert_eq!(other_rx.recv().await, Some(first));
        assert!(dispatch.await.is_ok());
        assert_eq!(other_rx.recv().await, Some(second));
        assert_eq!(other_rx.recv().await, None);
    }

    fn stats_frame(kind: &str, marker: u8) -> Bytes {
        let json = format!(r#"{{"{kind}":{{"marker":{marker}}}}}"#);
        let mut frame = Vec::with_capacity(json.len() + 3);
        frame.push(TransportChannelId::STATS);
        frame.extend_from_slice(&(json.len() as u16).to_be_bytes());
        frame.extend_from_slice(json.as_bytes());
        Bytes::from(frame)
    }

    #[tokio::test]
    async fn reliable_outbound_replaces_stale_snapshots_and_preserves_critical_order() {
        let (tx, mut rx) = reliable_outbound_channel(6);
        let first = Bytes::from_static(&[TransportChannelId::GENERAL, 1]);
        let stale_stats = stats_frame("Video", 1);
        let stale_rtt = Bytes::from_static(&[TransportChannelId::RTT, 0, 0, 1]);
        let second = Bytes::from_static(&[TransportChannelId::GENERAL, 2]);
        let fresh_stats = stats_frame("Video", 2);
        let fresh_rtt = Bytes::from_static(&[TransportChannelId::RTT, 0, 0, 2]);

        tx.send(first.clone()).await.unwrap();
        tx.send(stale_stats).await.unwrap();
        tx.send(stale_rtt).await.unwrap();
        tx.send(second.clone()).await.unwrap();
        tx.send(fresh_stats.clone()).await.unwrap();
        tx.send(fresh_rtt.clone()).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(first));
        assert_eq!(rx.recv().await, Some(second));
        assert_eq!(rx.recv().await, Some(fresh_stats));
        assert_eq!(rx.recv().await, Some(fresh_rtt));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn reliable_outbound_keeps_independent_snapshot_kinds() {
        let (tx, mut rx) = reliable_outbound_channel(4);
        let stale_video = stats_frame("Video", 1);
        let stats_rtt = stats_frame("Rtt", 2);
        let fresh_video = stats_frame("Video", 3);
        let transport_rtt = Bytes::from_static(&[TransportChannelId::RTT, 0, 0, 4]);

        tx.send(stale_video).await.unwrap();
        tx.send(stats_rtt.clone()).await.unwrap();
        tx.send(fresh_video.clone()).await.unwrap();
        tx.send(transport_rtt.clone()).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(stats_rtt));
        assert_eq!(rx.recv().await, Some(fresh_video));
        assert_eq!(rx.recv().await, Some(transport_rtt));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn stream_control_is_strict_fifo_and_never_replaced() {
        let first = Bytes::from_static(&[TransportChannelId::STREAM_CONTROL, b'1']);
        let second = Bytes::from_static(&[TransportChannelId::STREAM_CONTROL, b'2']);
        assert_eq!(reliable_replacement_key(&first), None);
        assert_eq!(reliable_replacement_key(&second), None);

        let (tx, mut rx) = reliable_outbound_channel(2);
        tx.send(first.clone()).await.unwrap();
        tx.send(second.clone()).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(first));
        assert_eq!(rx.recv().await, Some(second));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn reliable_outbound_strict_admission_evicts_oldest_replaceable_when_full() {
        let (tx, mut rx) = reliable_outbound_channel(2);
        let stale_stats = stats_frame("Video", 1);
        let existing_strict = Bytes::from_static(&[TransportChannelId::GENERAL, 1]);
        let new_strict = Bytes::from_static(&[TransportChannelId::GENERAL, 2]);

        tx.send(stale_stats).await.unwrap();
        tx.send(existing_strict.clone()).await.unwrap();
        tx.send(new_strict.clone()).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(existing_strict));
        assert_eq!(rx.recv().await, Some(new_strict));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn reliable_outbound_drops_new_snapshot_when_full_of_strict_frames() {
        let (tx, mut rx) = reliable_outbound_channel(2);
        let first = Bytes::from_static(&[TransportChannelId::GENERAL, 1]);
        let second = Bytes::from_static(&[TransportChannelId::GENERAL, 2]);
        let rtt = Bytes::from_static(&[TransportChannelId::RTT, 0, 0, 3]);

        tx.send(first.clone()).await.unwrap();
        tx.send(second.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_millis(10), tx.send(rtt))
            .await
            .expect("replaceable telemetry must not wait behind strict frames")
            .unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(first));
        assert_eq!(rx.recv().await, Some(second));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn reliable_outbound_closure_wakes_waiters_and_drains_after_sender_close() {
        let (tx, mut rx) = reliable_outbound_channel(1);
        let frame = Bytes::from_static(&[TransportChannelId::GENERAL, 1]);
        tx.send(frame.clone()).await.unwrap();
        drop(tx);
        assert_eq!(rx.recv().await, Some(frame));
        assert_eq!(rx.recv().await, None);

        let (tx, rx) = reliable_outbound_channel(1);
        drop(rx);
        assert_eq!(
            tx.send(Bytes::from_static(&[TransportChannelId::GENERAL]))
                .await,
            Err(ReliableOutboundSendError::Closed)
        );

        let (tx, rx) = reliable_outbound_channel(1);
        tx.send(Bytes::from_static(&[TransportChannelId::GENERAL, 1]))
            .await
            .unwrap();
        let blocked_send = tx.send(Bytes::from_static(&[TransportChannelId::GENERAL, 2]));
        tokio::pin!(blocked_send);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut blocked_send)
                .await
                .is_err()
        );
        drop(rx);
        assert_eq!(blocked_send.await, Err(ReliableOutboundSendError::Closed));

        let (tx, mut rx) = reliable_outbound_channel(1);
        let waiting_receive = rx.recv();
        tokio::pin!(waiting_receive);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting_receive)
                .await
                .is_err()
        );
        drop(tx);
        assert_eq!(waiting_receive.await, None);
    }

    #[tokio::test]
    async fn dropping_a_delta_after_a_queued_idr_requests_a_fresh_idr() {
        let (outbound_tx, mut outbound_rx) = mpsc::channel(VIDEO_QUEUE_CAPACITY + 1);
        let (video_tx, mut video_rx) = video_admission_channel();
        let (audio_tx, _audio_rx) = watch::channel(None);
        let (other_tx, _other_rx) = reliable_outbound_channel(1);
        let (inbound_tx, mut inbound_rx) = mpsc::channel(2);

        let idr = Bytes::from_static(&[TransportChannelId::HOST_VIDEO, 1, 0xaa]);
        outbound_tx.send(idr.clone()).await.unwrap();
        for index in 0..VIDEO_QUEUE_CAPACITY {
            outbound_tx
                .send(Bytes::from(vec![
                    TransportChannelId::HOST_VIDEO,
                    0,
                    index as u8,
                ]))
                .await
                .unwrap();
        }
        drop(outbound_tx);

        dispatch_outbound(
            &mut outbound_rx,
            video_tx,
            audio_tx,
            other_tx,
            inbound_tx,
            Arc::new(CongestionCounters::default()),
            Arc::new(RecoveryRequestState::default()),
        )
        .await
        .unwrap();

        assert_eq!(video_rx.recv().await, Some(idr));
        assert_eq!(
            inbound_rx.recv().await.as_deref(),
            Some(&[TransportChannelId::HOST_VIDEO, 0][..])
        );
    }

    #[tokio::test]
    async fn idr_supersedes_a_full_stale_delta_queue() {
        let (outbound_tx, mut outbound_rx) = mpsc::channel(VIDEO_QUEUE_CAPACITY + 1);
        let (video_tx, mut video_rx) = video_admission_channel();
        let (audio_tx, _audio_rx) = watch::channel(None);
        let (other_tx, _other_rx) = reliable_outbound_channel(1);
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);

        for index in 0..VIDEO_QUEUE_CAPACITY {
            outbound_tx
                .send(Bytes::from(vec![
                    TransportChannelId::HOST_VIDEO,
                    0,
                    index as u8,
                ]))
                .await
                .unwrap();
        }
        let idr = Bytes::from_static(&[TransportChannelId::HOST_VIDEO, 1, 0xcc]);
        outbound_tx.send(idr.clone()).await.unwrap();
        drop(outbound_tx);

        dispatch_outbound(
            &mut outbound_rx,
            video_tx,
            audio_tx,
            other_tx,
            inbound_tx,
            Arc::new(CongestionCounters::default()),
            Arc::new(RecoveryRequestState::default()),
        )
        .await
        .unwrap();

        assert_eq!(video_rx.recv().await, Some(idr));
        assert_eq!(video_rx.recv().await, None);
        assert!(inbound_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn failed_video_recovery_request_is_retried_after_capacity_returns() {
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
        inbound_tx
            .send(Bytes::from_static(&[TransportChannelId::GENERAL]))
            .await
            .unwrap();
        let recovery_request = RecoveryRequestState::default();
        let counters = CongestionCounters::default();

        // This models the first timeout attempt racing a full input queue.
        try_enqueue_video_recovery_request(&inbound_tx, &recovery_request, &counters);
        assert!(!recovery_request.enqueued.load(Ordering::Acquire));
        assert_eq!(
            inbound_rx.recv().await.as_deref(),
            Some(&[TransportChannelId::GENERAL][..])
        );

        // A subsequent discarded delta retries instead of waiting forever
        // for an IDR that was never requested.
        try_enqueue_video_recovery_request(&inbound_tx, &recovery_request, &counters);
        assert!(recovery_request.enqueued.load(Ordering::Acquire));
        assert_eq!(
            inbound_rx.recv().await.as_deref(),
            Some(&[TransportChannelId::HOST_VIDEO, 0][..])
        );

        // Once latched, later deltas do not create an IDR request storm.
        try_enqueue_video_recovery_request(&inbound_tx, &recovery_request, &counters);
        assert!(inbound_rx.try_recv().is_err());
        assert_eq!(counters.recovery_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn short_video_burst_does_not_trigger_false_congestion_recovery() {
        let (outbound_tx, mut outbound_rx) = mpsc::channel(VIDEO_QUEUE_CAPACITY);
        let (video_tx, mut video_rx) = video_admission_channel();
        let (audio_tx, _audio_rx) = watch::channel(None);
        let (other_tx, _other_rx) = reliable_outbound_channel(1);
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);

        let frames: Vec<_> = (0..VIDEO_QUEUE_CAPACITY)
            .map(|index| {
                Bytes::from(vec![
                    TransportChannelId::HOST_VIDEO,
                    u8::from(index == 0),
                    index as u8,
                ])
            })
            .collect();
        for frame in &frames {
            outbound_tx.send(frame.clone()).await.unwrap();
        }
        drop(outbound_tx);

        dispatch_outbound(
            &mut outbound_rx,
            video_tx,
            audio_tx,
            other_tx,
            inbound_tx,
            Arc::new(CongestionCounters::default()),
            Arc::new(RecoveryRequestState::default()),
        )
        .await
        .unwrap();

        for frame in frames {
            assert_eq!(video_rx.recv().await, Some(frame));
        }
        assert!(inbound_rx.try_recv().is_err());
    }

    #[test]
    fn cumulative_motion_recovers_loss_and_rejects_reordering() {
        let mut state = DatagramState::default();
        let packet = |sequence: u32, x: i32, y: i32| {
            let mut bytes = vec![DATAGRAM_MAGIC, DATAGRAM_RELATIVE_MOTION];
            bytes.extend_from_slice(&9_u16.to_be_bytes());
            bytes.extend_from_slice(&sequence.to_be_bytes());
            bytes.extend_from_slice(&x.to_be_bytes());
            bytes.extend_from_slice(&y.to_be_bytes());
            bytes
        };

        assert_eq!(
            state.decode(&packet(1, 4, -2)).unwrap()[0].as_ref(),
            &[6, 0, 0, 4, 0xff, 0xfe]
        );
        // Sequence 2 is lost. The cumulative value in 3 recovers both deltas.
        assert_eq!(
            state.decode(&packet(3, 15, 6)).unwrap()[0].as_ref(),
            &[6, 0, 0, 11, 0, 8]
        );
        assert!(state.decode(&packet(2, 8, 2)).is_none());
        assert!(state.decode(&packet(3, 15, 6)).is_none());
    }

    #[test]
    fn cumulative_epoch_and_sequence_wrap_are_newer() {
        assert!(is_newer_u32(0, u32::MAX));
        assert!(is_newer_u16(0, u16::MAX));
        assert!(!is_newer_u32(u32::MAX, 0));
        assert!(!is_newer_u16(u16::MAX, 0));
    }

    #[test]
    fn large_relative_delta_is_split_into_valid_i16_packets() {
        let frames = split_mouse_delta(0, 70_000, -70_000);
        assert_eq!(frames.len(), 3);
        assert!(frames.iter().all(|frame| frame.len() == 6));
        let sum_x: i32 = frames
            .iter()
            .map(|frame| i16::from_be_bytes([frame[2], frame[3]]) as i32)
            .sum();
        let sum_y: i32 = frames
            .iter()
            .map(|frame| i16::from_be_bytes([frame[4], frame[5]]) as i32)
            .sum();
        assert_eq!((sum_x, sum_y), (70_000, -70_000));
    }

    #[test]
    fn snapshot_is_channel_prefixed_and_sequenced() {
        let mut state = DatagramState::default();
        let mut packet = vec![
            DATAGRAM_MAGIC,
            DATAGRAM_SNAPSHOT,
            TransportChannelId::CONTROLLER0,
        ];
        packet.extend_from_slice(&2_u16.to_be_bytes());
        packet.extend_from_slice(&10_u32.to_be_bytes());
        packet.extend_from_slice(&[0xaa, 0xbb]);
        assert_eq!(
            state.decode(&packet).unwrap()[0].as_ref(),
            &[TransportChannelId::CONTROLLER0, 0xaa, 0xbb]
        );
        assert!(state.decode(&packet).is_none());

        let mut invalid = vec![
            DATAGRAM_MAGIC,
            DATAGRAM_SNAPSHOT,
            TransportChannelId::KEYBOARD,
        ];
        invalid.extend_from_slice(&2_u16.to_be_bytes());
        invalid.extend_from_slice(&11_u32.to_be_bytes());
        invalid.push(0xaa);
        assert!(state.decode(&invalid).is_none());

        let mut control = vec![
            DATAGRAM_MAGIC,
            DATAGRAM_SNAPSHOT,
            TransportChannelId::STREAM_CONTROL,
        ];
        control.extend_from_slice(&2_u16.to_be_bytes());
        control.extend_from_slice(&12_u32.to_be_bytes());
        control.push(0xaa);
        assert!(state.decode(&control).is_none());
    }

    #[test]
    fn reliable_snapshot_barrier_rejects_a_delayed_older_datagram() {
        let packet = |sequence: u32, value: u8| {
            let mut bytes = vec![
                DATAGRAM_MAGIC,
                DATAGRAM_SNAPSHOT,
                TransportChannelId::MOUSE_ABSOLUTE,
            ];
            bytes.extend_from_slice(&7_u16.to_be_bytes());
            bytes.extend_from_slice(&sequence.to_be_bytes());
            bytes.push(value);
            bytes
        };
        let mut shared_state = DatagramState::default();

        // The reliable fallback uses the same envelope and advances the state
        // shared with the datagram reader.
        let reliable = packet(11, 0xbb);
        assert!(is_snapshot_envelope(&reliable));
        assert_eq!(
            shared_state.decode(&reliable).unwrap()[0].as_ref(),
            &[TransportChannelId::MOUSE_ABSOLUTE, 0xbb]
        );

        // An older datagram that was already in the network is ignored.
        assert!(shared_state.decode(&packet(10, 0xaa)).is_none());
        assert_eq!(
            shared_state.decode(&packet(12, 0xcc)).unwrap()[0].as_ref(),
            &[TransportChannelId::MOUSE_ABSOLUTE, 0xcc]
        );

        // The inverse arrival order is monotonic too: an earlier datagram can
        // be delivered first, then the newer reliable snapshot supersedes it.
        let mut datagram_first = DatagramState::default();
        assert_eq!(
            datagram_first.decode(&packet(10, 0xaa)).unwrap()[0].as_ref(),
            &[TransportChannelId::MOUSE_ABSOLUTE, 0xaa]
        );
        assert_eq!(
            datagram_first.decode(&reliable).unwrap()[0].as_ref(),
            &[TransportChannelId::MOUSE_ABSOLUTE, 0xbb]
        );
        assert!(datagram_first.decode(&reliable).is_none());
        assert!(!is_snapshot_envelope(
            &[TransportChannelId::KEYBOARD, 0x01,]
        ));
    }
}
