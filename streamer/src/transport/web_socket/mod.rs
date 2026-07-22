use std::{
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use common::{
    api_bindings::{StreamClientMessage, StreamerStatsUpdate, TransportChannelId},
    ipc::{IpcSender, ServerIpcMessage, StreamerIpcMessage},
};
use log::{trace, warn};
use moonlight_common::stream::{
    audio::{AudioConfig, OpusMultistreamConfig},
    video::{DecodeResult, FrameType, VideoDecodeUnit, VideoSetup},
};
use tokio::{
    spawn,
    sync::{
        Mutex,
        mpsc::{Receiver, Sender, channel, error::TrySendError},
    },
    time::{MissedTickBehavior, interval},
};

use crate::transport::{
    InboundPacket, OutboundPacket, TransportChannel, TransportError, TransportEvent,
    TransportEvents, TransportSender,
};

const INBOUND_EVENT_QUEUE_CAPACITY: usize = 64;
const VIDEO_HEADER_BYTES: usize = 6;
const RTT_PROBE_INTERVAL: Duration = Duration::from_millis(200);
const RTT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

pub async fn new(
    ipc_sender: IpcSender<StreamerIpcMessage>,
) -> Result<(WebSocketTransportSender, WebSocketTransportEvents), anyhow::Error> {
    // This queue is only for browser-to-host traffic. Outbound media bypasses it,
    // so a slow browser connection cannot hold up mouse or keyboard input here.
    let (event_sender, event_receiver) = channel::<TransportEvent>(INBOUND_EVENT_QUEUE_CAPACITY);

    let rtt = Arc::new(Mutex::new(RttProbeState::new()));

    let sender = WebSocketTransportSender {
        event_sender,
        ipc_sender,
        rtt,
        recovery: VideoRecoveryState::default(),
    };

    // Probe independently of replies. A dropped reply is retried after the
    // bounded timeout rather than permanently stopping RTT measurements.
    spawn(run_rtt_probe_loop(
        Arc::downgrade(&sender.rtt),
        sender.ipc_sender.clone(),
    ));

    Ok((sender, WebSocketTransportEvents { event_receiver }))
}

pub struct WebSocketTransportEvents {
    event_receiver: Receiver<TransportEvent>,
}

#[async_trait]
impl TransportEvents for WebSocketTransportEvents {
    async fn poll_event(&mut self) -> Result<TransportEvent, TransportError> {
        trace!("Polling WebSocketEvents");
        self.event_receiver
            .recv()
            .await
            .ok_or(TransportError::Closed)
    }
}

pub struct WebSocketTransportSender {
    event_sender: Sender<TransportEvent>,
    ipc_sender: IpcSender<StreamerIpcMessage>,
    rtt: Arc<Mutex<RttProbeState>>,
    recovery: VideoRecoveryState,
}

async fn send_packet(
    ipc_sender: &IpcSender<StreamerIpcMessage>,
    packet: OutboundPacket,
) -> Result<(), TransportError> {
    let mut serialized = Vec::new();

    let (id, range) = match packet.serialize(&mut serialized) {
        Some(packet) => packet,
        None => {
            warn!("Failed to serialize packet: {packet:?}");
            return Ok(());
        }
    };

    // Allocate the final wire message at its exact size instead of growing and
    // shifting the serialization buffer in place.
    let mut framed = Vec::with_capacity(range.len() + 1);
    framed.push(id.0);
    framed.extend_from_slice(&serialized[range]);

    ipc_sender
        .send_checked(StreamerIpcMessage::WebSocketTransport(Bytes::from(framed)))
        .await
        .map_err(|_| TransportError::Closed)?;

    Ok(())
}

struct RttProbeState {
    sent_at: Option<Instant>,
    sequence_number: u16,
    awaiting_reply: bool,
}

impl RttProbeState {
    fn new() -> Self {
        Self {
            sent_at: None,
            sequence_number: 0,
            awaiting_reply: false,
        }
    }

    fn start_probe_if_due(&mut self, now: Instant) -> Option<u16> {
        if self.awaiting_reply
            && self
                .sent_at
                .is_some_and(|sent_at| now.duration_since(sent_at) < RTT_PROBE_TIMEOUT)
        {
            return None;
        }

        self.sequence_number = self.sequence_number.wrapping_add(1);
        self.sent_at = Some(now);
        self.awaiting_reply = true;
        Some(self.sequence_number)
    }

    fn accept_reply(&mut self, sequence_number: u16, now: Instant) -> Option<Duration> {
        if !self.awaiting_reply || sequence_number != self.sequence_number {
            return None;
        }

        self.awaiting_reply = false;
        self.sent_at.take().map(|sent_at| now - sent_at)
    }
}

async fn run_rtt_probe_loop(
    rtt_mutex: Weak<Mutex<RttProbeState>>,
    ipc_sender: IpcSender<StreamerIpcMessage>,
) {
    let mut ticker = interval(RTT_PROBE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let Some(rtt_mutex) = rtt_mutex.upgrade() else {
            return;
        };
        if ipc_sender.is_closed() {
            return;
        }

        let sequence_number = rtt_mutex.lock().await.start_probe_if_due(Instant::now());
        drop(rtt_mutex);

        let Some(sequence_number) = sequence_number else {
            continue;
        };
        if let Err(err) = send_packet(&ipc_sender, OutboundPacket::Rtt { sequence_number }).await {
            warn!(
                "Failed to send web socket rtt packet with sequence number {sequence_number}: {err}"
            );
            return;
        }
    }
}

async fn recv_rtt(
    rtt_mutex: Arc<Mutex<RttProbeState>>,
    ipc_sender: IpcSender<StreamerIpcMessage>,
    recv_sequence_number: u16,
) {
    let (expected_sequence_number, rtt) = {
        let mut state = rtt_mutex.lock().await;
        let expected = state.sequence_number;
        (
            expected,
            state.accept_reply(recv_sequence_number, Instant::now()),
        )
    };

    let Some(rtt) = rtt else {
        warn!(
            "Expected rtt packet with sequence_number {expected_sequence_number} but got {recv_sequence_number}"
        );
        return;
    };

    if let Err(err) = send_packet(
        &ipc_sender,
        OutboundPacket::Stats(StreamerStatsUpdate::BrowserRtt {
            rtt_ms: rtt.as_secs_f64() * 1000.0,
        }),
    )
    .await
    {
        warn!("Failed to send rtt stats update for web socket: {err}");
    }
}

#[derive(Default)]
struct VideoRecoveryState {
    inner: StdMutex<VideoRecoveryInner>,
}

#[derive(Default)]
struct VideoRecoveryInner {
    recovering: bool,
    /// True after returning NeedIdr for congestion. While latched, discarded
    /// P-frames return Ok so the decoder is not flooded with duplicate IDRs.
    recovery_request_outstanding: bool,
    /// Kept separate so an explicit browser request is never swallowed by an
    /// already-outstanding congestion recovery request.
    browser_requested_idr: bool,
}

impl VideoRecoveryState {
    fn lock(&self) -> std::sync::MutexGuard<'_, VideoRecoveryInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns a result only when this frame should be discarded before an IPC
    /// enqueue is attempted.
    fn before_enqueue(&self, frame_type: FrameType) -> Option<DecodeResult> {
        let mut state = self.lock();
        if !state.recovering || frame_type != FrameType::PFrame {
            return None;
        }

        if state.browser_requested_idr {
            state.browser_requested_idr = false;
            state.recovery_request_outstanding = true;
            return Some(DecodeResult::NeedIdr);
        }
        if !state.recovery_request_outstanding {
            state.recovery_request_outstanding = true;
            return Some(DecodeResult::NeedIdr);
        }
        Some(DecodeResult::Ok)
    }

    fn request_idr(&self) {
        self.lock().browser_requested_idr = true;
    }

    fn on_enqueue_failed(&self, frame_type: FrameType) -> DecodeResult {
        let mut state = self.lock();
        state.recovering = true;

        // Receipt of an IDR means the previous request was serviced. If that
        // IDR itself cannot enter IPC, immediately request exactly one new IDR.
        if frame_type == FrameType::Idr {
            state.recovery_request_outstanding = true;
            state.browser_requested_idr = false;
            return DecodeResult::NeedIdr;
        }

        if state.browser_requested_idr {
            state.browser_requested_idr = false;
            state.recovery_request_outstanding = true;
            return DecodeResult::NeedIdr;
        }
        if !state.recovery_request_outstanding {
            state.recovery_request_outstanding = true;
            DecodeResult::NeedIdr
        } else {
            DecodeResult::Ok
        }
    }

    fn on_enqueued(&self, frame_type: FrameType) -> DecodeResult {
        let mut state = self.lock();
        if frame_type == FrameType::Idr {
            state.recovering = false;
            state.recovery_request_outstanding = false;
            // An accepted IDR also satisfies an explicit browser request that
            // raced with, or directly triggered, this frame.
            state.browser_requested_idr = false;
            return DecodeResult::Ok;
        }

        if state.browser_requested_idr {
            state.browser_requested_idr = false;
            DecodeResult::NeedIdr
        } else {
            DecodeResult::Ok
        }
    }
}

fn encode_video_frame<'a>(
    frame_type: FrameType,
    timestamp_us: u32,
    buffers: impl IntoIterator<Item = &'a [u8]>,
    payload_len: usize,
) -> Vec<u8> {
    let mut framed = Vec::with_capacity(VIDEO_HEADER_BYTES + payload_len);
    framed.push(TransportChannelId::HOST_VIDEO);
    framed.push(match frame_type {
        FrameType::Idr => 1,
        FrameType::PFrame => 0,
    });
    framed.extend_from_slice(&timestamp_us.to_be_bytes());
    for buffer in buffers {
        framed.extend_from_slice(buffer);
    }
    framed
}

#[async_trait]
impl TransportSender for WebSocketTransportSender {
    async fn setup_video(&self, _setup: VideoSetup) -> i32 {
        // empty
        0
    }
    async fn send_video_unit<'a>(
        &'a self,
        unit: VideoDecodeUnit<&'a [u8]>,
    ) -> Result<DecodeResult, TransportError> {
        if let Some(result) = self.recovery.before_enqueue(unit.frame_type) {
            return Ok(result);
        }

        // Wire format: channel id (1), frame type (1), timestamp in us (4),
        // followed by the Annex-B access unit. Keep this header in sync with
        // DepacketizeVideoPipe in the browser.
        let payload_len = unit.buffers.iter().map(|buffer| buffer.data.len()).sum();
        let frame_type = unit.frame_type;
        let framed = encode_video_frame(
            frame_type,
            unit.timestamp.as_micros() as u32,
            unit.buffers.iter().map(|buffer| buffer.data),
            payload_len,
        );

        match self
            .ipc_sender
            .try_send_low_priority(StreamerIpcMessage::WebSocketTransport(Bytes::from(framed)))
        {
            Ok(()) => Ok(self.recovery.on_enqueued(frame_type)),
            Err(TrySendError::Full(_)) => Ok(self.recovery.on_enqueue_failed(frame_type)),
            Err(TrySendError::Closed(_)) => Err(TransportError::Closed),
        }
    }

    async fn setup_audio(
        &self,
        _audio_config: AudioConfig,
        _stream_config: OpusMultistreamConfig,
    ) -> i32 {
        // empty
        0
    }
    async fn send_audio_sample(&self, data: &[u8]) -> Result<(), TransportError> {
        let mut framed = Vec::with_capacity(data.len() + 1);
        framed.push(TransportChannelId::HOST_AUDIO);
        framed.extend_from_slice(data);

        match self
            .ipc_sender
            .try_send_realtime(StreamerIpcMessage::WebSocketTransport(Bytes::from(framed)))
        {
            Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Closed(_)) => Err(TransportError::Closed),
        }
    }

    async fn send(&self, packet: OutboundPacket) -> Result<(), TransportError> {
        send_packet(&self.ipc_sender, packet).await
    }

    async fn on_ipc_message(&self, message: ServerIpcMessage) -> Result<(), TransportError> {
        match message {
            ServerIpcMessage::WebSocketTransport(message) => {
                if message.is_empty() {
                    warn!("Empty packet received!");
                    return Ok(());
                }

                let channel_id = message[0];

                let Some(packet) =
                    InboundPacket::deserialize(TransportChannel(channel_id), &message[1..])
                else {
                    warn!("Failed to receive packet on channel {channel_id}");
                    return Ok(());
                };

                if let InboundPacket::RequestVideoIdr = packet {
                    self.recovery.request_idr();
                }

                if let InboundPacket::Rtt { sequence_number } = packet {
                    spawn(recv_rtt(
                        self.rtt.clone(),
                        self.ipc_sender.clone(),
                        sequence_number,
                    ));
                }

                if self
                    .event_sender
                    .send(TransportEvent::RecvPacket(packet))
                    .await
                    .is_err()
                {
                    return Err(TransportError::Closed);
                }
            }
            #[allow(clippy::collapsible_match)]
            ServerIpcMessage::WebSocket(StreamClientMessage::StartStream { settings }) => {
                if self
                    .event_sender
                    .send(TransportEvent::StartStream { settings })
                    .await
                    .is_err()
                {
                    warn!("Failed to send start stream event");
                    return Err(TransportError::Closed);
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_setup_complete(&self) {
        // empty
    }

    async fn close(&self) -> Result<(), TransportError> {
        // emtpy
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_frame_wire_format_is_byte_exact() {
        let first = [0x00, 0x00, 0x00, 0x01, 0x67];
        let second = [0x00, 0x00, 0x01, 0x65, 0xaa];
        let encoded = encode_video_frame(
            FrameType::Idr,
            0x0102_0304,
            [&first[..], &second[..]],
            first.len() + second.len(),
        );

        assert_eq!(
            encoded,
            [
                TransportChannelId::HOST_VIDEO,
                1,
                0x01,
                0x02,
                0x03,
                0x04,
                0x00,
                0x00,
                0x00,
                0x01,
                0x67,
                0x00,
                0x00,
                0x01,
                0x65,
                0xaa,
            ]
        );
        assert_eq!(encoded.len(), encoded.capacity());
    }

    #[test]
    fn overload_requests_one_idr_then_latches_until_it_arrives() {
        let state = VideoRecoveryState::default();

        assert!(matches!(
            state.on_enqueue_failed(FrameType::PFrame),
            DecodeResult::NeedIdr
        ));
        for _ in 0..100 {
            assert!(matches!(
                state.before_enqueue(FrameType::PFrame),
                Some(DecodeResult::Ok)
            ));
        }
        assert!(state.before_enqueue(FrameType::Idr).is_none());

        assert!(matches!(
            state.on_enqueued(FrameType::Idr),
            DecodeResult::Ok
        ));
        assert!(state.before_enqueue(FrameType::PFrame).is_none());
    }

    #[test]
    fn failed_idr_enqueue_reissues_recovery_once() {
        let state = VideoRecoveryState::default();

        assert!(matches!(
            state.on_enqueue_failed(FrameType::PFrame),
            DecodeResult::NeedIdr
        ));
        assert!(state.before_enqueue(FrameType::Idr).is_none());
        assert!(matches!(
            state.on_enqueue_failed(FrameType::Idr),
            DecodeResult::NeedIdr
        ));
        assert!(matches!(
            state.before_enqueue(FrameType::PFrame),
            Some(DecodeResult::Ok)
        ));

        assert!(state.before_enqueue(FrameType::Idr).is_none());
        assert!(matches!(
            state.on_enqueued(FrameType::Idr),
            DecodeResult::Ok
        ));
        assert!(state.before_enqueue(FrameType::PFrame).is_none());
    }

    #[test]
    fn explicit_browser_idr_request_is_preserved() {
        let state = VideoRecoveryState::default();
        state.request_idr();

        assert!(matches!(
            state.on_enqueued(FrameType::PFrame),
            DecodeResult::NeedIdr
        ));
        assert!(matches!(
            state.on_enqueued(FrameType::PFrame),
            DecodeResult::Ok
        ));
    }

    #[test]
    fn an_accepted_idr_satisfies_an_explicit_browser_request() {
        let state = VideoRecoveryState::default();
        state.request_idr();

        assert!(matches!(
            state.on_enqueued(FrameType::Idr),
            DecodeResult::Ok
        ));
        assert!(matches!(
            state.on_enqueued(FrameType::PFrame),
            DecodeResult::Ok
        ));
    }

    #[test]
    fn explicit_browser_idr_is_not_swallowed_by_recovery_latch() {
        let state = VideoRecoveryState::default();
        assert!(matches!(
            state.on_enqueue_failed(FrameType::PFrame),
            DecodeResult::NeedIdr
        ));

        state.request_idr();
        assert!(matches!(
            state.before_enqueue(FrameType::PFrame),
            Some(DecodeResult::NeedIdr)
        ));
        assert!(matches!(
            state.before_enqueue(FrameType::PFrame),
            Some(DecodeResult::Ok)
        ));
    }

    #[test]
    fn rtt_probe_retries_only_after_timeout_and_ignores_old_replies() {
        let mut state = RttProbeState::new();
        let start = Instant::now();
        let first = state
            .start_probe_if_due(start)
            .expect("first probe should start immediately");

        assert!(
            state
                .start_probe_if_due(start + RTT_PROBE_TIMEOUT - Duration::from_millis(1))
                .is_none(),
            "only one probe may be outstanding before its timeout"
        );

        let second = state
            .start_probe_if_due(start + RTT_PROBE_TIMEOUT)
            .expect("lost reply should be retried at the timeout");
        assert_ne!(second, first);
        assert!(
            state
                .accept_reply(first, start + RTT_PROBE_TIMEOUT + Duration::from_millis(1))
                .is_none(),
            "a late reply for the timed-out probe must not complete the new one"
        );
        assert_eq!(
            state.accept_reply(
                second,
                start + RTT_PROBE_TIMEOUT + Duration::from_millis(25)
            ),
            Some(Duration::from_millis(25))
        );
        assert!(
            state
                .start_probe_if_due(start + RTT_PROBE_TIMEOUT + Duration::from_millis(26))
                .is_some(),
            "a valid reply allows the next interval to start one new probe"
        );
    }
}
