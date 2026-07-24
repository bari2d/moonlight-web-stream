use std::{
    marker::PhantomData,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use log::LevelFilter;
use pem::Pem;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{
        AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, Stdin,
        Stdout,
    },
    process::{ChildStderr, ChildStdin, ChildStdout},
    spawn,
    sync::{
        Notify,
        mpsc::{
            Receiver, Sender, channel,
            error::{SendError, TrySendError},
        },
    },
};
use tracing::{Span, info, trace, warn};

use crate::{
    api_bindings::{StreamClientMessage, StreamPermissions, StreamServerMessage},
    config::WebRtcConfig,
};

/// Upper bound for one encoded IPC message. This is deliberately large enough
/// for high-resolution encoded video frames while preventing a corrupt length
/// prefix from causing an unbounded allocation.
const MAX_IPC_FRAME_BYTES: usize = 64 * 1024 * 1024;
const IPC_LENGTH_PREFIX_BYTES: usize = size_of::<u32>();
/// Reliable control and lifecycle messages. This queue is deliberately
/// independent from realtime media so video congestion cannot delay Stop,
/// setup, input, or RTT messages.
const IPC_RELIABLE_QUEUE_CAPACITY: usize = 16;
/// Short realtime queue used by audio. It is drained before low-priority video,
/// but remains bounded so stale audio cannot accumulate latency.
const IPC_REALTIME_QUEUE_CAPACITY: usize = 4;

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamerConfig {
    pub webrtc: WebRtcConfig,
    pub log_level: LevelFilter,
}

/// Coalesced, cumulative congestion telemetry from the browser-facing QUIC
/// connection. The streamer uses deltas between snapshots to make bitrate
/// decisions without putting feedback on the latency-sensitive input lane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkFeedback {
    pub rtt_ms: u32,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub congestion_events: u64,
    pub admission_drops: u64,
    pub video_write_timeouts: u64,
    pub recovery_requests: u64,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerIpcMessage {
    Init {
        config: StreamerConfig,
        host_address: String,
        host_http_port: u16,
        client_unique_id: Option<String>,
        client_private_key: Pem,
        client_certificate: Pem,
        server_certificate: Pem,
        app_id: u32,
        video_frame_queue_size: usize,
        audio_sample_queue_size: usize,
        permissions: StreamPermissions,
    },
    WebSocket(StreamClientMessage),
    WebSocketTransport(Bytes),
    NetworkFeedback(NetworkFeedback),
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamerIpcMessage {
    WebSocket(StreamServerMessage),
    WebSocketTransport(Bytes),
    Stop,
}

#[derive(Debug)]
struct PendingSlot<Message> {
    message: StdMutex<Option<Message>>,
    notify: Notify,
    writer_closed: AtomicBool,
}

impl<Message> PendingSlot<Message> {
    fn new() -> Self {
        Self {
            message: StdMutex::new(None),
            notify: Notify::new(),
            writer_closed: AtomicBool::new(false),
        }
    }

    fn take(&self) -> Option<Message> {
        self.message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    fn close(&self) {
        // Serialize closure with slot insertion. Once close returns, a
        // concurrent sender cannot pass its second closed check and publish an
        // undrainable message after the IPC writer has exited.
        let mut message = self
            .message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.writer_closed.store(true, Ordering::Release);
        message.take();
        self.notify.notify_waiters();
    }
}

struct IpcQueues<Message> {
    reliable: Receiver<Message>,
    realtime: Receiver<Message>,
    latest: Arc<PendingSlot<Message>>,
    low_priority: Arc<PendingSlot<Message>>,
}

fn priority_channel<Message>(span: Span) -> (IpcSender<Message>, IpcQueues<Message>) {
    let (reliable_sender, reliable) = channel(IPC_RELIABLE_QUEUE_CAPACITY);
    let (realtime_sender, realtime) = channel(IPC_REALTIME_QUEUE_CAPACITY);
    let latest = Arc::new(PendingSlot::new());
    let low_priority = Arc::new(PendingSlot::new());

    (
        IpcSender {
            reliable_sender,
            realtime_sender,
            latest: latest.clone(),
            low_priority: low_priority.clone(),
            span,
        },
        IpcQueues {
            reliable,
            realtime,
            latest,
            low_priority,
        },
    )
}

// We're using the:
// Stdin: message passing
// Stdout: message passing
// Stderr: logging

pub async fn create_child_ipc<Message, ChildMessage>(
    span: Span,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: Option<ChildStderr>,
) -> (IpcSender<Message>, IpcReceiver<ChildMessage>)
where
    Message: Send + Serialize + 'static,
    ChildMessage: DeserializeOwned,
{
    if let Some(stderr) = stderr {
        // This is the log output of the streamer
        let span = span.clone();

        spawn(async move {
            let buf_reader = BufReader::new(stderr);
            let mut lines = buf_reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!(parent: &span, "{line}");
            }
        });
    }

    let (sender, queues) = priority_channel::<Message>(span.clone());

    spawn({
        let span = span.clone();

        async move {
            ipc_sender(span.clone(), stdin, queues).await;
        }
    });

    (
        sender,
        IpcReceiver {
            errored: false,
            read: create_reader(stdout),
            encoded: Vec::new(),
            phantom: Default::default(),
            span,
        },
    )
}

pub async fn create_process_ipc<ParentMessage, Message>(
    span: Span,
    stdin: Stdin,
    stdout: Stdout,
) -> (IpcSender<Message>, IpcReceiver<ParentMessage>)
where
    ParentMessage: DeserializeOwned,
    Message: Send + Serialize + 'static,
{
    let (sender, queues) = priority_channel::<Message>(span.clone());

    spawn({
        let span = span.clone();

        async move {
            ipc_sender(span.clone(), stdout, queues).await;
        }
    });

    (
        sender,
        IpcReceiver {
            errored: false,
            read: create_reader(stdin),
            encoded: Vec::new(),
            phantom: Default::default(),
            span,
        },
    )
}
fn create_reader(
    read: impl AsyncRead + Send + Unpin + 'static,
) -> Box<dyn AsyncRead + Send + Unpin + 'static> {
    Box::new(BufReader::new(read))
}

async fn ipc_sender<Message>(
    span: Span,
    mut write: impl AsyncWrite + Unpin,
    mut queues: IpcQueues<Message>,
) where
    Message: Serialize,
{
    // Reuse one framed buffer. Encoded video can be several MiB, so allocating
    // a new body and issuing a separate prefix write for every frame creates
    // avoidable allocator and pipe overhead on the hottest IPC path.
    let mut frame = Vec::new();
    while let Some(value) = next_ipc_message(&mut queues).await {
        frame.clear();
        frame.resize(IPC_LENGTH_PREFIX_BYTES, 0);
        if let Err(err) =
            bincode::serde::encode_into_std_write(&value, &mut frame, bincode::config::standard())
        {
            warn!(parent: &span, "[Ipc]: failed to encode message: {err}");
            continue;
        }

        let encoded_len = frame.len() - IPC_LENGTH_PREFIX_BYTES;
        if encoded_len > MAX_IPC_FRAME_BYTES {
            warn!(
                parent: &span,
                "[Ipc]: refusing to send oversized message ({} bytes; maximum is {} bytes)",
                encoded_len,
                MAX_IPC_FRAME_BYTES
            );
            continue;
        }

        let encoded_len = encoded_len as u32;
        frame[..IPC_LENGTH_PREFIX_BYTES].copy_from_slice(&encoded_len.to_be_bytes());
        trace!(parent: &span, "[Ipc] sending binary frame ({encoded_len} bytes)");

        if let Err(err) = write.write_all(&frame).await {
            warn!(parent: &span, "[Ipc]: failed to write framed message: {err}");
            break;
        }

        if let Err(err) = write.flush().await {
            warn!(parent: &span, "[Ipc]: failed to flush message: {err}");
            break;
        }
    }

    // Close both bounded queues and both single-value slots together so every
    // sending API observes the same terminal state after an IPC write failure.
    queues.reliable.close();
    queues.realtime.close();
    queues.latest.close();
    queues.low_priority.close();
}

async fn next_ipc_message<Message>(queues: &mut IpcQueues<Message>) -> Option<Message> {
    loop {
        // Explicit polling plus a biased select makes ordering deterministic:
        // reliable control first, short-lived realtime data second, the latest
        // coalesced snapshot third, and queued video last.
        if let Ok(message) = queues.reliable.try_recv() {
            return Some(message);
        }
        if let Ok(message) = queues.realtime.try_recv() {
            return Some(message);
        }
        if let Some(message) = queues.latest.take() {
            return Some(message);
        }
        if let Some(message) = queues.low_priority.take() {
            return Some(message);
        }

        let reliable_closed = queues.reliable.is_closed();
        let realtime_closed = queues.realtime.is_closed();
        if reliable_closed && realtime_closed {
            return None;
        }

        tokio::select! {
            biased;
            message = queues.reliable.recv(), if !reliable_closed => {
                if let Some(message) = message {
                    return Some(message);
                }
            }
            message = queues.realtime.recv(), if !realtime_closed => {
                if let Some(message) = message {
                    return Some(message);
                }
            }
            _ = queues.latest.notify.notified() => {
                // Loop back so reliable and realtime messages retain priority.
            }
            _ = queues.low_priority.notify.notified() => {
                // Loop back through the priority checks. A reliable message may
                // have arrived at the same time as this notification.
            }
        }
    }
}

#[derive(Debug)]
pub struct IpcSender<Message> {
    reliable_sender: Sender<Message>,
    realtime_sender: Sender<Message>,
    latest: Arc<PendingSlot<Message>>,
    low_priority: Arc<PendingSlot<Message>>,
    span: Span,
}

impl<Message> Clone for IpcSender<Message> {
    fn clone(&self) -> Self {
        Self {
            reliable_sender: self.reliable_sender.clone(),
            realtime_sender: self.realtime_sender.clone(),
            latest: self.latest.clone(),
            low_priority: self.low_priority.clone(),
            span: self.span.clone(),
        }
    }
}

impl<Message> IpcSender<Message>
where
    Message: Serialize + Send + 'static,
{
    /// Enqueue a reliable high-priority message. Backpressure is applied rather
    /// than dropping control or lifecycle state.
    pub async fn send(&self, message: Message) {
        if self.send_checked(message).await.is_err() {
            warn!(parent: &self.span, "failed to send message");
        }
    }

    /// Enqueue a reliable message while preserving closure information for
    /// latency-sensitive callers that need to terminate their own loops.
    pub async fn send_checked(&self, message: Message) -> Result<(), SendError<Message>> {
        self.reliable_sender.send(message).await
    }

    pub fn blocking_send(&self, message: Message) {
        if self.reliable_sender.blocking_send(message).is_err() {
            warn!(parent: &self.span, "failed to send message");
        }
    }

    /// Attempt to enqueue a reliable high-priority message without waiting.
    pub fn try_send(&self, message: Message) -> Result<(), TrySendError<Message>> {
        self.reliable_sender.try_send(message)
    }

    /// Attempt to enqueue short-lived realtime data such as audio. This queue
    /// is separate from video and drains ahead of it, but remains bounded.
    pub fn try_send_realtime(&self, message: Message) -> Result<(), TrySendError<Message>> {
        self.realtime_sender.try_send(message)
    }

    /// Publish replaceable state such as cumulative network feedback. Exactly
    /// one unsent value is retained across the IPC boundary; a newer publish
    /// atomically replaces an older one instead of replaying a stale backlog
    /// after the child process catches up.
    pub fn try_send_latest(&self, message: Message) -> Result<(), TrySendError<Message>> {
        if self.latest.writer_closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(message));
        }

        let mut slot = self
            .latest
            .message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.latest.writer_closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(message));
        }
        *slot = Some(message);
        drop(slot);
        self.latest.notify.notify_one();
        Ok(())
    }

    /// Attempt to enqueue disposable bulk data such as an encoded video frame.
    /// There is exactly one low-priority slot, so video can never crowd reliable
    /// or realtime queues. Returning Full lets the video sender enter GOP-safe
    /// IDR recovery instead of silently replacing a dependent frame.
    pub fn try_send_low_priority(&self, message: Message) -> Result<(), TrySendError<Message>> {
        if self.low_priority.writer_closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(message));
        }

        let mut slot = self
            .low_priority
            .message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.low_priority.writer_closed.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(message));
        }
        if slot.is_some() {
            return Err(TrySendError::Full(message));
        }
        *slot = Some(message);
        drop(slot);
        self.low_priority.notify.notify_one();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.reliable_sender.is_closed()
            || self.latest.writer_closed.load(Ordering::Acquire)
            || self.low_priority.writer_closed.load(Ordering::Acquire)
    }
}

pub struct IpcReceiver<Message> {
    errored: bool,
    read: Box<dyn AsyncRead + Send + Unpin>,
    encoded: Vec<u8>,
    phantom: PhantomData<Message>,
    span: Span,
}

impl<Message> IpcReceiver<Message>
where
    Message: DeserializeOwned,
{
    pub async fn recv(&mut self) -> Option<Message> {
        if self.errored {
            return None;
        }

        let mut prefix = [0_u8; IPC_LENGTH_PREFIX_BYTES];
        match self.read.read_exact(&mut prefix[..1]).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(err) => {
                self.errored = true;
                warn!(parent: &self.span, "[Ipc]: failed to read message length: {err}");
                return None;
            }
        }

        if let Err(err) = self.read.read_exact(&mut prefix[1..]).await {
            self.errored = true;
            warn!(parent: &self.span, "[Ipc]: truncated message length prefix: {err}");
            return None;
        }

        let encoded_len = u32::from_be_bytes(prefix) as usize;
        if encoded_len > MAX_IPC_FRAME_BYTES {
            self.errored = true;
            warn!(
                parent: &self.span,
                "[Ipc]: rejected oversized message ({encoded_len} bytes; maximum is {MAX_IPC_FRAME_BYTES} bytes)"
            );
            return None;
        }

        self.encoded.resize(encoded_len, 0);
        if let Err(err) = self.read.read_exact(&mut self.encoded).await {
            self.errored = true;
            warn!(
                parent: &self.span,
                "[Ipc]: truncated message body (expected {encoded_len} bytes): {err}"
            );
            return None;
        }

        trace!(parent: &self.span, "[Ipc] received binary frame ({encoded_len} bytes)");

        match bincode::serde::decode_from_slice::<Message, _>(
            &self.encoded,
            bincode::config::standard(),
        ) {
            Ok((value, consumed)) if consumed == encoded_len => Some(value),
            Ok((_, consumed)) => {
                self.errored = true;
                warn!(
                    parent: &self.span,
                    "[Ipc]: decoded only {consumed} of {encoded_len} message bytes"
                );
                None
            }
            Err(err) => {
                self.errored = true;
                warn!(parent: &self.span, "[Ipc]: failed to decode message: {err}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum PriorityMessage {
        Control(u8),
        Audio(u8),
        Feedback(u8),
        Video(u8),
    }

    fn encode_frame<Message: Serialize>(message: &Message) -> Vec<u8> {
        let encoded = bincode::serde::encode_to_vec(message, bincode::config::standard())
            .expect("test message should encode");
        let mut frame = Vec::with_capacity(IPC_LENGTH_PREFIX_BYTES + encoded.len());
        frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encoded);
        frame
    }

    fn receiver<Message: DeserializeOwned>(bytes: Vec<u8>) -> IpcReceiver<Message> {
        IpcReceiver {
            errored: false,
            read: create_reader(Cursor::new(bytes)),
            encoded: Vec::new(),
            phantom: PhantomData,
            span: Span::none(),
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
    }

    #[test]
    fn binary_bytes_round_trip() {
        let expected = Bytes::from_static(&[0, 1, 2, 3, 0xff, 0, 0x80]);
        let frame = encode_frame(&StreamerIpcMessage::WebSocketTransport(expected.clone()));
        let mut receiver = receiver::<StreamerIpcMessage>(frame);

        let received = runtime().block_on(receiver.recv());
        match received {
            Some(StreamerIpcMessage::WebSocketTransport(actual)) => {
                assert_eq!(actual, expected);
            }
            _ => panic!("unexpected decoded IPC message"),
        }
    }

    #[test]
    fn network_feedback_round_trips_through_binary_ipc() {
        let expected = NetworkFeedback {
            rtt_ms: 47,
            sent_packets: 10_000,
            lost_packets: 23,
            congestion_events: 4,
            admission_drops: 7,
            video_write_timeouts: 2,
            recovery_requests: 3,
        };
        let frame = encode_frame(&ServerIpcMessage::NetworkFeedback(expected));
        let mut receiver = receiver::<ServerIpcMessage>(frame);

        match runtime().block_on(receiver.recv()) {
            Some(ServerIpcMessage::NetworkFeedback(actual)) => assert_eq!(actual, expected),
            _ => panic!("unexpected decoded IPC message"),
        }
    }

    #[test]
    fn oversized_frame_is_rejected_without_allocating_body() {
        let oversized_len = (MAX_IPC_FRAME_BYTES as u32 + 1).to_be_bytes();
        let mut receiver = receiver::<StreamerIpcMessage>(oversized_len.to_vec());

        assert!(runtime().block_on(receiver.recv()).is_none());
        assert!(receiver.errored);
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let mut frame = encode_frame(&StreamerIpcMessage::Stop);
        frame.pop();
        let mut receiver = receiver::<StreamerIpcMessage>(frame);

        assert!(runtime().block_on(receiver.recv()).is_none());
        assert!(receiver.errored);
    }

    #[test]
    fn reliable_and_realtime_queues_are_independently_bounded() {
        let (sender, _queues) = priority_channel(Span::none());

        for index in 0..IPC_RELIABLE_QUEUE_CAPACITY {
            sender
                .try_send(PriorityMessage::Control(index as u8))
                .expect("reliable queue should have capacity");
        }
        assert!(matches!(
            sender.try_send(PriorityMessage::Control(0xff)),
            Err(TrySendError::Full(PriorityMessage::Control(0xff)))
        ));

        // A full reliable queue does not consume the separately bounded audio
        // capacity (and vice versa).
        for index in 0..IPC_REALTIME_QUEUE_CAPACITY {
            sender
                .try_send_realtime(PriorityMessage::Audio(index as u8))
                .expect("realtime queue should have independent capacity");
        }
        assert!(matches!(
            sender.try_send_realtime(PriorityMessage::Audio(0xff)),
            Err(TrySendError::Full(PriorityMessage::Audio(0xff)))
        ));
    }

    #[test]
    fn priority_order_preserves_control_realtime_latest_and_video_lanes() {
        let (sender, mut queues) = priority_channel(Span::none());

        sender
            .try_send_low_priority(PriorityMessage::Video(1))
            .expect("first video should fit");
        assert!(matches!(
            sender.try_send_low_priority(PriorityMessage::Video(2)),
            Err(TrySendError::Full(PriorityMessage::Video(2)))
        ));
        sender
            .try_send_realtime(PriorityMessage::Audio(3))
            .expect("audio should fit independently of video");
        sender
            .try_send_latest(PriorityMessage::Feedback(4))
            .expect("latest-value feedback should fit independently of media");
        sender
            .try_send(PriorityMessage::Control(5))
            .expect("control should fit independently of media");

        runtime().block_on(async {
            assert_eq!(
                next_ipc_message(&mut queues).await,
                Some(PriorityMessage::Control(5))
            );
            assert_eq!(
                next_ipc_message(&mut queues).await,
                Some(PriorityMessage::Audio(3))
            );
            assert_eq!(
                next_ipc_message(&mut queues).await,
                Some(PriorityMessage::Feedback(4))
            );
            assert_eq!(
                next_ipc_message(&mut queues).await,
                Some(PriorityMessage::Video(1))
            );
        });
    }

    #[test]
    fn priority_lanes_share_one_binary_framed_writer() {
        let (sender, queues) = priority_channel(Span::none());
        sender
            .try_send_low_priority(PriorityMessage::Video(1))
            .expect("video should fit");
        sender
            .try_send_realtime(PriorityMessage::Audio(2))
            .expect("audio should fit");
        sender
            .try_send_latest(PriorityMessage::Feedback(3))
            .expect("feedback should fit");
        sender
            .try_send(PriorityMessage::Control(4))
            .expect("control should fit");
        drop(sender);

        runtime().block_on(async move {
            let (read, write) = tokio::io::duplex(1_024);
            let writer = tokio::spawn(ipc_sender(Span::none(), write, queues));
            let mut receiver = IpcReceiver::<PriorityMessage> {
                errored: false,
                read: create_reader(read),
                encoded: Vec::new(),
                phantom: PhantomData,
                span: Span::none(),
            };

            assert_eq!(receiver.recv().await, Some(PriorityMessage::Control(4)));
            assert_eq!(receiver.recv().await, Some(PriorityMessage::Audio(2)));
            assert_eq!(receiver.recv().await, Some(PriorityMessage::Feedback(3)));
            assert_eq!(receiver.recv().await, Some(PriorityMessage::Video(1)));
            assert!(receiver.recv().await.is_none());
            writer.await.expect("IPC writer task should finish cleanly");
        });
    }

    #[test]
    fn latest_value_is_coalesced_before_crossing_the_ipc_boundary() {
        let (sender, queues) = priority_channel(Span::none());
        for value in 1..=100 {
            sender
                .try_send_latest(PriorityMessage::Feedback(value))
                .expect("latest-value slot should replace without filling");
        }
        drop(sender);

        runtime().block_on(async move {
            let (read, write) = tokio::io::duplex(1_024);
            let writer = tokio::spawn(ipc_sender(Span::none(), write, queues));
            let mut receiver = IpcReceiver::<PriorityMessage> {
                errored: false,
                read: create_reader(read),
                encoded: Vec::new(),
                phantom: PhantomData,
                span: Span::none(),
            };

            assert_eq!(receiver.recv().await, Some(PriorityMessage::Feedback(100)));
            assert!(receiver.recv().await.is_none());
            writer.await.expect("IPC writer task should finish cleanly");
        });
    }

    #[test]
    fn low_priority_video_is_strictly_bounded_to_one_pending_frame() {
        let (sender, mut queues) = priority_channel(Span::none());

        sender
            .try_send_low_priority(PriorityMessage::Video(7))
            .expect("first video should fit");
        for index in 0..1_000_u16 {
            assert!(matches!(
                sender.try_send_low_priority(PriorityMessage::Video((index % 256) as u8)),
                Err(TrySendError::Full(_))
            ));
        }

        assert_eq!(
            runtime().block_on(next_ipc_message(&mut queues)),
            Some(PriorityMessage::Video(7))
        );
        assert!(queues.low_priority.take().is_none());
        sender
            .try_send_low_priority(PriorityMessage::Video(8))
            .expect("slot should be reusable after it drains");
    }

    #[test]
    fn closure_rejects_low_priority_insertion_and_clears_pending_video() {
        let (sender, queues) = priority_channel(Span::none());
        sender
            .try_send_low_priority(PriorityMessage::Video(1))
            .expect("video should initially fit");

        queues.low_priority.close();

        assert!(queues.low_priority.take().is_none());
        assert!(matches!(
            sender.try_send_low_priority(PriorityMessage::Video(2)),
            Err(TrySendError::Closed(PriorityMessage::Video(2)))
        ));
    }

    #[test]
    fn closure_rejects_latest_value_and_clears_pending_feedback() {
        let (sender, queues) = priority_channel(Span::none());
        sender
            .try_send_latest(PriorityMessage::Feedback(1))
            .expect("feedback should initially fit");

        queues.latest.close();

        assert!(queues.latest.take().is_none());
        assert!(matches!(
            sender.try_send_latest(PriorityMessage::Feedback(2)),
            Err(TrySendError::Closed(PriorityMessage::Feedback(2)))
        ));
    }

    #[test]
    fn checked_reliable_send_reports_writer_closure() {
        let (sender, mut queues) = priority_channel(Span::none());
        queues.reliable.close();

        assert!(
            runtime()
                .block_on(sender.send_checked(PriorityMessage::Control(1)))
                .is_err()
        );
    }
}
