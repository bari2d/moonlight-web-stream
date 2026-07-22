use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use actix_codec::Encoder;
use actix_http::{
    body::{BodyStream, BoxBody},
    ws::{CloseReason, Codec, Message},
};
use actix_web::{
    Error, HttpRequest, HttpResponse,
    web::{Bytes, BytesMut, Payload},
};
use actix_ws::{Closed, MessageStream};
use common::api_bindings::TransportChannelId;
use futures::Stream;

const RELIABLE_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinarySendOutcome {
    Enqueued,
    Dropped,
    NeedIdr,
}

/// The low-latency outbound half of an Actix WebSocket.
///
/// Reliable control messages use a bounded FIFO. Audio and video never enter
/// that queue: each has one replaceable slot, so an unpolled HTTP body cannot
/// accumulate seconds of stale media in a hidden channel.
pub struct LowLatencySession {
    inner: Arc<Shared>,
}

impl Clone for LowLatencySession {
    fn clone(&self) -> Self {
        self.inner.session_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for LowLatencySession {
    fn drop(&mut self) {
        if self.inner.session_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.abort_if_open();
        }
    }
}

impl LowLatencySession {
    pub fn text(&self, message: impl Into<String>) -> Result<(), Closed> {
        self.inner
            .enqueue_reliable(Message::Text(message.into().into()))
    }

    pub fn pong(&self, message: &[u8]) -> Result<(), Closed> {
        self.inner
            .enqueue_reliable(Message::Pong(Bytes::copy_from_slice(message)))
    }

    pub fn binary(&self, message: impl Into<Bytes>) -> Result<BinarySendOutcome, Closed> {
        let message = message.into();
        match message.first().copied() {
            Some(TransportChannelId::HOST_AUDIO) => self.inner.enqueue_audio(message),
            Some(TransportChannelId::HOST_VIDEO) => self.inner.enqueue_video(message),
            _ => {
                self.inner.enqueue_reliable(Message::Binary(message))?;
                Ok(BinarySendOutcome::Enqueued)
            }
        }
    }

    /// Discards media that was queued for the WebSocket before WebTransport
    /// was selected. Reliable JSON/lifecycle messages remain in order.
    pub fn clear_media(&self) {
        self.inner.clear_media();
    }

    /// Stops accepting new work, discards media, drains the bounded reliable
    /// FIFO, then sends Close. This preserves final error/lifecycle messages.
    pub fn close(&self, reason: Option<CloseReason>) -> Result<(), Closed> {
        self.inner.close(reason)
    }

    /// Sends Close ahead of every queued item. Use for overload or transport
    /// failure where ending the old session is more important than final logs.
    pub fn close_now(&self, reason: Option<CloseReason>) -> Result<(), Closed> {
        self.inner.close_now(reason)
    }

    #[cfg(test)]
    fn reliable_len(&self) -> usize {
        self.inner.with_state(|state| state.reliable.len())
    }
}

/// Performs the stock Actix handshake and keeps its public inbound decoder,
/// while replacing the stock 32-message outbound channel/body entirely.
pub fn handle(
    request: &HttpRequest,
    payload: Payload,
) -> Result<(HttpResponse, LowLatencySession, MessageStream), Error> {
    let (response, stock_session, inbound) = actix_ws::handle(request, payload)?;
    drop(stock_session);

    let (session, outbound) = pair();
    let response = response
        .map_body(|_head, stock_body| BodyStream::new(outbound.with_guard(stock_body)))
        .map_into_boxed_body();

    Ok((response, session, inbound))
}

fn pair() -> (LowLatencySession, LowLatencyBody) {
    let inner = Arc::new(Shared {
        state: Mutex::new(OutboundState::default()),
        session_handles: AtomicUsize::new(1),
    });
    (
        LowLatencySession {
            inner: inner.clone(),
        },
        LowLatencyBody {
            inner,
            codec: Codec::new(),
            _stock_body_guard: None,
        },
    )
}

struct Shared {
    state: Mutex<OutboundState>,
    session_handles: AtomicUsize,
}

impl Shared {
    fn with_state<T>(&self, callback: impl FnOnce(&mut OutboundState) -> T) -> T {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        callback(&mut state)
    }

    fn enqueue_reliable(&self, message: Message) -> Result<(), Closed> {
        let (result, wake) = self.with_state(|state| {
            if !state.is_open() {
                return (Err(Closed), None);
            }
            if state.reliable.len() >= RELIABLE_QUEUE_CAPACITY {
                state.abort();
                return (Err(Closed), state.waker.take());
            }
            state.reliable.push_back(message);
            (Ok(()), state.waker.take())
        });
        wake_once(wake);
        result
    }

    fn enqueue_audio(&self, message: Bytes) -> Result<BinarySendOutcome, Closed> {
        let (result, wake) = self.with_state(|state| {
            if !state.is_open() {
                return (Err(Closed), None);
            }
            state.audio = Some(Message::Binary(message));
            (Ok(BinarySendOutcome::Enqueued), state.waker.take())
        });
        wake_once(wake);
        result
    }

    fn enqueue_video(&self, message: Bytes) -> Result<BinarySendOutcome, Closed> {
        let (result, wake) = self.with_state(|state| {
            if !state.is_open() {
                return (Err(Closed), None);
            }

            let is_idr = message.get(1) == Some(&1);
            if is_idr {
                state.video = Some(Message::Binary(message));
                state.video_recovering = false;
                state.idr_request_pending = false;
                return (Ok(BinarySendOutcome::Enqueued), state.waker.take());
            }

            if state.video_recovering {
                return (Ok(state.request_idr()), None);
            }

            if state.video.is_none() {
                state.video = Some(Message::Binary(message));
                return (Ok(BinarySendOutcome::Enqueued), state.waker.take());
            }

            let queued_is_idr = matches!(
                state.video.as_ref(),
                Some(Message::Binary(queued)) if queued.get(1) == Some(&1)
            );
            if queued_is_idr {
                // Protect the reset point already waiting for the socket, but
                // dropping its first dependent delta makes that GOP unusable.
                // Keep the queued IDR, latch recovery, and ask for exactly one
                // replacement IDR.
                state.video_recovering = true;
                return (Ok(state.request_idr()), None);
            }

            // Keep the already queued decodable frame. Dropping any dependent
            // delta means all later deltas are unsafe until a fresh IDR.
            state.video_recovering = true;
            (Ok(state.request_idr()), None)
        });
        wake_once(wake);
        result
    }

    fn clear_media(&self) {
        self.with_state(OutboundState::clear_media);
    }

    fn close(&self, reason: Option<CloseReason>) -> Result<(), Closed> {
        let (result, wake) = self.with_state(|state| {
            if !state.is_open() {
                return (Err(Closed), None);
            }
            state.clear_media();
            state.terminal = Terminal::GracefulClosePending(reason);
            (Ok(()), state.waker.take())
        });
        wake_once(wake);
        result
    }

    fn close_now(&self, reason: Option<CloseReason>) -> Result<(), Closed> {
        let (result, wake) = self.with_state(|state| {
            if !matches!(
                &state.terminal,
                Terminal::Open | Terminal::GracefulClosePending(_)
            ) {
                return (Err(Closed), None);
            }
            state.clear_all();
            state.terminal = Terminal::CloseNowPending(reason);
            (Ok(()), state.waker.take())
        });
        wake_once(wake);
        result
    }

    fn abort(&self) {
        let wake = self.with_state(|state| {
            state.abort();
            state.waker.take()
        });
        wake_once(wake);
    }

    fn abort_if_open(&self) {
        let wake = self.with_state(|state| {
            if state.is_open() {
                state.abort();
                state.waker.take()
            } else {
                None
            }
        });
        wake_once(wake);
    }
}

#[derive(Debug, Clone, Copy)]
enum MediaTurn {
    Audio,
    Video,
}

enum Terminal {
    Open,
    GracefulClosePending(Option<CloseReason>),
    CloseNowPending(Option<CloseReason>),
    EndAfterClose,
    Aborted,
    Finished,
}

struct OutboundState {
    reliable: VecDeque<Message>,
    audio: Option<Message>,
    video: Option<Message>,
    video_recovering: bool,
    idr_request_pending: bool,
    next_media: MediaTurn,
    terminal: Terminal,
    waker: Option<Waker>,
}

impl Default for OutboundState {
    fn default() -> Self {
        Self {
            reliable: VecDeque::with_capacity(RELIABLE_QUEUE_CAPACITY),
            audio: None,
            video: None,
            video_recovering: false,
            idr_request_pending: false,
            next_media: MediaTurn::Audio,
            terminal: Terminal::Open,
            waker: None,
        }
    }
}

impl OutboundState {
    fn is_open(&self) -> bool {
        matches!(self.terminal, Terminal::Open)
    }

    fn request_idr(&mut self) -> BinarySendOutcome {
        if self.idr_request_pending {
            BinarySendOutcome::Dropped
        } else {
            self.idr_request_pending = true;
            BinarySendOutcome::NeedIdr
        }
    }

    fn clear_media(&mut self) {
        self.audio = None;
        self.video = None;
        self.video_recovering = false;
        self.idr_request_pending = false;
    }

    fn clear_all(&mut self) {
        self.reliable.clear();
        self.clear_media();
    }

    fn abort(&mut self) {
        self.clear_all();
        self.terminal = Terminal::Aborted;
    }

    fn select(&mut self, waker: &Waker) -> Selection {
        match &self.terminal {
            Terminal::CloseNowPending(_) => {
                let Terminal::CloseNowPending(reason) =
                    std::mem::replace(&mut self.terminal, Terminal::EndAfterClose)
                else {
                    unreachable!();
                };
                return Selection::Message(Message::Close(reason));
            }
            Terminal::GracefulClosePending(_) if self.reliable.is_empty() => {
                let Terminal::GracefulClosePending(reason) =
                    std::mem::replace(&mut self.terminal, Terminal::EndAfterClose)
                else {
                    unreachable!();
                };
                return Selection::Message(Message::Close(reason));
            }
            Terminal::EndAfterClose | Terminal::Aborted | Terminal::Finished => {
                self.terminal = Terminal::Finished;
                return Selection::End;
            }
            Terminal::Open | Terminal::GracefulClosePending(_) => {}
        }

        if let Some(message) = self.reliable.pop_front() {
            return Selection::Message(message);
        }

        let message = match self.next_media {
            MediaTurn::Audio => self
                .audio
                .take()
                .map(|message| (message, MediaTurn::Video))
                .or_else(|| self.video.take().map(|message| (message, MediaTurn::Audio))),
            MediaTurn::Video => self
                .video
                .take()
                .map(|message| (message, MediaTurn::Audio))
                .or_else(|| self.audio.take().map(|message| (message, MediaTurn::Video))),
        };
        if let Some((message, next_media)) = message {
            self.next_media = next_media;
            return Selection::Message(message);
        }

        self.waker = Some(waker.clone());
        Selection::Pending
    }
}

enum Selection {
    Message(Message),
    End,
    Pending,
}

struct LowLatencyBody {
    inner: Arc<Shared>,
    codec: Codec,
    // actix-ws 0.4+ ties inbound lifetime to its stock response body. Retain
    // that body unpolled so replacing outbound encoding cannot close inbound.
    _stock_body_guard: Option<BoxBody>,
}

impl LowLatencyBody {
    fn with_guard(mut self, stock_body: BoxBody) -> Self {
        self._stock_body_guard = Some(stock_body);
        self
    }
}

impl Stream for LowLatencyBody {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selection = self.inner.with_state(|state| state.select(context.waker()));
        let message = match selection {
            Selection::Message(message) => message,
            Selection::End => return Poll::Ready(None),
            Selection::Pending => return Poll::Pending,
        };

        // A fresh output item contains exactly one encoded WebSocket message.
        // Actix therefore cannot hide a batch of stale frames in one body poll.
        let mut encoded = BytesMut::new();
        if let Err(error) = self.codec.encode(message, &mut encoded) {
            self.inner.abort();
            return Poll::Ready(Some(Err(error.into())));
        }
        Poll::Ready(Some(Ok(encoded.freeze())))
    }
}

impl Drop for LowLatencyBody {
    fn drop(&mut self) {
        // If the HTTP connection disappears, every producer observes Closed on
        // its next send instead of filling an orphaned queue.
        self.inner.abort();
    }
}

fn wake_once(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_codec::Decoder;
    use actix_http::ws::Frame;
    use actix_web::FromRequest;
    use futures::task::noop_waker_ref;

    fn poll(body: &mut Pin<Box<LowLatencyBody>>) -> Poll<Option<Result<Bytes, Error>>> {
        let mut context = Context::from_waker(noop_waker_ref());
        body.as_mut().poll_next(&mut context)
    }

    fn next(body: &mut Pin<Box<LowLatencyBody>>) -> Bytes {
        match poll(body) {
            Poll::Ready(Some(Ok(bytes))) => bytes,
            Poll::Ready(Some(Err(error))) => panic!("body error: {error}"),
            Poll::Ready(None) => panic!("body ended"),
            Poll::Pending => panic!("body was pending"),
        }
    }

    fn decode(encoded: Bytes) -> Frame {
        let mut codec = Codec::new().client_mode();
        let mut encoded = BytesMut::from(encoded.as_ref());
        let frame = codec.decode(&mut encoded).unwrap().unwrap();
        assert!(codec.decode(&mut encoded).unwrap().is_none());
        assert!(encoded.is_empty());
        frame
    }

    fn binary(frame: Frame) -> Bytes {
        let Frame::Binary(bytes) = frame else {
            panic!("expected binary frame");
        };
        bytes
    }

    #[test]
    fn media_slots_are_constant_size_and_audio_keeps_only_latest() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        for value in 0..10_u8 {
            assert_eq!(
                session
                    .binary(Bytes::from(vec![TransportChannelId::HOST_AUDIO, value]))
                    .unwrap(),
                BinarySendOutcome::Enqueued
            );
        }
        assert_eq!(session.reliable_len(), 0);
        assert_eq!(
            binary(decode(next(&mut body))),
            Bytes::from(vec![TransportChannelId::HOST_AUDIO, 9])
        );
        assert!(poll(&mut body).is_pending());
    }

    #[test]
    fn video_overflow_protects_queued_frame_and_recovers_only_at_idr() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        let delta = |value| Bytes::from(vec![TransportChannelId::HOST_VIDEO, 0, value]);
        let idr = |value| Bytes::from(vec![TransportChannelId::HOST_VIDEO, 1, value]);

        assert_eq!(
            session.binary(delta(1)).unwrap(),
            BinarySendOutcome::Enqueued
        );
        assert_eq!(
            session.binary(delta(2)).unwrap(),
            BinarySendOutcome::NeedIdr
        );
        assert_eq!(
            session.binary(delta(3)).unwrap(),
            BinarySendOutcome::Dropped
        );
        assert_eq!(binary(decode(next(&mut body))), delta(1));
        assert_eq!(
            session.binary(delta(4)).unwrap(),
            BinarySendOutcome::Dropped
        );
        assert_eq!(session.binary(idr(5)).unwrap(), BinarySendOutcome::Enqueued);
        assert_eq!(binary(decode(next(&mut body))), idr(5));
    }

    #[test]
    fn dropping_delta_behind_queued_idr_latches_recovery_until_fresh_idr() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        let idr = |value| Bytes::from(vec![TransportChannelId::HOST_VIDEO, 1, value]);
        let delta = |value| Bytes::from(vec![TransportChannelId::HOST_VIDEO, 0, value]);

        assert_eq!(session.binary(idr(1)).unwrap(), BinarySendOutcome::Enqueued);
        assert_eq!(
            session.binary(delta(2)).unwrap(),
            BinarySendOutcome::NeedIdr
        );
        assert_eq!(binary(decode(next(&mut body))), idr(1));
        assert_eq!(
            session.binary(delta(3)).unwrap(),
            BinarySendOutcome::Dropped
        );
        assert_eq!(session.binary(idr(4)).unwrap(), BinarySendOutcome::Enqueued);
        assert_eq!(binary(decode(next(&mut body))), idr(4));
        assert_eq!(
            session.binary(delta(5)).unwrap(),
            BinarySendOutcome::Enqueued
        );
        assert_eq!(binary(decode(next(&mut body))), delta(5));
    }

    #[test]
    fn reliable_control_has_priority_and_media_is_fair() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        session
            .binary(Bytes::from_static(&[TransportChannelId::HOST_AUDIO, 7]))
            .unwrap();
        session
            .binary(Bytes::from_static(&[TransportChannelId::HOST_VIDEO, 1, 8]))
            .unwrap();
        session.text("control").unwrap();

        assert!(matches!(
            decode(next(&mut body)),
            Frame::Text(text) if text == Bytes::from_static(b"control")
        ));
        assert_eq!(
            binary(decode(next(&mut body)))[0],
            TransportChannelId::HOST_AUDIO
        );
        assert_eq!(
            binary(decode(next(&mut body)))[0],
            TransportChannelId::HOST_VIDEO
        );
    }

    #[test]
    fn close_is_immediate_and_discards_queued_work() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        session.text("stale").unwrap();
        session
            .binary(Bytes::from_static(&[TransportChannelId::HOST_AUDIO, 1]))
            .unwrap();
        session.close_now(None).unwrap();

        assert!(matches!(decode(next(&mut body)), Frame::Close(None)));
        assert!(matches!(poll(&mut body), Poll::Ready(None)));
    }

    #[test]
    fn graceful_close_preserves_reliable_fifo_but_discards_media() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        session.text("final reason").unwrap();
        session
            .binary(Bytes::from_static(&[TransportChannelId::HOST_AUDIO, 1]))
            .unwrap();
        session.close(None).unwrap();

        assert!(matches!(
            decode(next(&mut body)),
            Frame::Text(text) if text == Bytes::from_static(b"final reason")
        ));
        assert!(matches!(decode(next(&mut body)), Frame::Close(None)));
        assert!(matches!(poll(&mut body), Poll::Ready(None)));
    }

    #[test]
    fn each_body_item_contains_exactly_one_message() {
        let (session, body) = pair();
        let mut body = Box::pin(body);
        session.text("first").unwrap();
        session.text("second").unwrap();

        assert!(matches!(
            decode(next(&mut body)),
            Frame::Text(text) if text == Bytes::from_static(b"first")
        ));
        assert!(matches!(
            decode(next(&mut body)),
            Frame::Text(text) if text == Bytes::from_static(b"second")
        ));
    }

    #[test]
    fn reliable_overflow_fails_the_entire_session() {
        let (session, mut body) = pair();
        for index in 0..RELIABLE_QUEUE_CAPACITY {
            session.text(index.to_string()).unwrap();
        }
        assert!(session.text("overflow").is_err());
        assert!(session.text("after overflow").is_err());
        assert!(matches!(
            Pin::new(&mut body).poll_next(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn dropping_response_body_closes_all_senders() {
        let (session, body) = pair();
        drop(body);
        assert!(session.text("orphaned").is_err());
    }

    #[tokio::test]
    async fn replacing_outbound_body_keeps_inbound_decoder_alive() {
        let request = actix_web::test::TestRequest::get()
            .insert_header(("connection", "upgrade"))
            .insert_header(("upgrade", "websocket"))
            .insert_header(("sec-websocket-version", "13"))
            .insert_header(("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="))
            .to_http_request();

        let mut client_codec = Codec::new().client_mode();
        let mut wire = BytesMut::new();
        client_codec
            .encode(Message::Text("hello".into()), &mut wire)
            .unwrap();
        let mut dev_payload = actix_web::dev::Payload::from(wire.freeze());
        let payload = Payload::from_request(&request, &mut dev_payload)
            .await
            .unwrap();

        let (response, _session, mut inbound) = handle(&request, payload).unwrap();
        assert!(matches!(
            inbound.recv().await,
            Some(Ok(actix_ws::Message::Text(text))) if text == "hello"
        ));
        drop(response);
    }
}
