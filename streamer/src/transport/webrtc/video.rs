use std::{
    io::Cursor,
    ops::Range,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::{Bytes, BytesMut};
use common::{
    api_bindings::{LogMessageType, StreamServerMessage},
    ipc::StreamerIpcMessage,
};
use moonlight_common::stream::video::{
    DecodeResult, FrameType, VideoDecodeUnit, VideoFormat, VideoFormats, VideoSetup,
};
use tokio::runtime::Handle;
use tracing::{debug, error, info, trace, warn};
use webrtc::{
    api::media_engine::{MIME_TYPE_AV1, MIME_TYPE_H264, MIME_TYPE_HEVC, MediaEngine},
    peer_connection::RTCPeerConnection,
    rtcp::payload_feedbacks::{
        picture_loss_indication::PictureLossIndication,
        receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate,
    },
    rtp::{
        codecs::{av1::Av1Payloader, h265::RTP_OUTBOUND_MTU},
        header::Header,
        packet::Packet,
        packetizer::Payloader,
    },
    rtp_transceiver::{
        RTCPFeedback,
        rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType},
    },
    track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
};

use crate::transport::{
    TransportEvent,
    webrtc::{
        WebRtcInner,
        sender::{SequencedTrackLocalStaticRTP, TrackLocalSender},
        video::{
            h264::{payloader::H264Payloader, reader::H264Reader},
            h265::{payloader::H265Payloader, reader::H265Reader},
        },
    },
};

mod annexb;
mod h264;
mod h265;

// Av1 specification:
// - https://aomediacodec.github.io/av1-rtp-spec/v1.0.0.html

enum VideoCodec {
    H264 {
        nal_reader: H264Reader<Cursor<Vec<u8>>>,
        payloader: H264Payloader,
    },
    H265 {
        nal_reader: H265Reader<Cursor<Vec<u8>>>,
        payloader: H265Payloader,
    },
    Av1 {
        payloader: Av1Payloader,
    },
}

pub struct WebRtcVideo {
    supported_video_formats: VideoFormats,
    sender: TrackLocalSender<SequencedTrackLocalStaticRTP>,
    needs_idr: Arc<AtomicBool>,
    awaiting_idr: bool,
    clock_rate: u32,
    codec: Option<VideoCodec>,
    samples: Vec<BytesMut>,
}

impl WebRtcVideo {
    pub fn new(runtime: Handle, peer: Weak<RTCPeerConnection>, frame_queue_size: usize) -> Self {
        Self {
            clock_rate: 0,
            needs_idr: Default::default(),
            awaiting_idr: false,
            sender: TrackLocalSender::new(runtime, peer, frame_queue_size),
            codec: None,
            supported_video_formats: VideoFormats::empty(),
            samples: Default::default(),
        }
    }

    pub async fn set_codecs(&mut self, supported_codecs: VideoFormats) {
        self.supported_video_formats = supported_codecs;
    }

    pub async fn setup(
        &mut self,
        inner: &Arc<WebRtcInner>,
        VideoSetup {
            format,
            width,
            height,
            redraw_rate,
        }: VideoSetup,
    ) -> bool {
        info!("[Stream] Stream setup: {width}x{height}x{redraw_rate} and {format:?}");

        if !format.contained_in(self.supported_video_formats) {
            let message = format!(
                "The host tried to setup a video stream with a non supported video format: {format:?}, supported formats: {}",
                self.supported_video_formats
            );

            error!("{}", message);

            if let Err(err) = inner
                .event_sender
                .send(TransportEvent::SendIpc(StreamerIpcMessage::WebSocket(
                    StreamServerMessage::DebugLog {
                        message,
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )))
                .await
            {
                warn!("Failed to send error to client: {err}");
            }

            return false;
        }

        let Some(codec) = video_format_to_codec(format) else {
            // This shouldn't happen
            error!("Failed to get video codec with format {:?}", format);
            return false;
        };

        let needs_idr = self.needs_idr.clone();
        if let Err(err) = self
            .sender
            .create_track(
                TrackLocalStaticRTP::new(
                    codec.capability.clone(),
                    "video".to_string(),
                    "moonlight".to_string(),
                )
                .into(),
                {
                    let needs_idr = needs_idr.clone();

                    move |packet| {
                        let packet = packet.as_any();

                        if packet.is::<PictureLossIndication>() {
                            needs_idr.store(true, Ordering::Release);
                        }
                        if let Some(_max_bitrate) =
                            packet.downcast_ref::<ReceiverEstimatedMaximumBitrate>()
                        {
                            // Moonlight doesn't support dynamic bitrate changing :(
                        }
                    }
                },
            )
            .await
        {
            let message = format!(
                "Failed to create video track with format {format:?} and codec \"{codec:?}\": {err:?}"
            );
            error!("{}", message);

            if let Err(err) = inner
                .event_sender
                .send(TransportEvent::SendIpc(StreamerIpcMessage::WebSocket(
                    StreamServerMessage::DebugLog {
                        message,
                        ty: Some(LogMessageType::FatalDescription),
                    },
                )))
                .await
            {
                warn!("Failed to send error to client: {err}");
            }
            return false;
        }

        self.clock_rate = codec.capability.clock_rate;

        self.codec = match format {
            // -- H264
            VideoFormat::H264 | VideoFormat::H264High8_444 => Some(VideoCodec::H264 {
                nal_reader: H264Reader::new(Cursor::new(Vec::new()), 0),
                payloader: Default::default(),
            }),
            // -- H265
            VideoFormat::H265
            | VideoFormat::H265Main10
            | VideoFormat::H265Rext8_444
            | VideoFormat::H265Rext10_444 => Some(VideoCodec::H265 {
                nal_reader: H265Reader::new(Cursor::new(Vec::new()), 0),
                payloader: Default::default(),
            }),
            // -- AV1
            VideoFormat::Av1Main8
            | VideoFormat::Av1Main10
            | VideoFormat::Av1High8_444
            | VideoFormat::Av1High10_444 => Some(VideoCodec::Av1 {
                payloader: Default::default(),
            }),
        };

        true
    }

    pub async fn send_decode_unit(&mut self, unit: &VideoDecodeUnit<&[u8]>) -> DecodeResult {
        let important = matches!(unit.frame_type, FrameType::Idr);

        if self.needs_idr.swap(false, Ordering::AcqRel) {
            self.awaiting_idr = true;
        }

        if self.awaiting_idr && !important {
            return DecodeResult::NeedIdr;
        }

        let timestamp = (unit.timestamp.as_nanos() * 90000 / 1_000_000_000) as u32;

        let mut full_frame = Vec::new();
        for buffer in &unit.buffers {
            full_frame.extend_from_slice(buffer.data);
        }

        let frame_queued = match &mut self.codec {
            // -- H264
            Some(VideoCodec::H264 {
                nal_reader,
                payloader,
            }) => {
                nal_reader.reset(Cursor::new(full_frame));

                let mut frame_complete = true;
                loop {
                    let nal = match nal_reader.next_nal() {
                        Ok(Some(nal)) => nal,
                        Ok(None) => break,
                        Err(err) => {
                            warn!("discarding incomplete h264 frame: {err:?}");
                            frame_complete = false;
                            break;
                        }
                    };

                    trace!(
                        target: "video::header",
                        nal_start_code = ?nal.start_code,
                        nal_header = ?nal.header,
                        "h264 header"
                    );
                    trace!(
                        target: "video::nalu",
                        nal_bytes = nal.full.len(),
                        "h264 nalu"
                    );

                    if nal.header.nal_unit_type == h264::NalUnitType::FillerData {
                        trace!(target: "video","Ignoring nal because it's filler data: {:?}", nal.header);
                        continue;
                    }

                    let data = trim_bytes_to_range(
                        nal.full,
                        nal.header_range.start..nal.payload_range.end,
                    );

                    self.samples.push(data);
                }

                if frame_complete {
                    send_single_frame(
                        &mut self.samples,
                        &mut self.sender,
                        payloader,
                        timestamp,
                        important,
                    )
                    .await
                } else {
                    self.samples.clear();
                    false
                }
            }
            // -- H265
            Some(VideoCodec::H265 {
                nal_reader,
                payloader,
            }) => {
                nal_reader.reset(Cursor::new(full_frame));

                let mut frame_complete = true;
                loop {
                    let nal = match nal_reader.next_nal() {
                        Ok(Some(nal)) => nal,
                        Ok(None) => break,
                        Err(err) => {
                            warn!("discarding incomplete h265 frame: {err:?}");
                            frame_complete = false;
                            break;
                        }
                    };

                    trace!(
                        target: "video::header",
                        nal_start_code = ?nal.start_code,
                        nal_header = ?nal.header,
                        "h265 header"
                    );
                    trace!(
                        target: "video::nalu",
                        nal_bytes = nal.full.len(),
                        "h265 nalu"
                    );

                    let data = trim_bytes_to_range(
                        nal.full,
                        nal.header_range.start..nal.payload_range.end,
                    );

                    self.samples.push(data);
                }

                if frame_complete {
                    send_single_frame(
                        &mut self.samples,
                        &mut self.sender,
                        payloader,
                        timestamp,
                        important,
                    )
                    .await
                } else {
                    self.samples.clear();
                    false
                }
            }
            // -- AV1
            Some(VideoCodec::Av1 { payloader }) => {
                self.samples.push(BytesMut::from(full_frame.as_slice()));

                send_single_frame(
                    &mut self.samples,
                    &mut self.sender,
                    payloader,
                    timestamp,
                    important,
                )
                .await
            }
            None => {
                warn!("Failed to send decode unit because of missing codec!");
                false
            }
        };

        let idr_requested = self.needs_idr.swap(false, Ordering::AcqRel);
        if !frame_queued || idr_requested {
            self.awaiting_idr = true;
            return DecodeResult::NeedIdr;
        }

        if important {
            self.awaiting_idr = false;
        }

        DecodeResult::Ok
    }
}

pub fn register_video_codecs(media_engine: &mut MediaEngine) -> Result<(), webrtc::Error> {
    for format in VideoFormat::all() {
        let Some(codec) = video_format_to_codec(format) else {
            continue;
        };
        debug!(
            "Registering Video Format {format:?}, Codec: {:?}",
            codec.capability
        );

        media_engine.register_codec(codec, RTPCodecType::Video)?;
    }

    Ok(())
}

async fn send_single_frame<P>(
    samples: &mut Vec<BytesMut>,
    sender: &mut TrackLocalSender<SequencedTrackLocalStaticRTP>,
    payloader: &mut P,
    timestamp: u32,
    important: bool,
) -> bool
where
    P: Payloader + Clone,
{
    if samples.is_empty() {
        warn!("discarding video frame with no codec samples");
        return false;
    }

    let frame = std::mem::take(samples);
    let mut staged_payloader = payloader.clone();
    let mut frame_samples = Vec::new();
    for sample in frame {
        if sample.is_empty() {
            warn!("discarding video frame containing an empty codec sample");
            return false;
        }

        let packets = match packetize(
            &mut staged_payloader,
            RTP_OUTBOUND_MTU,
            0, // is set in the write fn
            timestamp,
            &sample.freeze(),
            false,
        ) {
            Ok(value) => value,
            Err(err) => {
                warn!("discarding video frame after packetization failed: {err}");
                return false;
            }
        };

        frame_samples.extend(packets);
    }

    if frame_samples.is_empty() {
        warn!("discarding video frame with no RTP packets");
        return false;
    }

    // Some payloaders buffer codec headers and emit no packet for those input
    // samples. Mark the final packet only after the whole access unit has been
    // packetized so a trailing buffered header cannot leave the frame unmarked.
    if let Some(last_packet) = frame_samples.last_mut() {
        last_packet.header.marker = true;
    }

    if important {
        sender
            .replace_queued_samples(frame_samples, important)
            .await;
        *payloader = staged_payloader;
        return true;
    }

    if !sender.send_samples(frame_samples, important).await {
        sender.clear_queue(true).await;
        return false;
    }

    *payloader = staged_payloader;
    true
}

fn packetize(
    payloader: &mut impl Payloader,
    mtu: usize,
    sequence_number: u16,
    timestamp: u32,
    payload: &Bytes,
    end_has_marker: bool,
) -> Result<Vec<Packet>, anyhow::Error> {
    let payloads = payloader.payload(mtu - 12, payload)?;
    let payloads_len = payloads.len();
    let mut packets = Vec::with_capacity(payloads_len);
    for (i, payload) in payloads.into_iter().enumerate() {
        packets.push(Packet {
            header: Header {
                version: 2,
                padding: false,
                extension: false,
                marker: end_has_marker && i == payloads_len - 1,
                sequence_number,
                timestamp,
                payload_type: 0, // Value is handled when writing
                ssrc: 0,         // Value is handled when writing
                ..Default::default()
            },
            payload,
        });
    }

    Ok(packets)
}

fn video_format_to_codec(format: VideoFormat) -> Option<RTCRtpCodecParameters> {
    let rtcp_feedback = vec![
        RTCPFeedback {
            typ: "nack".to_string(),
            parameter: "".to_string(),
        },
        RTCPFeedback {
            typ: "nack".to_string(),
            parameter: "pli".to_string(),
        },
        RTCPFeedback {
            typ: "goog-remb".to_string(),
            parameter: "".to_string(),
        },
    ];

    match format {
        // -- H264 Constrained Baseline Profile
        VideoFormat::H264 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                        .to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 96,
            ..Default::default()
        }),
        // -- H264 High Profile
        VideoFormat::H264High8_444 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032"
                        .to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 97,
            ..Default::default()
        }),

        // -- H265 Main Profile
        VideoFormat::H265 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_HEVC.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 98,
            ..Default::default()
        }),
        // -- H265 Main10 Profile
        VideoFormat::H265Main10 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_HEVC.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "profile-id=2;tier-flag=0;level-id=93;tx-mode=SRST".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 99,
            ..Default::default()
        }),
        // -- H265 RExt 4:4:4 8-bit
        VideoFormat::H265Rext8_444 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_HEVC.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "profile-id=4;tier-flag=0;level-id=120;tx-mode=SRST".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 100,
            ..Default::default()
        }),
        // -- H265 RExt 4:4:4 10-bit
        VideoFormat::H265Rext10_444 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_HEVC.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "profile-id=5;tier-flag=0;level-id=93;tx-mode=SRST".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 101,
            ..Default::default()
        }),

        // -- Av1
        VideoFormat::Av1Main8 | VideoFormat::Av1Main10 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_AV1.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "profile=0".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 102,
            ..Default::default()
        }),
        VideoFormat::Av1High8_444 | VideoFormat::Av1High10_444 => Some(RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_AV1.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: "profile=1".to_owned(),
                rtcp_feedback: rtcp_feedback.clone(),
            },
            payload_type: 103,
            ..Default::default()
        }),
    }
}

fn trim_bytes_to_range(mut buf: BytesMut, range: Range<usize>) -> BytesMut {
    if range.start > 0 {
        let _ = buf.split_to(range.start);
    }

    if range.end - range.start < buf.len() {
        let _ = buf.split_off(range.end - range.start);
    }

    buf
}
