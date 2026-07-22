use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use common::api_bindings::{StatsHostProcessingLatency, StreamerStatsUpdate};
use log::{debug, error, warn};
use moonlight_common::stream::{
    c::bindings::EstimatedRttInfo,
    video::{
        DecodeResult, VideoCapabilities, VideoDecodeUnit, VideoDecoder, VideoFormats, VideoSetup,
    },
};

use crate::{StreamConnection, transport::OutboundPacket};

const UPSTREAM_TIMESTAMP_TICKS_PER_SECOND: u128 = 90_000;
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Undo the timestamp unit conversion in the pinned moonlight-common C adapter.
///
/// The C callback's `presentationTimeUs` is already measured in microseconds,
/// but the pinned adapter treats it as a 90 kHz timestamp when constructing a
/// `Duration`. Normalize it once at decoder ingress so every transport and
/// stats consumer sees the original presentation time. Remove this when the
/// pinned dependency fixes its C adapter.
fn normalize_pinned_c_timestamp(timestamp: Duration) -> Duration {
    let micros = timestamp
        .as_nanos()
        .checked_mul(UPSTREAM_TIMESTAMP_TICKS_PER_SECOND)
        .and_then(|scaled| scaled.checked_add(NANOS_PER_SECOND / 2))
        .map(|rounded| rounded / NANOS_PER_SECOND)
        .unwrap_or(u128::MAX)
        .min(u64::MAX as u128) as u64;

    Duration::from_micros(micros)
}

pub(crate) struct StreamVideoDecoder {
    pub(crate) stream: Weak<StreamConnection>,
    pub(crate) supported_formats: VideoFormats,
    pub(crate) stats: VideoStats,
}

impl VideoDecoder for StreamVideoDecoder {
    fn setup(&mut self, setup: VideoSetup) -> i32 {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to setup video because stream is deallocated");
            return -1;
        };

        {
            let mut stream_info = stream.stream_setup.blocking_lock();
            stream_info.video = Some(setup);
        }

        {
            stream.runtime.clone().block_on(async move {
                let sender = {
                    let sender = stream.transport_sender.lock().await;
                    sender.clone()
                };

                if let Some(sender) = sender {
                    sender.setup_video(setup).await
                } else {
                    error!("Failed to setup video because of missing transport!");
                    -1
                }
            })
        }
    }

    fn start(&mut self) {}
    fn stop(&mut self) {}

    fn submit_decode_unit(&mut self, unit: VideoDecodeUnit<&[u8]>) -> DecodeResult {
        let mut unit = unit;
        unit.timestamp = normalize_pinned_c_timestamp(unit.timestamp);

        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to send video decode unit because stream is deallocated");
            return DecodeResult::Ok;
        };

        let sender = {
            let sender = stream.transport_sender.blocking_lock();
            sender.clone()
        };

        let start = Instant::now();

        let result = stream.runtime.block_on(async {
            if let Some(sender) = sender.as_ref() {
                match sender.send_video_unit(unit.as_ref()).await {
                    Err(err) => {
                        warn!("Failed to send video decode unit: {err}");
                        DecodeResult::Ok
                    }
                    Ok(value) => value,
                }
            } else {
                debug!("Dropping video packet because of missing transport");

                DecodeResult::Ok
            }
        });

        let frame_processing_time = Instant::now() - start;
        self.stats.analyze(&stream, &unit, frame_processing_time);

        result
    }

    fn supported_formats(&self) -> VideoFormats {
        self.supported_formats
    }

    fn capabilities(&self) -> VideoCapabilities {
        VideoCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_pinned_c_timestamp;
    use std::time::Duration;

    fn pinned_c_timestamp(presentation_time_us: u64) -> Duration {
        Duration::from_nanos(presentation_time_us.saturating_mul(1_000_000_000) / 90_000)
    }

    #[test]
    fn normalizes_representative_presentation_timestamps() {
        for presentation_time_us in [0, 1, 16_667, 1_000_000, 3_600_000_000] {
            assert_eq!(
                normalize_pinned_c_timestamp(pinned_c_timestamp(presentation_time_us)),
                Duration::from_micros(presentation_time_us)
            );
        }
    }

    #[test]
    fn timestamp_normalization_saturates_instead_of_overflowing() {
        assert_eq!(
            normalize_pinned_c_timestamp(Duration::MAX),
            Duration::from_micros(u64::MAX)
        );
    }
}

#[derive(Debug, Default)]
pub(crate) struct VideoStats {
    last_send: Option<Instant>,
    min_host_processing_latency: Duration,
    max_host_processing_latency: Duration,
    total_host_processing_latency: Duration,
    host_processing_frame_count: usize,
    min_streamer_processing_time: Duration,
    max_streamer_processing_time: Duration,
    total_streamer_processing_time: Duration,
    streamer_processing_time_frame_count: usize,
}

impl VideoStats {
    fn analyze(
        &mut self,
        stream: &Arc<StreamConnection>,
        unit: &VideoDecodeUnit<&[u8]>,
        frame_processing_time: Duration,
    ) {
        if let Some(host_processing_latency) = unit.frame_processing_latency {
            self.min_host_processing_latency = self
                .min_host_processing_latency
                .min(host_processing_latency);
            self.max_host_processing_latency = self
                .max_host_processing_latency
                .max(host_processing_latency);
            self.total_host_processing_latency += host_processing_latency;
            self.host_processing_frame_count += 1;
        }

        self.min_streamer_processing_time =
            self.min_streamer_processing_time.min(frame_processing_time);
        self.max_streamer_processing_time =
            self.max_streamer_processing_time.max(frame_processing_time);
        self.total_streamer_processing_time += frame_processing_time;
        self.streamer_processing_time_frame_count += 1;

        // Send in 1 sec intervall
        if self
            .last_send
            .map(|last_send| last_send + Duration::from_secs(1) < Instant::now())
            .unwrap_or(true)
        {
            // Collect data
            let has_host_processing_latency = self.host_processing_frame_count > 0;
            let min_host_processing_latency = self.min_host_processing_latency;
            let max_host_processing_latency = self.max_host_processing_latency;
            let avg_host_processing_latency = self
                .total_host_processing_latency
                .checked_div(self.host_processing_frame_count as u32)
                .unwrap_or(Duration::ZERO);

            let min_streamer_processing_time = self.min_streamer_processing_time;
            let max_streamer_processing_time = self.max_streamer_processing_time;
            let avg_streamer_processing_time = self
                .total_streamer_processing_time
                .checked_div(self.streamer_processing_time_frame_count as u32)
                .unwrap_or(Duration::ZERO);

            // Send data
            let runtime = stream.runtime.clone();

            let stream = stream.clone();
            runtime.spawn(async move {
                stream
                    .try_send_packet(
                        OutboundPacket::Stats(StreamerStatsUpdate::Video {
                            host_processing_latency: has_host_processing_latency.then_some(
                                StatsHostProcessingLatency {
                                    min_host_processing_latency_ms: min_host_processing_latency
                                        .as_secs_f64()
                                        * 1000.0,
                                    max_host_processing_latency_ms: max_host_processing_latency
                                        .as_secs_f64()
                                        * 1000.0,
                                    avg_host_processing_latency_ms: avg_host_processing_latency
                                        .as_secs_f64()
                                        * 1000.0,
                                },
                            ),
                            min_streamer_processing_time_ms: min_streamer_processing_time
                                .as_secs_f64()
                                * 1000.0,
                            max_streamer_processing_time_ms: max_streamer_processing_time
                                .as_secs_f64()
                                * 1000.0,
                            avg_streamer_processing_time_ms: avg_streamer_processing_time
                                .as_secs_f64()
                                * 1000.0,
                        }),
                        "host / streamer processing latency",
                        false,
                    )
                    .await;

                // Send RTT info
                let ml_stream_lock = stream.stream.read().await;
                if let Some(ml_stream) = ml_stream_lock.as_ref() {
                    let rtt = ml_stream.estimated_rtt_info();
                    drop(ml_stream_lock);

                    match rtt {
                        Ok(EstimatedRttInfo { rtt, rtt_variance }) => {
                            stream
                                .try_send_packet(
                                    OutboundPacket::Stats(StreamerStatsUpdate::Rtt {
                                        rtt_ms: rtt.as_secs_f64() * 1000.0,
                                        rtt_variance_ms: rtt_variance.as_secs_f64() * 1000.0,
                                    }),
                                    "estimated rtt info",
                                    false,
                                )
                                .await;
                        }
                        Err(err) => {
                            warn!("failed to get estimated rtt info: {err:?}");
                        }
                    };
                }
            });

            // Clear data
            self.min_host_processing_latency = Duration::MAX;
            self.max_host_processing_latency = Duration::ZERO;
            self.total_host_processing_latency = Duration::ZERO;
            self.host_processing_frame_count = 0;
            self.min_streamer_processing_time = Duration::MAX;
            self.max_streamer_processing_time = Duration::ZERO;
            self.total_streamer_processing_time = Duration::ZERO;
            self.streamer_processing_time_frame_count = 0;

            self.last_send = Some(Instant::now());
        }
    }
}
