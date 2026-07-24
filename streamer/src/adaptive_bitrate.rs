use std::time::{Duration, Instant};

const REQUIRED_CONGESTED_SAMPLES: u8 = 3;
const DOWNSHIFT_NUMERATOR: u64 = 3;
const DOWNSHIFT_DENOMINATOR: u64 = 4;
const MIN_PACKETS_FOR_LOSS_SIGNAL: u64 = 32;
const LOSS_PERCENT_THRESHOLD: u64 = 2;
const MIN_CONGESTION_SAMPLE_GAP: Duration = Duration::from_millis(250);
const CONGESTION_STREAK_MAX_GAP: Duration = Duration::from_secs(2);
const RTT_BASELINE_WINDOW: Duration = Duration::from_secs(10);
const RTT_PATH_CHANGE_FACTOR: u32 = 4;
const RTT_PATH_CHANGE_MIN_DELTA_MS: u32 = 100;
const STARTUP_WARMUP: Duration = Duration::from_secs(5);

pub(crate) const ADAPTIVE_RESTART_COOLDOWN: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FeedbackSample {
    pub rtt_ms: u32,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub congestion_events: u64,
    pub admission_drops: u64,
    pub video_write_timeouts: u64,
    pub recovery_requests: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BitrateReduction {
    pub previous_bitrate_kbps: u32,
    pub bitrate_kbps: u32,
}

#[derive(Debug)]
pub(crate) struct AdaptiveBitrateController {
    enabled: bool,
    ceiling_bitrate_kbps: u32,
    current_bitrate_kbps: u32,
    floor_bitrate_kbps: u32,
    previous_feedback: Option<FeedbackSample>,
    minimum_rtt_ms: Option<u32>,
    current_rtt_window_min_ms: Option<u32>,
    previous_rtt_window_min_ms: Option<u32>,
    rtt_window_started: Option<Instant>,
    congested_samples: u8,
    last_observation: Option<Instant>,
    last_reduction: Option<Instant>,
    startup_warmup_until: Option<Instant>,
}

impl Default for AdaptiveBitrateController {
    fn default() -> Self {
        Self::new(false, 0, 0)
    }
}

impl AdaptiveBitrateController {
    pub(crate) fn new(enabled: bool, ceiling_bitrate_kbps: u32, floor_bitrate_kbps: u32) -> Self {
        let floor_bitrate_kbps = floor_bitrate_kbps.min(ceiling_bitrate_kbps);
        Self {
            enabled,
            ceiling_bitrate_kbps,
            current_bitrate_kbps: ceiling_bitrate_kbps,
            floor_bitrate_kbps,
            previous_feedback: None,
            minimum_rtt_ms: None,
            current_rtt_window_min_ms: None,
            previous_rtt_window_min_ms: None,
            rtt_window_started: None,
            congested_samples: 0,
            last_observation: None,
            last_reduction: None,
            startup_warmup_until: None,
        }
    }

    pub(crate) fn reset(
        &mut self,
        enabled: bool,
        ceiling_bitrate_kbps: u32,
        floor_bitrate_kbps: u32,
        now: Instant,
    ) {
        *self = Self::new(enabled, ceiling_bitrate_kbps, floor_bitrate_kbps);
        self.startup_warmup_until = Some(now + STARTUP_WARMUP);
    }

    pub(crate) fn current_bitrate_kbps(&self) -> u32 {
        self.current_bitrate_kbps
    }

    pub(crate) fn rollback(&mut self, bitrate_kbps: u32) {
        self.current_bitrate_kbps = bitrate_kbps
            .max(self.floor_bitrate_kbps)
            .min(self.ceiling_bitrate_kbps);
        self.congested_samples = 0;
    }

    pub(crate) fn cancel_reduction(&mut self, reduction: BitrateReduction) -> bool {
        if self.current_bitrate_kbps != reduction.bitrate_kbps {
            return false;
        }
        self.rollback(reduction.previous_bitrate_kbps);
        true
    }

    /// Keep cumulative counters and the RTT baseline current while a bitrate
    /// restart is pending, without queuing a second downshift behind it.
    pub(crate) fn observe_while_restart_pending(&mut self, feedback: FeedbackSample, now: Instant) {
        let previous = self.previous_feedback.replace(feedback);
        self.last_observation = Some(now);
        self.congested_samples = 0;

        if previous.is_some_and(|previous| feedback.counters_reset_since(previous)) {
            self.reset_rtt_baseline(feedback.rtt_ms, now);
        } else {
            self.record_rtt(feedback.rtt_ms, now);
        }
    }

    pub(crate) fn observe(
        &mut self,
        feedback: FeedbackSample,
        now: Instant,
    ) -> Option<BitrateReduction> {
        let previous = self.previous_feedback.replace(feedback);
        self.record_rtt(feedback.rtt_ms, now);

        let observation_gap = self
            .last_observation
            .replace(now)
            .map(|last| now.saturating_duration_since(last));
        if observation_gap.is_some_and(|gap| gap > CONGESTION_STREAK_MAX_GAP) {
            self.congested_samples = 0;
        }

        // Initial decoder setup, the first IDR, and freshly established QUIC
        // counters are noisy but not evidence that the selected steady-state
        // bitrate is too high. Keep all cumulative/RTT baselines fresh during
        // this window, while refusing to build a congestion streak.
        if self
            .startup_warmup_until
            .is_some_and(|warmup_until| now < warmup_until)
        {
            if previous.is_some_and(|previous| feedback.counters_reset_since(previous)) {
                self.reset_rtt_baseline(feedback.rtt_ms, now);
            }
            self.congested_samples = 0;
            return None;
        }
        self.startup_warmup_until = None;

        if !self.enabled || self.current_bitrate_kbps <= self.floor_bitrate_kbps {
            self.congested_samples = 0;
            return None;
        }

        // The first report establishes cumulative-counter and RTT baselines. It
        // must never trigger a restart simply because the connection has
        // already transferred a large number of packets.
        let previous = previous?;
        if feedback.counters_reset_since(previous) {
            self.reset_rtt_baseline(feedback.rtt_ms, now);
            self.congested_samples = 0;
            return None;
        }

        let delta = feedback.delta_since(previous);
        let transport_congestion = delta.indicates_transport_congestion();
        let baseline_rtt_ms = self.minimum_rtt_ms.unwrap_or(0);
        let rtt_congestion = delta.indicates_rtt_congestion(baseline_rtt_ms);
        let apparent_path_change = !transport_congestion
            && baseline_rtt_ms > 0
            && feedback.rtt_ms
                > baseline_rtt_ms
                    .saturating_mul(RTT_PATH_CHANGE_FACTOR)
                    .max(baseline_rtt_ms.saturating_add(RTT_PATH_CHANGE_MIN_DELTA_MS));
        if apparent_path_change {
            self.reset_rtt_baseline(feedback.rtt_ms, now);
            self.congested_samples = 0;
            return None;
        }

        // IPC/watch coalescing can deliver a buffered report and the newest
        // report back-to-back. Counters still advance, but those reports are
        // not independent evidence of sustained congestion. A clean newest
        // report is recovery evidence and clears any prior streak.
        if observation_gap.is_some_and(|gap| gap < MIN_CONGESTION_SAMPLE_GAP) {
            if !transport_congestion && !rtt_congestion {
                self.congested_samples = 0;
            }
            return None;
        }

        if transport_congestion || rtt_congestion {
            self.congested_samples = self.congested_samples.saturating_add(1);
        } else {
            self.congested_samples = 0;
        }

        if self.congested_samples < REQUIRED_CONGESTED_SAMPLES
            || self
                .last_reduction
                .is_some_and(|last| now.saturating_duration_since(last) < ADAPTIVE_RESTART_COOLDOWN)
        {
            return None;
        }

        let previous_bitrate_kbps = self.current_bitrate_kbps;
        let reduced = ((u64::from(previous_bitrate_kbps) * DOWNSHIFT_NUMERATOR)
            / DOWNSHIFT_DENOMINATOR) as u32;
        let bitrate_kbps = reduced.max(self.floor_bitrate_kbps);
        self.congested_samples = 0;
        self.last_reduction = Some(now);

        if bitrate_kbps >= previous_bitrate_kbps {
            return None;
        }

        self.current_bitrate_kbps = bitrate_kbps;
        Some(BitrateReduction {
            previous_bitrate_kbps,
            bitrate_kbps,
        })
    }

    fn record_rtt(&mut self, rtt_ms: u32, now: Instant) {
        if rtt_ms == 0 {
            return;
        }

        match self.rtt_window_started {
            None => {
                self.rtt_window_started = Some(now);
                self.current_rtt_window_min_ms = Some(rtt_ms);
            }
            Some(started) if now.saturating_duration_since(started) >= RTT_BASELINE_WINDOW => {
                let elapsed = now.saturating_duration_since(started);
                self.previous_rtt_window_min_ms = (elapsed < RTT_BASELINE_WINDOW * 2)
                    .then_some(self.current_rtt_window_min_ms)
                    .flatten();
                self.current_rtt_window_min_ms = Some(rtt_ms);
                self.rtt_window_started = Some(now);
            }
            Some(_) => {
                self.current_rtt_window_min_ms = Some(
                    self.current_rtt_window_min_ms
                        .map_or(rtt_ms, |minimum| minimum.min(rtt_ms)),
                );
            }
        }

        self.minimum_rtt_ms = match (
            self.previous_rtt_window_min_ms,
            self.current_rtt_window_min_ms,
        ) {
            (Some(previous), Some(current)) => Some(previous.min(current)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
    }

    fn reset_rtt_baseline(&mut self, rtt_ms: u32, now: Instant) {
        self.previous_rtt_window_min_ms = None;
        self.current_rtt_window_min_ms = (rtt_ms > 0).then_some(rtt_ms);
        self.minimum_rtt_ms = self.current_rtt_window_min_ms;
        self.rtt_window_started = self.current_rtt_window_min_ms.map(|_| now);
    }
}

impl FeedbackSample {
    fn counters_reset_since(self, previous: Self) -> bool {
        self.sent_packets < previous.sent_packets
            || self.lost_packets < previous.lost_packets
            || self.congestion_events < previous.congestion_events
            || self.admission_drops < previous.admission_drops
            || self.video_write_timeouts < previous.video_write_timeouts
            || self.recovery_requests < previous.recovery_requests
    }

    fn delta_since(self, previous: Self) -> FeedbackDelta {
        FeedbackDelta {
            rtt_ms: self.rtt_ms,
            sent_packets: cumulative_delta(self.sent_packets, previous.sent_packets),
            lost_packets: cumulative_delta(self.lost_packets, previous.lost_packets),
            congestion_events: cumulative_delta(self.congestion_events, previous.congestion_events),
            admission_drops: cumulative_delta(self.admission_drops, previous.admission_drops),
            video_write_timeouts: cumulative_delta(
                self.video_write_timeouts,
                previous.video_write_timeouts,
            ),
            recovery_requests: cumulative_delta(self.recovery_requests, previous.recovery_requests),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct FeedbackDelta {
    rtt_ms: u32,
    sent_packets: u64,
    lost_packets: u64,
    congestion_events: u64,
    admission_drops: u64,
    video_write_timeouts: u64,
    recovery_requests: u64,
}

impl FeedbackDelta {
    fn indicates_transport_congestion(self) -> bool {
        let loss_signal = self.sent_packets >= MIN_PACKETS_FOR_LOSS_SIGNAL
            && self.lost_packets.saturating_mul(100)
                >= self.sent_packets.saturating_mul(LOSS_PERCENT_THRESHOLD);
        self.video_write_timeouts > 0
            || self.admission_drops > 0
            || self.congestion_events > 0
            || self.recovery_requests > 0
            || loss_signal
    }

    fn indicates_rtt_congestion(self, minimum_rtt_ms: u32) -> bool {
        let rtt_allowance = 20_u32.max(minimum_rtt_ms / 2);
        self.rtt_ms > 0
            && minimum_rtt_ms > 0
            && self.rtt_ms > minimum_rtt_ms.saturating_add(rtt_allowance)
    }
}

fn cumulative_delta(current: u64, previous: u64) -> u64 {
    // The server counters reset for a replacement WebTransport connection. In
    // that case the new value is the complete delta rather than an underflow.
    current.checked_sub(previous).unwrap_or(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feedback(sent: u64, lost: u64, congestion_events: u64) -> FeedbackSample {
        FeedbackSample {
            rtt_ms: 20,
            sent_packets: sent,
            lost_packets: lost,
            congestion_events,
            ..FeedbackSample::default()
        }
    }

    #[test]
    fn startup_warmup_ignores_initial_connection_noise() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(false, 0, 0);
        controller.reset(true, 10_000, 2_000, start);

        assert_eq!(controller.observe(feedback(100, 0, 0), start), None);
        for half_second in 1..10 {
            assert_eq!(
                controller.observe(
                    feedback(100 + half_second * 100, half_second * 5, half_second,),
                    start + Duration::from_millis(half_second * 500),
                ),
                None
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 10_000);

        // Three independently spaced congested reports are still required
        // after the grace period expires.
        for half_second in 10..=11 {
            assert_eq!(
                controller.observe(
                    feedback(100 + half_second * 100, half_second * 5, half_second,),
                    start + Duration::from_millis(half_second * 500),
                ),
                None
            );
        }
        assert_eq!(
            controller.observe(feedback(1_300, 60, 12), start + Duration::from_secs(6),),
            Some(BitrateReduction {
                previous_bitrate_kbps: 10_000,
                bitrate_kbps: 7_500,
            })
        );
    }

    #[test]
    fn isolated_congestion_burst_is_ignored() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 10_000, 2_000);

        assert_eq!(controller.observe(feedback(100, 0, 0), start), None);
        assert_eq!(
            controller.observe(feedback(200, 0, 1), start + Duration::from_secs(1)),
            None
        );
        assert_eq!(
            controller.observe(feedback(300, 0, 1), start + Duration::from_secs(2)),
            None
        );
        assert_eq!(controller.current_bitrate_kbps(), 10_000);
    }

    #[test]
    fn sustained_congestion_reduces_bitrate_by_one_quarter() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 10_000, 2_000);
        assert_eq!(controller.ceiling_bitrate_kbps, 10_000);
        assert_eq!(controller.observe(feedback(100, 0, 0), start), None);

        for second in 1..=2 {
            assert_eq!(
                controller.observe(
                    feedback(100 + second * 100, 0, second),
                    start + Duration::from_secs(second),
                ),
                None
            );
        }
        assert_eq!(
            controller.observe(feedback(400, 0, 3), start + Duration::from_secs(3)),
            Some(BitrateReduction {
                previous_bitrate_kbps: 10_000,
                bitrate_kbps: 7_500,
            })
        );
    }

    #[test]
    fn cooldown_suppresses_repeated_restarts() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 10_000, 2_000);
        controller.observe(feedback(100, 0, 0), start);
        for second in 1..=3 {
            controller.observe(
                feedback(100 + second * 100, 0, second),
                start + Duration::from_secs(second),
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 7_500);

        for second in 4..=17 {
            assert_eq!(
                controller.observe(
                    feedback(100 + second * 100, 0, second),
                    start + Duration::from_secs(second),
                ),
                None
            );
        }
        assert_eq!(
            controller.observe(feedback(1_900, 0, 18), start + Duration::from_secs(18)),
            Some(BitrateReduction {
                previous_bitrate_kbps: 7_500,
                bitrate_kbps: 5_625,
            })
        );
    }

    #[test]
    fn reductions_stop_at_configured_floor() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 3_000, 2_500);
        controller.observe(feedback(100, 0, 0), start);
        for second in 1..=3 {
            controller.observe(
                feedback(100 + second * 100, 0, second),
                start + Duration::from_secs(second),
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 2_500);

        for second in 20..=24 {
            assert_eq!(
                controller.observe(
                    feedback(100 + second * 100, 0, second),
                    start + Duration::from_secs(second),
                ),
                None
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 2_500);
    }

    #[test]
    fn cumulative_counters_use_deltas_and_handle_reset() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        assert_eq!(controller.observe(feedback(10_000, 500, 90), start), None);

        // Unchanged totals are clean even though the lifetime loss count is high.
        assert_eq!(
            controller.observe(feedback(10_100, 500, 90), start + Duration::from_secs(1)),
            None
        );

        // A server-side counter reset establishes a new baseline. Subsequent
        // deltas still trigger after three independently spaced samples.
        for second in 2..=5 {
            assert_eq!(
                controller.observe(
                    feedback((second - 1) * 100, (second - 1) * 5, second - 1),
                    start + Duration::from_secs(second),
                ),
                if second == 5 {
                    Some(BitrateReduction {
                        previous_bitrate_kbps: 8_000,
                        bitrate_kbps: 6_000,
                    })
                } else {
                    None
                }
            );
        }
    }

    #[test]
    fn rollback_restores_previous_known_good_rate() {
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        controller.current_bitrate_kbps = 6_000;
        controller.rollback(8_000);
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
    }

    #[test]
    fn pending_restart_observations_do_not_stack_downshifts() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        controller.observe(feedback(100, 0, 0), start);
        for second in 1..=3 {
            controller.observe(
                feedback(100 + second * 100, 0, second),
                start + Duration::from_secs(second),
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 6_000);

        for second in 4..=40 {
            controller.observe_while_restart_pending(
                feedback(100 + second * 100, 0, second),
                start + Duration::from_secs(second),
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 6_000);

        // Cancelling the stale pending restart restores the only committed
        // target; none of the reports above queued another reduction.
        assert!(controller.cancel_reduction(BitrateReduction {
            previous_bitrate_kbps: 8_000,
            bitrate_kbps: 6_000,
        }));
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
    }

    #[test]
    fn expired_restart_only_rolls_back_its_own_pending_target() {
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        controller.current_bitrate_kbps = 6_000;

        assert!(!controller.cancel_reduction(BitrateReduction {
            previous_bitrate_kbps: 6_000,
            bitrate_kbps: 4_500,
        }));
        assert_eq!(controller.current_bitrate_kbps(), 6_000);

        assert!(controller.cancel_reduction(BitrateReduction {
            previous_bitrate_kbps: 8_000,
            bitrate_kbps: 6_000,
        }));
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
    }

    #[test]
    fn congestion_streak_expires_and_zero_rtt_is_not_a_baseline() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        let mut sample = feedback(100, 0, 0);
        sample.rtt_ms = 0;
        controller.observe(sample, start);

        sample.recovery_requests = 1;
        assert_eq!(
            controller.observe(sample, start + Duration::from_millis(500)),
            None
        );
        sample.recovery_requests = 2;
        assert_eq!(
            controller.observe(sample, start + Duration::from_secs(4)),
            None
        );
        sample.recovery_requests = 3;
        assert_eq!(
            controller.observe(sample, start + Duration::from_secs(8)),
            None
        );
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
        assert_eq!(controller.minimum_rtt_ms, None);
    }

    #[test]
    fn back_to_back_coalesced_reports_do_not_look_sustained() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        controller.observe(feedback(100, 0, 0), start);
        assert_eq!(
            controller.observe(feedback(200, 0, 1), start + Duration::from_millis(500)),
            None
        );
        assert_eq!(
            controller.observe(feedback(300, 0, 2), start + Duration::from_millis(501)),
            None
        );
        assert_eq!(
            controller.observe(feedback(400, 0, 3), start + Duration::from_millis(502)),
            None
        );
        assert_eq!(
            controller.observe(feedback(500, 0, 4), start + Duration::from_millis(1_000)),
            None
        );
        assert_eq!(
            controller.observe(feedback(600, 0, 5), start + Duration::from_millis(1_500)),
            Some(BitrateReduction {
                previous_bitrate_kbps: 8_000,
                bitrate_kbps: 6_000,
            })
        );
    }

    #[test]
    fn healthy_lan_to_wan_path_change_rebases_rtt() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        let mut sample = feedback(100, 0, 0);
        sample.rtt_ms = 5;
        controller.observe(sample, start);

        for index in 1..=40 {
            sample.sent_packets += 100;
            sample.rtt_ms = 250;
            assert_eq!(
                controller.observe(sample, start + Duration::from_millis(index * 500)),
                None
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
        assert_eq!(controller.minimum_rtt_ms, Some(250));
    }

    #[test]
    fn counter_reset_immediately_rebases_rtt() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        let mut sample = feedback(10_000, 100, 10);
        sample.rtt_ms = 5;
        controller.observe(sample, start);

        sample = feedback(10, 0, 0);
        sample.rtt_ms = 250;
        assert_eq!(
            controller.observe(sample, start + Duration::from_millis(500)),
            None
        );
        assert_eq!(controller.minimum_rtt_ms, Some(250));
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
    }

    #[test]
    fn back_to_back_clean_report_clears_congestion_streak() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        controller.observe(feedback(100, 0, 0), start);

        let samples = [
            (500, feedback(200, 0, 1)),
            (501, feedback(201, 0, 1)),
            (1_000, feedback(300, 0, 2)),
            (1_001, feedback(301, 0, 2)),
            (1_500, feedback(400, 0, 3)),
        ];
        for (millis, sample) in samples {
            assert_eq!(
                controller.observe(sample, start + Duration::from_millis(millis)),
                None
            );
        }
        assert_eq!(controller.current_bitrate_kbps(), 8_000);
    }

    #[test]
    fn rtt_window_rollover_does_not_hide_bufferbloat() {
        let start = Instant::now();
        let mut controller = AdaptiveBitrateController::new(true, 8_000, 2_000);
        let mut sample = feedback(100, 0, 0);
        sample.rtt_ms = 20;
        controller.observe(sample, start);

        for (index, millis) in [9_500, 10_000, 10_500].into_iter().enumerate() {
            sample.sent_packets += 100;
            sample.rtt_ms = 80;
            assert_eq!(
                controller.observe(sample, start + Duration::from_millis(millis)),
                if index == 2 {
                    Some(BitrateReduction {
                        previous_bitrate_kbps: 8_000,
                        bitrate_kbps: 6_000,
                    })
                } else {
                    None
                }
            );
        }
    }
}
