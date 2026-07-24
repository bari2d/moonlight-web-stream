use log::warn;
use moonlight_common::stream::video::VideoFormats;
use serde::Serialize;

use crate::api_bindings::{StreamPermissions, StreamSettings};

pub mod api_bindings;
pub mod api_bindings_ext;
pub mod config;
pub mod ipc;

/// Applies the permissions / restrictions to the current settings of the user.
/// This won't error, it'll just overwrite it, because the GUI should indicate those restrictions.
pub fn apply_permissions_to_settings(
    permissions: &StreamPermissions,
    settings: &mut StreamSettings,
) {
    let StreamPermissions {
        allow_add_hosts: _,
        maximum_bitrate_kbps,
        allow_codec_h264,
        allow_codec_h265,
        allow_codec_av1,
        allow_hdr,
        allow_transport_webrtc: _,
        allow_transport_websockets: _,
    } = permissions;

    if let Some(maximum_bitrate) = maximum_bitrate_kbps
        && settings.bitrate_kbps > *maximum_bitrate
    {
        settings.bitrate_kbps = *maximum_bitrate;
    }

    // Keep adaptive changes inside the user's requested/permitted ceiling. A
    // minimum below 500 Kbps is not useful for the supported video profiles,
    // except when an administrator has explicitly set an even lower ceiling.
    let adaptive_floor = settings.bitrate_kbps.min(500);
    settings.minimum_bitrate_kbps = settings
        .minimum_bitrate_kbps
        .clamp(adaptive_floor, settings.bitrate_kbps);

    let mut supported_codecs = VideoFormats::from_bits_truncate(settings.supported_codecs);
    if !allow_codec_h264 {
        supported_codecs &= !VideoFormats::MASK_H264;
    }
    if !allow_codec_h265 {
        supported_codecs &= !VideoFormats::MASK_H265;
    }
    if !allow_codec_av1 {
        supported_codecs &= !VideoFormats::MASK_AV1;
    }
    settings.supported_codecs = supported_codecs.bits();

    if !allow_hdr {
        settings.hdr = false;
    }

    // Transport restrictions are handled in the streamer
}

pub fn serialize_json<T>(message: &T) -> Option<String>
where
    T: Serialize,
{
    let Ok(json) = serde_json::to_string(&message) else {
        warn!("[Stream]: failed to serialize to json");
        return None;
    };

    Some(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unrestricted_permissions(maximum_bitrate_kbps: Option<u32>) -> StreamPermissions {
        StreamPermissions {
            allow_add_hosts: true,
            maximum_bitrate_kbps,
            allow_codec_h264: true,
            allow_codec_h265: true,
            allow_codec_av1: true,
            allow_hdr: true,
            allow_transport_webrtc: true,
            allow_transport_websockets: true,
        }
    }

    #[test]
    fn old_stream_settings_default_to_secure_adaptation() {
        let settings: StreamSettings = serde_json::from_value(serde_json::json!({
            "bitrate_kbps": 10_000,
            "width": 1920,
            "height": 1080,
            "fps": 60,
            "play_audio_local": false,
            "supported_codecs": 0,
            "hdr": false
        }))
        .unwrap();

        assert!(settings.adaptive_bitrate);
        assert_eq!(settings.minimum_bitrate_kbps, 2_000);
        assert!(settings.encrypt_host_video);
        assert!(settings.encrypt_host_audio);
    }

    #[test]
    fn adaptive_minimum_is_bounded_by_floor_and_permitted_ceiling() {
        let mut settings = StreamSettings {
            bitrate_kbps: 10_000,
            adaptive_bitrate: true,
            minimum_bitrate_kbps: 100,
            width: 1920,
            height: 1080,
            fps: 60,
            play_audio_local: false,
            encrypt_host_video: true,
            encrypt_host_audio: true,
            supported_codecs: 0,
            hdr: false,
        };
        let permissions = unrestricted_permissions(Some(4_000));

        apply_permissions_to_settings(&permissions, &mut settings);
        assert_eq!(settings.bitrate_kbps, 4_000);
        assert_eq!(settings.minimum_bitrate_kbps, 500);

        settings.minimum_bitrate_kbps = 8_000;
        apply_permissions_to_settings(&permissions, &mut settings);
        assert_eq!(settings.minimum_bitrate_kbps, 4_000);
    }
}
