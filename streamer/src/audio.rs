use std::sync::Weak;

use bytes::Bytes;
use log::{debug, error, warn};
use moonlight_common::stream::audio::{
    AudioConfig, AudioDecoder, AudioFrame, OpusMultistreamConfig,
};

use crate::StreamConnection;

pub(crate) struct StreamAudioDecoder {
    pub(crate) stream: Weak<StreamConnection>,
    pub(crate) generation: u64,
}

impl AudioDecoder for StreamAudioDecoder {
    fn setup(&mut self, audio_config: AudioConfig, stream_config: OpusMultistreamConfig) -> i32 {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to setup audio because stream is deallocated");
            return -1;
        };
        if !stream.is_current_native_generation(self.generation) {
            return 0;
        }

        {
            let mut stream_info = stream.stream_setup.blocking_lock();
            if !stream.is_current_native_generation(self.generation) {
                return 0;
            }
            stream_info.audio = Some(stream_config.clone());
        }

        let generation = self.generation;
        stream.runtime.clone().block_on(async move {
            if !stream.is_current_native_generation(generation) {
                return 0;
            }
            let sender = {
                let sender = stream.transport_sender.lock().await;
                sender.clone()
            };

            if let Some(sender) = sender {
                sender.setup_audio(audio_config, stream_config).await
            } else {
                error!("Failed to setup audio because of missing transport!");
                -1
            }
        })
    }

    fn start(&mut self) {}
    fn stop(&mut self) {}

    fn decode_and_play_sample(&mut self, sample: AudioFrame<&[u8]>) {
        let Some(stream) = self.stream.upgrade() else {
            warn!("Failed to send audio sample because stream is deallocated");
            return;
        };
        if !stream.is_native_media_ready(self.generation) {
            return;
        }

        let packet = (self.generation, Bytes::copy_from_slice(sample.buffer));
        match stream.audio_dispatch_tx.try_send(packet) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!("Audio dispatch queue overflow; dropping newest packet");
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                debug!("Dropping audio packet because dispatch is closed");
            }
        }
    }

    fn config(&self) -> AudioConfig {
        AudioConfig::STEREO
    }
}
