// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use cxx::SharedPtr;
use livekit_runtime::Stream;
use tokio::sync::mpsc;
use webrtc_sys::audio_track as sys_at;

use crate::{
    audio_frame::{AudioFrame, TimedAudioFrame},
    audio_track::RtcAudioTrack,
};

pub struct NativeAudioStream {
    native_sink: SharedPtr<sys_at::ffi::NativeAudioSink>,
    audio_track: RtcAudioTrack,
    frame_rx: mpsc::UnboundedReceiver<AudioFrame<'static>>,
}

pub struct NativeTimedAudioStream {
    native_sink: SharedPtr<sys_at::ffi::NativeAudioSink>,
    audio_track: RtcAudioTrack,
    frame_rx: mpsc::UnboundedReceiver<TimedAudioFrame<'static>>,
}

impl NativeAudioStream {
    pub fn new(audio_track: RtcAudioTrack, sample_rate: i32, num_channels: i32) -> Self {
        let (frame_tx, frame_rx) = mpsc::unbounded_channel();
        let observer = Arc::new(AudioTrackObserver { frame_tx });
        let native_sink = sys_at::ffi::new_native_audio_sink(
            Box::new(sys_at::AudioSinkWrapper::new(observer.clone())),
            sample_rate,
            num_channels,
        );

        let audio = unsafe { sys_at::ffi::media_to_audio(audio_track.sys_handle()) };
        audio.add_sink(&native_sink);

        Self { native_sink, audio_track, frame_rx }
    }

    pub fn track(&self) -> RtcAudioTrack {
        self.audio_track.clone()
    }

    pub fn close(&mut self) {
        let audio = unsafe { sys_at::ffi::media_to_audio(self.audio_track.sys_handle()) };
        audio.remove_sink(&self.native_sink);

        self.frame_rx.close();
    }
}

impl Drop for NativeAudioStream {
    fn drop(&mut self) {
        self.close();
    }
}

impl NativeTimedAudioStream {
    pub fn new(audio_track: RtcAudioTrack, sample_rate: i32, num_channels: i32) -> Self {
        let (frame_tx, frame_rx) = mpsc::unbounded_channel();
        let observer = Arc::new(TimedAudioTrackObserver { frame_tx });
        let native_sink = sys_at::ffi::new_native_audio_sink(
            Box::new(sys_at::AudioSinkWrapper::new(observer)),
            sample_rate,
            num_channels,
        );

        let audio = unsafe { sys_at::ffi::media_to_audio(audio_track.sys_handle()) };
        audio.add_sink(&native_sink);

        Self { native_sink, audio_track, frame_rx }
    }

    pub fn track(&self) -> RtcAudioTrack {
        self.audio_track.clone()
    }

    pub fn close(&mut self) {
        let audio = unsafe { sys_at::ffi::media_to_audio(self.audio_track.sys_handle()) };
        audio.remove_sink(&self.native_sink);
        self.frame_rx.close();
    }
}

impl Drop for NativeTimedAudioStream {
    fn drop(&mut self) {
        self.close();
    }
}

impl Stream for NativeTimedAudioStream {
    type Item = TimedAudioFrame<'static>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.frame_rx.poll_recv(cx)
    }
}

impl Stream for NativeAudioStream {
    type Item = AudioFrame<'static>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.frame_rx.poll_recv(cx)
    }
}

pub struct AudioTrackObserver {
    frame_tx: mpsc::UnboundedSender<AudioFrame<'static>>,
}

impl sys_at::AudioSink for AudioTrackObserver {
    fn on_data(&self, data: &[i16], sample_rate: i32, nb_channels: usize, nb_frames: usize) {
        let _ = self.frame_tx.send(AudioFrame {
            data: data.to_owned().into(),
            sample_rate: sample_rate as u32,
            num_channels: nb_channels as u32,
            samples_per_channel: nb_frames as u32,
        });
    }
}

pub struct TimedAudioTrackObserver {
    frame_tx: mpsc::UnboundedSender<TimedAudioFrame<'static>>,
}

impl sys_at::AudioSink for TimedAudioTrackObserver {
    fn on_data(&self, data: &[i16], sample_rate: i32, nb_channels: usize, nb_frames: usize) {
        self.on_data_with_timestamp(data, sample_rate, nb_channels, nb_frames, None);
    }

    fn on_data_with_timestamp(
        &self,
        data: &[i16],
        sample_rate: i32,
        nb_channels: usize,
        nb_frames: usize,
        absolute_capture_timestamp_ms: Option<i64>,
    ) {
        let _ = self.frame_tx.send(TimedAudioFrame {
            frame: AudioFrame {
                data: data.to_owned().into(),
                sample_rate: sample_rate as u32,
                num_channels: nb_channels as u32,
                samples_per_channel: nb_frames as u32,
            },
            absolute_capture_timestamp_ms,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use webrtc_sys::audio_track::AudioSink;

    #[test]
    fn timed_observer_keeps_audio_and_capture_timestamp_together() {
        let (frame_tx, mut frame_rx) = mpsc::unbounded_channel();
        let observer = TimedAudioTrackObserver { frame_tx };

        observer.on_data_with_timestamp(&[10, 20], 48_000, 1, 2, Some(1_234));

        let timed = frame_rx.try_recv().unwrap();
        assert_eq!(timed.absolute_capture_timestamp_ms, Some(1_234));
        assert_eq!(timed.frame.data.as_ref(), &[10, 20]);
        assert_eq!(timed.frame.sample_rate, 48_000);
        assert_eq!(timed.frame.num_channels, 1);
        assert_eq!(timed.frame.samples_per_channel, 2);
    }
}
