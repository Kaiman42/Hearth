use crate::worker::actions::helpers::get_manager_call;
use anyhow::{Context, Result};
use hearth_interconnect::worker_communication::DirectWorkerCommunication;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, ORIGIN, REFERER};
use reqwest::Client;
use songbird::input::{HttpRequest, YoutubeDl};
use songbird::tracks::TrackHandle;
use songbird::Songbird;
use std::fmt;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
enum PlaybackError {
    MissingAudioURL,
}

impl fmt::Display for PlaybackError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PlaybackError::MissingAudioURL => write!(f, "Missing Audio URL"),
        }
    }
}

fn build_stream_client(request_url: &str) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));

    if let Ok(url) = Url::parse(request_url) {
        let referer = format!("{}/", url.origin().ascii_serialization());
        if let Ok(referer_value) = HeaderValue::from_str(&referer) {
            headers.insert(REFERER, referer_value);
        }

        let origin = url.origin().ascii_serialization();
        if let Ok(origin_value) = HeaderValue::from_str(&origin) {
            headers.insert(ORIGIN, origin_value);
        }
    }

    Client::builder()
        .cookie_store(true)
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")
        .default_headers(headers)
        .build()
        .context("Failed to build HTTP client for direct stream playback")
}

pub async fn play_direct_link(
    dwc: &DirectWorkerCommunication,
    manager: &mut Option<Arc<Songbird>>,
    _client: Client,
) -> Result<TrackHandle> {
    let handler_lock = get_manager_call(&dwc.guild_id, manager).await?;
    let mut handler = handler_lock.lock().await;
    let request_url = dwc
        .play_audio_url
        .clone()
        .context(PlaybackError::MissingAudioURL.to_string())?;
    let client = build_stream_client(&request_url)?;
    let source = HttpRequest::new(
        client,
        request_url,
    );
    let track_handle = handler.play_input(source.into());
    Ok(track_handle)
}

pub async fn play_from_youtube(
    manager: &mut Option<Arc<Songbird>>,
    dwc: &DirectWorkerCommunication,
    client: Client,
) -> Result<TrackHandle> {
    let handler_lock = get_manager_call(&dwc.guild_id, manager).await?;
    let mut handler = handler_lock.lock().await;
    let source = YoutubeDl::new(
        client,
        dwc.play_audio_url
            .clone()
            .context(PlaybackError::MissingAudioURL.to_string())?,
    );
    let track_handle = handler.play_input(source.into());
    Ok(track_handle)
}
