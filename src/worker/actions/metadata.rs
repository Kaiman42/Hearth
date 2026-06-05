use crate::config::Config;
use crate::worker::connector::send_message;
use anyhow::{Context, Result};
use hearth_interconnect::errors::ErrorReport;
use hearth_interconnect::messages::{Message, Metadata};
use songbird::tracks::TrackHandle;
use std::sync::OnceLock;
use std::thread;
use tokio::sync::Mutex;

static CONFIG: OnceLock<Mutex<Option<Config>>> = OnceLock::new();
static REQUEST_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static JOB_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static GUILD_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[macro_export]
macro_rules! report_metadata_error {
    ($e: ident) => {
        use $crate::worker::errors::report_error;

        let mut cx = CONFIG.get().unwrap().lock().await;
        let c = cx.as_mut();

        let mut jx = JOB_ID.get().unwrap().lock().await;
        let j = jx.as_mut();

        let mut rx = REQUEST_ID.get().unwrap().lock().await;
        let r = rx.as_mut();

        let mut gx = GUILD_ID.get().unwrap().lock().await;
        let g = gx.as_mut();

        report_error(
            ErrorReport {
                error: format!("Failed to perform Metadata Extraction with error: {}", $e),
                request_id: r.unwrap().clone(),
                job_id: j.unwrap().clone(),
                guild_id: g.unwrap().clone(),
            },
            &*c.unwrap(),
        );
    };
}

async fn get_codec_metadata(duration: Option<u64>, sample_rate: Option<u32>, position: u64) -> Result<Metadata> {
    let mut jx = JOB_ID.get().unwrap().lock().await;
    let j = jx.as_mut();

    let mut gx = GUILD_ID.get().unwrap().lock().await;
    let g = gx.as_mut();

    let job_id = j
        .as_ref()
        .context("Failed to get JOB ID. While getting Metadata")?;

    let guild_id = g
        .as_ref()
        .context("Failed to get JOB ID. While getting Metadata")?;

    Ok(Metadata {
        duration,
        position: Some(position),
        sample_rate,
        job_id: job_id.to_string(),
        guild_id: guild_id.to_string(),
    })
}

async fn get_metadata_sub(duration: Option<u64>, sample_rate: Option<u32>, position: u64) {
    let r = get_codec_metadata(duration, sample_rate, position).await;
    match r {
        Ok(a) => {
            send_message(&Message::ExternalMetadataResult(a)).await;
        }
        Err(e) => {
            report_metadata_error!(e);
        }
    }
}

pub async fn get_metadata(
    track: &Option<TrackHandle>,
    config: &Config,
    request_id: String,
    job_id: String,
    guild_id: String,
) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;

    let _ = CONFIG.set(Mutex::new(Some(config.clone())));
    let _ = JOB_ID.set(Mutex::new(Some(job_id)));
    let _ = REQUEST_ID.set(Mutex::new(Some(request_id)));
    let _ = GUILD_ID.set(Mutex::new(Some(guild_id)));

    let duration = None;
    let sample_rate = None;

    let _ = t.action(move |view| {
        let position = view.position.as_secs();
        thread::spawn(move || {
            futures::executor::block_on(get_metadata_sub(duration, sample_rate, position));
        });
        None
    });

    Ok(())
}
