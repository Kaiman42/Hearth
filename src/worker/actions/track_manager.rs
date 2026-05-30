use std::time::Duration;

use anyhow::{Context, Result};
use nonmax::NonMaxU32;

use songbird::tracks::TrackHandle;

pub async fn pause_playback(track: &Option<TrackHandle>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    t.pause().context("Failed to pause track")?;
    Ok(())
}

pub async fn resume_playback(track: &Option<TrackHandle>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    t.play().context("Failed to play track")?;
    Ok(())
}

pub async fn seek_to_position(track: &Option<TrackHandle>, position: Option<u64>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    let duration_pos = Duration::from_millis(position.context("Failed to get seek position")?);
    let _ = t.seek(duration_pos);
    Ok(())
}

pub async fn loop_x_times(track: &Option<TrackHandle>, times: Option<usize>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    let loop_times = times.context("Failed to get Loop Times")?;
    let loop_times_u32 = u32::try_from(loop_times).context("Loop Times exceeds supported range")?;
    let loop_times =
        NonMaxU32::new(loop_times_u32).context("Loop Times must be greater than zero")?;
    t.loop_for(loop_times)?;
    Ok(())
}

pub async fn loop_indefinitely(track: &Option<TrackHandle>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    t.enable_loop()?;
    Ok(())
}

pub async fn force_stop_loop(track: &Option<TrackHandle>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    t.disable_loop()?;
    Ok(())
}

pub async fn set_playback_volume(track: &Option<TrackHandle>, volume: Option<f32>) -> Result<()> {
    let t = track.as_ref().context("Track not found")?;
    t.set_volume(volume.context("Failed to get Volume from request")?)
        .context("Failed to set volume")?;
    Ok(())
}
