use hearth_interconnect::messages::Message;
use nats::asynk::Connection;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::utils::generic_connector::{nats_connect, nats_listen, nats_publish};
use songbird::Songbird;

use crate::config::Config;
use crate::scheduler::distributor::{distribute_job, ROUND_ROBIN_INDEX, WORKERS};
use crate::worker::queue_processor::{ProcessorIPC, ProcessorIPCData};
use anyhow::Result;
use hearth_interconnect::errors::ErrorReport;

use log::{debug, info, warn};
use tokio::sync::broadcast::Sender;

use crate::worker::errors::report_error;
use tokio::sync::Mutex;

pub static SCHEDULER_NC: OnceLock<Mutex<Option<Connection>>> = OnceLock::new();

pub async fn initialize_api(config: &Config, ipc: &mut ProcessorIPC) {
    let nc = nats_connect(config).await;
    let _ = SCHEDULER_NC.set(Mutex::new(Some(nc.clone())));
    let _ = ROUND_ROBIN_INDEX.set(Mutex::new(0));
    let _ = WORKERS.set(Mutex::new(vec![]));

    nats_listen(
        &nc,
        config,
        parse_message_callback,
        ipc,
        initialized_callback,
        None,
    )
    .await;
}

async fn parse_message_callback(
    parsed_message: Message,
    config: Config,
    _: Arc<Sender<ProcessorIPCData>>,
    _: Option<Arc<Songbird>>,
) -> Result<()> {
    debug!("SCHEDULER GOT MSG: {:?}", parsed_message);
    match parsed_message {
        Message::ExternalQueueJob(j) => {
            let rid = j.request_id.clone();
            let guild_id = j.guild_id.clone();

            let distribute = distribute_job(j, &config).await;
            match distribute {
                Ok(_) => {}
                Err(e) => report_error(
                    ErrorReport {
                        error: e.to_string(),
                        request_id: rid,
                        job_id: "N/A".to_string(),
                        guild_id,
                    },
                    &config,
                ),
            }
        }
        Message::WorkerShutdownAlert(shutdown_alert) => {
            let mut workers = WORKERS.get().unwrap().lock().await;
            workers.retain(|x| x != &shutdown_alert.worker_id);
            let mut index_guard = ROUND_ROBIN_INDEX.get().unwrap().lock().await;
            *index_guard = 0;
        }
        Message::InternalWorkerAnalytics(_a) => {
            //TODO
        }
        Message::InternalPongResponse(r) => {
            let mut workers = WORKERS.get().unwrap().lock().await;
            if !workers.contains(&r.worker_id) {
                workers.push(r.worker_id.clone());
                info!("ADDED NEW WORKER: {}", r.worker_id);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn initialized_callback(config: Config) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(3000));
        let mut icounts = 0;
        loop {
            interval.tick().await;
            send_message(&Message::InternalPingPongRequest).await;
            icounts += 1;
            if icounts > 4 {
                let wg = WORKERS.get().unwrap().lock().await;
                if wg.len() == 0 {
                    warn!("Ping checking has been stopped. But no workers have been found! Make sure all of your workers are running!");
                    break;
                }
                info!("Ping Checking Stopped. Found: {} workers!", wg.len());
                break;
            }
        }
    });
}

pub async fn send_message(message: &Message) {
    let mut nc = SCHEDULER_NC.get().unwrap().lock().await;
    let nc = nc.as_mut().unwrap();
    nats_publish(nc, message).await;
}
