use crate::config::Config;
use crate::utils::generic_connector::{nats_connect, nats_listen, nats_publish};
use crate::worker::errors::report_error;
use crate::worker::queue_processor::{
    process_job, JobID, ProcessorIPC, ProcessorIPCData, ProcessorIncomingAction,
};
use crate::worker::{JOB_CHANNELS, WORKER_GUILD_IDS};
use anyhow::Result;
use hearth_interconnect::messages::{Message, PingPongResponse};
use hearth_interconnect::worker_communication::Job;
use log::{debug, error, info};
use nats::asynk::Connection;
use songbird::Songbird;
use std::sync::{Arc, OnceLock};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, Mutex};

pub static WORKER_NC: OnceLock<Mutex<Option<Connection>>> = OnceLock::new();

pub async fn queue_internal_job(job: Job, config: &Config, songbird: Option<Arc<Songbird>>) {
    if &job.worker_id == config.config.worker_id.as_ref().unwrap() {
        info!("Starting new worker");

        let proc_config = config.clone();

        let (tx_processor, _rx_processor): (Sender<ProcessorIPCData>, Receiver<ProcessorIPCData>) =
            broadcast::channel(16);

        let tx_arc = Arc::new(tx_processor);

        {
            WORKER_GUILD_IDS
                .get()
                .unwrap()
                .lock()
                .await
                .push(job.guild_id.clone());
            JOB_CHANNELS
                .get()
                .unwrap()
                .insert(JobID::Specific(job.job_id.clone()), tx_arc.clone());
        }

        let job_tx = tx_arc.clone();

        tokio::spawn(async move {
            process_job(job, &proc_config, job_tx, report_error, songbird).await;
        });
    }
}

pub async fn initialize_api(
    config: &Config,
    ipc: &mut ProcessorIPC,
    songbird: Option<Arc<Songbird>>,
) {
    let nc = nats_connect(config).await;
    let _ = WORKER_NC.set(Mutex::new(Some(nc.clone())));
    nats_listen(
        &nc,
        config,
        parse_message_callback,
        ipc,
        initialized_callback,
        songbird,
    )
    .await;
}

async fn parse_message_callback(
    message: Message,
    config: Config,
    _sender: Arc<Sender<ProcessorIPCData>>,
    songbird: Option<Arc<Songbird>>,
) -> Result<()> {
    debug!("WORKER GOT MSG: {:?}", message);
    match message {
        Message::DirectWorkerCommunication(dwc) => {
            if &dwc.worker_id == config.config.worker_id.as_ref().unwrap() {
                info!(
                    "Received DWC action {:?} for job {} on worker {}",
                    dwc.action_type,
                    dwc.job_id,
                    dwc.worker_id
                );
                let job_id = dwc.job_id.clone();

                let channel = JOB_CHANNELS
                    .get()
                    .unwrap()
                    .get(&JobID::Specific(job_id.clone()));

                match channel {
                    Some(channel) => {
                        let result = channel.send(ProcessorIPCData {
                            action_type: ProcessorIncomingAction::Actions(dwc.action_type.clone()),
                            songbird: None,
                            job_id: JobID::Specific(job_id.clone()),
                            dwc: Some(dwc.clone()),
                            error_report: None,
                        });
                        match result {
                            Ok(_) => {}
                            Err(_e) => {
                                error!("Failed to route DWC job message");
                            }
                        }
                    }
                    None => {
                        error!("Failed to route DWC job message - Could not find channel!");
                        queue_internal_job(
                            Job {
                                job_id: job_id.clone(),
                                worker_id: dwc.worker_id.clone(),
                                request_id: dwc.request_id.clone().unwrap(),
                                guild_id: dwc.guild_id.clone(),
                            },
                            &config,
                            songbird,
                        )
                        .await;
                    }
                }
            }
        }
        Message::InternalPingPongRequest => {
            let mut nc = WORKER_NC.get().unwrap().lock().await;
            let nc = nc.as_mut().unwrap();

            nats_publish(
                nc,
                &Message::InternalPongResponse(PingPongResponse {
                    worker_id: config.config.worker_id.clone().unwrap(),
                }),
            )
            .await;
        }
        Message::InternalWorkerQueueJob(job) => {
            queue_internal_job(job, &config, songbird).await;
        }
        _ => {}
    }
    Ok(())
}

async fn initialized_callback(_: Config) {}

pub async fn send_message(message: &Message) {
    let mut nc = WORKER_NC.get().unwrap().lock().await;
    let nc = nc.as_mut().unwrap();
    nats_publish(nc, message).await;
}
