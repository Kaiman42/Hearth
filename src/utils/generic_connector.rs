use std::sync::Arc;

use hearth_interconnect::messages::Message;
use log::{debug, error};

use crate::config::Config;
use crate::utils::constants::NATS_REQUEST_TIMEOUT;
use crate::worker::queue_processor::{ProcessorIPC, ProcessorIPCData};
use anyhow::Result;
use async_fn_traits::{AsyncFn1, AsyncFn4};
use nats::asynk::Connection;
use songbird::Songbird;
use tokio::sync::broadcast::Sender;

pub async fn nats_connect(config: &Config) -> Connection {
    let server = &config.nats.nats_server;

    let nc = if let Some(token) = &config.nats.nats_token {
        nats::asynk::connect_with_token(&server, token)
            .await
            .expect("Failed to connect to NATS")
    } else {
        nats::asynk::connect(&server)
            .await
            .expect("Failed to connect to NATS")
    };

    nc
}

pub async fn nats_listen(
    nc: &Connection,
    config: &Config,
    callback: impl AsyncFn4<
        Message,
        Config,
        Arc<Sender<ProcessorIPCData>>,
        Option<Arc<Songbird>>,
        Output = Result<()>,
    >,
    ipc: &mut ProcessorIPC,
    initialized_callback: impl AsyncFn1<Config, Output = ()>,
    songbird: Option<Arc<Songbird>>,
) {
    let subject = "communication";
    let sub = nc.subscribe(subject).await.expect("Failed to subscribe");

    initialized_callback(config.clone()).await;

    loop {
        match sub.next().await {
            Some(msg) => {
                let payload = msg.data;
                let parsed_message: Result<Message, serde_json::Error> =
                    serde_json::from_slice(&payload);

                match parsed_message {
                    Ok(m) => {
                        let _ = callback(
                            m,
                            config.clone(),
                            ipc.sender.clone(),
                            songbird.clone(),
                        )
                        .await;
                    }
                    Err(e) => error!("{}", e),
                }
            }
            None => break,
        }
    }
}

pub async fn nats_publish(nc: &Connection, message: &Message) {
    let data = serde_json::to_string(message).unwrap();
    nc.publish("communication", data.as_bytes())
        .await
        .unwrap();
    debug!("Sent MSG");
}
