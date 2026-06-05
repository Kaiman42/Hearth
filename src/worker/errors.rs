use hearth_interconnect::errors::ErrorReport;
use hearth_interconnect::messages::Message;
use log::error;

use crate::config::Config;
use crate::worker::connector::send_message;

pub fn report_error(error: ErrorReport, _config: &Config) {
    error!("{}", error.error);

    tokio::task::spawn(async move {
        send_message(&Message::ErrorReport(error)).await;
    });
}
