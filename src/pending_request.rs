use std::time::{Duration, Instant};

use alloy_json_rpc::Response;
use crossbeam::channel::{self, Receiver, Sender};

pub struct PendingRequest {
    pub(crate) notify: Sender<Response>,
    #[cfg(feature = "metrics")]
    pub(crate) sent_on: Instant,
}

impl PendingRequest {
    pub(crate) fn new() -> (Receiver<Response>, Self) {
        let (send, recv) = channel::bounded::<Response>(1);
        (
            recv,
            Self {
                notify: send,
                #[cfg(feature = "metrics")]
                sent_on: Instant::now(),
            },
        )
    }

    pub(crate) fn notify(&self, r: Response) -> Result<(), channel::SendError<Response>> {
        self.notify.send(r)?;

        Ok(())
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn record_metric(&self) -> Duration {
        self.sent_on.elapsed()
    }
}
