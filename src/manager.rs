use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use alloy_json_rpc::{Id, Response, SerializedRequest};
use crossbeam::channel::{self, Sender};
use dashmap::DashMap;
#[cfg(feature = "metrics")]
use hdrhistogram::Histogram;
#[cfg(feature = "metrics")]
use num_format::{Locale, ToFormattedString};

use crate::{
    connection::IpcConnectionHandle, errors::TransportError, pending_request::PendingRequest,
};

#[derive(Clone, Debug)]
pub(crate) struct ReManager {
    requests: Arc<DashMap<Id, PendingRequest>>,
    connection: IpcConnectionHandle,

    to_send: Sender<Option<SerializedRequest>>,

    #[cfg(feature = "metrics")]
    send_to_metrics_channel: Sender<Option<Duration>>,

    #[cfg(feature = "metrics")]
    timed_out_requests: Arc<DashMap<Id, Instant>>,

    #[cfg(feature = "metrics")]
    timed_out_req_count: Arc<AtomicU64>,
    #[cfg(feature = "metrics")]
    total_req_count: Arc<AtomicU64>,
}

impl ReManager {
    #[cfg(not(feature = "metrics"))]
    fn new(connection: IpcConnectionHandle, send: Sender<Option<SerializedRequest>>) -> Self {
        Self {
            connection,
            to_send: send,
            requests: Arc::new(DashMap::new()),
        }
    }

    #[cfg(feature = "metrics")]
    fn new_with_metrics(
        connection: IpcConnectionHandle,
        send: Sender<Option<SerializedRequest>>,
        send_to_metrics_channel: Sender<Option<Duration>>,
    ) -> Self {
        Self {
            connection,
            to_send: send,
            send_to_metrics_channel,
            requests: Arc::new(DashMap::new()),
            timed_out_requests: Arc::new(DashMap::new()),
            timed_out_req_count: Arc::new(AtomicU64::new(0)),
            total_req_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn close(&self) {
        self.connection.close();
        let _ = self.to_send.send(None);
    }

    pub(crate) fn start(
        connection: IpcConnectionHandle,
    ) -> (
        Self,
        JoinHandle<Result<(), TransportError>>,
        JoinHandle<Result<(), TransportError>>,
    ) {
        let (sender, receiver) = channel::bounded(128);

        #[cfg(feature = "metrics")]
        let (metrics_send, metrics_recv) = channel::unbounded();
        #[cfg(feature = "metrics")]
        let manager = ReManager::new_with_metrics(connection, sender, metrics_send);

        #[cfg(not(feature = "metrics"))]
        let manager = ReManager::new(connection, sender);

        #[cfg(feature = "metrics")]
        thread::spawn({
            let manager = manager.clone();
            move || {
                ReManager::metrics_loop(
                    metrics_recv,
                    manager.timed_out_requests,
                    manager.timed_out_req_count,
                    manager.total_req_count,
                );
            }
        });

        let (rec, send) = (manager.clone(), manager.clone());

        //TODO: this needs better handling I ended up heare because ReManager needs new to_send
        let rec_jh = thread::spawn(move || -> Result<(), TransportError> {
            rec.receive_loop()?;
            Ok(())
        });

        let send_jh = thread::spawn(move || -> Result<(), TransportError> {
            send.send_loop(receiver)?;
            Ok(())
        });

        //TODO: this is FUGLY fix it
        (manager, rec_jh, send_jh)
    }

    pub(crate) fn send(&self, req: SerializedRequest) -> Result<Response, TransportError> {
        let (recv, pending_req) = PendingRequest::new();
        let id = req.id().clone();

        self.to_send.send(Some(req))?;
        // Only insert after we are sure that it was sent (at least) to the channel
        self.requests.insert(id, pending_req);
        #[cfg(feature = "metrics")]
        self.total_req_count.fetch_add(1, Ordering::Relaxed);

        let r = recv.recv()?;
        Ok(r)
    }

    pub(crate) fn send_with_timeout(
        &self,
        req: SerializedRequest,
        timeout: Duration,
    ) -> Result<Response, TransportError> {
        let (recv, pending_req) = PendingRequest::new();
        let id = req.id().clone();
        let del_id = req.id().clone();

        self.to_send.send(Some(req))?;
        // Only insert after we are sure that it was sent (at least) to the channel
        self.requests.insert(id, pending_req);
        #[cfg(feature = "metrics")]
        self.total_req_count.fetch_add(1, Ordering::Relaxed);

        let r = match recv.recv_timeout(timeout) {
            Ok(r) => r,
            Err(e) => {
                //TODO: add retry logic
                //In case of timeout drop the request
                if let Some((_, r)) = self.requests.remove(&del_id) {
                    #[cfg(feature = "metrics")]
                    self.timed_out_requests.insert(del_id, r.sent_on);
                    #[cfg(feature = "metrics")]
                    self.timed_out_req_count.fetch_add(1, Ordering::Relaxed);
                }
                return Err(e.into());
            }
        };

        Ok(r)
    }

    fn send_loop(
        &self,
        to_send: channel::Receiver<Option<SerializedRequest>>,
    ) -> Result<(), TransportError> {
        while let Ok(Some(req)) = to_send.recv() {
            let req = req.serialized().get().to_owned().into();
            self.connection.send(Some(req))?;
        }

        Ok(())
    }
    fn receive_loop(&self) -> Result<(), TransportError> {
        while let Ok(Some(resp)) = self.connection.recv() {
            if let Some((_, pending_req)) = self.requests.remove(&resp.id) {
                pending_req.notify(resp)?;

                #[cfg(feature = "metrics")]
                let _ = self
                    .send_to_metrics_channel
                    .send(Some(pending_req.record_metric()));
                continue;
            }

            // reqtuest maybe timed out
            #[cfg(feature = "metrics")]
            if let Some((_, timed_out_req)) = self.timed_out_requests.remove(&resp.id) {
                #[cfg(feature = "metrics")]
                let _ = self
                    .send_to_metrics_channel
                    .send(Some(timed_out_req.elapsed()));
            }
        }

        #[cfg(feature = "metrics")]
        let _ = self.send_to_metrics_channel.send(None);

        self.drop_all_pending_requests();
        Ok(())
    }

    fn drop_all_pending_requests(&self) {
        // DashMap doesn't have drain, this mimics it
        // More info: https://github.com/xacrimon/dashmap/issues/141
        for k in self
            .requests
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            if let Some((_, pending_req)) = self.requests.remove(&k) {
                drop(pending_req);
            }
        }
    }

    #[cfg(feature = "metrics")]
    fn metrics_loop(
        recv_metircs: channel::Receiver<Option<Duration>>,
        timed_out_req: Arc<DashMap<Id, Instant>>,
        timed_out_req_count: Arc<AtomicU64>,
        total_req_count: Arc<AtomicU64>,
    ) {
        let started = Instant::now();
        let mut last_time_logged = Instant::now();
        // range is from 1 micro_sec to 1s (1000 micro sec in 1 milisec and 1000 milisec in 1 sec)
        let mut histogram: Histogram<u64> =
            hdrhistogram::Histogram::new_with_bounds(1, 1000 * 1000, 3)
                .expect("Failed to create histogram");

        let mut total_failed_req_count = 0.0;
        while let Ok(Some(duration)) = recv_metircs.recv() {
            if let Ok(d) = duration.as_micros().try_into() {
                if let Err(e) = histogram.record(d) {
                    println!("IPC Failed to record metric: {}", e);
                }
            } else {
                println!(
                    "IPC  Failed to record metric: duration too big {}",
                    duration.as_secs()
                );
            }

            let failed_req_count = {
                let threshold = Duration::from_secs(3);
                // Not completely accurate due to Dashmap nature, but will do for our tests
                let before_cleanup = timed_out_req.len();
                timed_out_req.retain(|_, req| req.elapsed() < threshold);
                before_cleanup - timed_out_req.len()
            };
            total_failed_req_count += failed_req_count as f64;

            let log_on_interval = last_time_logged.elapsed().as_secs() >= 180;
            if log_on_interval {
                let elapsed_as_sec = started.elapsed().as_secs();
                let (elapsed_since_start, unit) = match elapsed_as_sec {
                    0..60 => (elapsed_as_sec, "s"),
                    60..3600 => (elapsed_as_sec / 60, "min"),
                    _ => (elapsed_as_sec / 3600, "hrs"),
                };

                let timed_out_count = timed_out_req_count.load(Ordering::Relaxed) as f64;
                let total_req_count = total_req_count.load(Ordering::Relaxed);

                let requests_per_sec = (total_req_count as f64) / elapsed_as_sec as f64;
                let f = format!(
                    r#"*************************************************
                       *************************************************
                           * Duration: {}{}
                           * Requests per sec: {:.2}
                           * MAX: {:.2}ms
                           * MEDIAN: {}μs
                           * AVERAGE: {:.2}μs
                           * TAIL LATENCY:
                           * 90%ile: {}μs
                           * 95%ile: {}μs
                           * 99%ile: {:.2}ms
                           * 99.9%ile: {:.2}ms
                           * Requests that timed out: {} / {}: {:.3}%
                           * Failed requests: {} / {}: {:.3}%
                       ****************************************************
                       ****************************************************"#,
                    elapsed_since_start,
                    unit,
                    requests_per_sec,
                    (histogram.max() as f64) / 1000.0,
                    histogram.value_at_quantile(0.5),
                    histogram.mean(),
                    histogram.value_at_quantile(0.9),
                    histogram.value_at_quantile(0.95),
                    (histogram.value_at_quantile(0.99) as f64) / 1000.0,
                    (histogram.value_at_quantile(0.999) as f64) / 1000.0,
                    timed_out_count,
                    total_req_count.to_formatted_string(&Locale::en),
                    (100.0 * timed_out_count) / total_req_count as f64,
                    total_failed_req_count,
                    total_req_count.to_formatted_string(&Locale::en),
                    (100.0 * total_failed_req_count) / total_req_count as f64,
                );
                println!("{}", f);
                last_time_logged = Instant::now();
            }
        }
    }
}
